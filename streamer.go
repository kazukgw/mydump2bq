package mydump2bq

import (
	"context"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type BigQueryStreamer struct {
	Conf BigQueryStreamerConfig
	*bigquery.Client
	Uploaders map[string]*bigquery.Uploader
	inCh      chan *Row
	stopCh    chan struct{}
	finishCh  chan struct{}
}

func NewBigQueryStreamer(conf BigQueryStreamerConfig) (*BigQueryStreamer, error) {
	cli, err := conf.NewClient()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bqst := &BigQueryStreamer{
		Conf:      conf,
		Client:    cli,
		Uploaders: make(map[string]*bigquery.Uploader),
		inCh:      make(chan *Row),
		stopCh:    make(chan struct{}),
		finishCh:  make(chan struct{}),
	}
	return bqst, nil
}

func (st *BigQueryStreamer) Init() error {
	var err error
	for mytable, bqconf := range st.Conf.TableMap {
		ctx := context.Background()
		cli := st.Client
		table := cli.Dataset(bqconf.Dataset).Table(bqconf.Table)
		_, err = table.Metadata(ctx)
		if err != nil {
			table, err = st.CreateTable(
				cli,
				bqconf.Dataset,
				bqconf.Table,
				bqconf.BQSchema,
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		st.Uploaders[mytable] = table.Uploader()
	}
	return nil
}

func (st *BigQueryStreamer) CreateTable(
	client *bigquery.Client,
	datasetID string,
	tableID string,
	schema BQSchema,
) (*bigquery.Table, error) {
	ctx := context.Background()
	t := client.Dataset(datasetID).Table(tableID)
	bqschema := []*bigquery.FieldSchema{}
	for _, s := range schema {
		fs := &bigquery.FieldSchema{
			Name: s.Name,
			Type: st.getFieldType(strings.ToLower(s.Type)),
		}
		bqschema := append(bqschema, fs)
	}
	meta := &bigquery.TableMetadata{
		Name:   tableID,
		Schema: bqschema,
	}
	err := t.Create(ctx, meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return t, nil
}

func (st *BigQueryStreamer) getFieldType(ftype string) bigquery.FieldType {
	if ftype == "string" {
		return bigquery.StringFieldType
	} else if ftype == "bytes" {
		return bigquery.BytesFieldType
	} else if ftype == "integer" {
		return bigquery.IntegerFieldType
	} else if ftype == "float" {
		return bigquery.FloatFieldType
	} else if ftype == "boolean" {
		return bigquery.BooleanFieldType
	} else if ftype == "timestamp" {
		return bigquery.TimestampFieldType
	} else if ftype == "record" {
		return bigquery.RecordFieldType
	} else if ftype == "date" {
		return bigquery.DateFieldType
	} else if ftype == "time" {
		return bigquery.TimeFieldType
	} else if ftype == "datetime" {
		return bigquery.DateTimeFieldType
	} else {
		return bigquery.StringFieldType
	}
}

func (st *BigQueryStreamer) Start() {
	bufsize := 100
	buf := make([]*Row, bufsize)
	seqnum := 0
	go func() {
		for {
			select {
			case row := <-st.inCh:
				buf = append(buf, row)
				seqnum += 1
				if len(buf) == bufsize {
					go func(rows []*Row) {
						ctx := context.Background()
						st.Uploader.Put(ctx, rows)
					}(buf)
					buf = make([]*Row, bufsize)
				}
			case <-st.StopCh:
				break
			}
		}
		st.FinishCh <- struct{}{}
	}()
}
