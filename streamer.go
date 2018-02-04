package mydump2bq

import (
	"context"

	"cloud.google.com/go/bigquery"
)

type BigQueryStreamer struct {
	DatasetID string
	TableID   string
	*MySQLTable
	*bigquery.Uploader
	InCh     chan *Row
	StopCh   chan struct{}
	FinishCh chan struct{}
}

func NewBigQueryStreamer(datasetID, tableID string) *BigQueryStreamer {
	// u := client.Dataset(datasetID).Table(tableID).Uploader()
	return nil
}

func (st *BigQueryStreamer) Start() {
	bufsize := 100
	buf := make([]*Row, bufsize)
	seqnum := 0
	go func() {
		for {
			select {
			case row := <-st.InCh:
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
