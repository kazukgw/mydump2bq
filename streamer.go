package mydump2bq

import (
	"context"
	//
	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	// log "github.com/sirupsen/logrus"
)

const (
	ROW_BUFFER_SIZE = 100
)

type Streamer struct {
	*bigquery.Client
	*TableMap
	*bigquery.Uploader
	rowBuffer []*Row
	rowCh     chan *Row
	errCh     chan error
	stopCh    chan bool
}

func NewStreamer(cli *bigquery.Client, tm *TableMap) *Streamer {
	u := tm.Table.Uploader()
	return &Streamer{
		Client:    cli,
		TableMap:  tm,
		Uploader:  u,
		rowBuffer: []*Row{},
		rowCh:     make(chan *Row),
		errCh:     make(chan error),
		stopCh:    make(chan bool),
	}
}

func (st *Streamer) Start(onErr func(error), onStop func()) {
	go func() {
		for {
			select {
			case r := <-st.rowCh:
				st.put(r)
			case <-st.stopCh:
				st.Flash()
				onStop()
			case err := <-st.errCh:
				onErr(err)
			}
		}
	}()
}

func (st *Streamer) Put(r *Row) {
	st.rowCh <- r
}

func (st *Streamer) put(r *Row) {
	if len(st.rowBuffer) > ROW_BUFFER_SIZE {
		st.uploaderPut()
		st.rowBuffer = []*Row{}
	}
	st.rowBuffer = append(st.rowBuffer, r)
}

func (st *Streamer) Flash() {
	st.uploaderPut()
	st.rowBuffer = []*Row{}
}

func (st *Streamer) uploaderPut() {
	go func(rb []*Row) {
		var err error
		retryCnt := 3
		for retryCnt > 0 {
			ctx := context.Background()
			err = st.Uploader.Put(ctx, rb)
			if err == nil {
				break
			}
			retryCnt -= 1
		}
		if err != nil {
			st.errCh <- err
		}
	}(st.rowBuffer)
}
