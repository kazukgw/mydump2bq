package mydump2bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
)

const (
	ROW_BUFFER_SIZE = 100
)

type Streamer struct {
	*bigquery.Client
	*TableMap
	*bigquery.Uploader
	RowCh     chan *Row
	ErrCh     chan error
	StopCh    chan bool
	DoneCh    chan bool
	rowBuffer []*Row
}

func NewStreamer(cli *bigquery.Client, tm *TableMap) *Streamer {
	u := tm.Table.Uploader()
	return &Streamer{
		Client:    cli,
		TableMap:  tm,
		Uploader:  u,
		RowCh:     make(chan *Row),
		ErrCh:     make(chan error),
		StopCh:    make(chan bool),
		DoneCh:    make(chan bool),
		rowBuffer: []*Row{},
	}
}

func (st *Streamer) Start() {
	go func() {
		for {
			select {
			case r := <-st.RowCh:
				st.put(r)
			case <-st.StopCh:
				st.Flash()
				break
			}
		}
		st.DoneCh <- true
	}()
}

func (st *Streamer) Put(r *Row) {
	st.RowCh <- r
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
		defer func() {
			if err := recover(); err != nil {
				st.ErrCh <- errors.WithStack(errors.Errorf("panic: %s", err))
			}
		}()
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
			st.ErrCh <- errors.WithStack(err)
		}
	}(st.rowBuffer)
}
