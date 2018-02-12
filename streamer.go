package mydump2bq

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	ROW_BUFFER_SIZE = 1000
)

type Streamer struct {
	*bigquery.Client
	*TableMap
	*bigquery.Uploader
	RowCh        chan *Row
	ErrCh        chan error
	StopCh       chan bool
	DoneCh       chan bool
	rowBuffer    []*Row
	rowBufCh     chan []*Row
	workerDoneCh chan bool
	wg           *sync.WaitGroup
}

func NewStreamer(cli *bigquery.Client, tm *TableMap) *Streamer {
	u := tm.Table.Uploader()
	return &Streamer{
		Client:       cli,
		TableMap:     tm,
		Uploader:     u,
		RowCh:        make(chan *Row),
		ErrCh:        make(chan error),
		StopCh:       make(chan bool),
		DoneCh:       make(chan bool),
		rowBuffer:    make([]*Row, 0),
		rowBufCh:     make(chan []*Row, 10),
		workerDoneCh: make(chan bool),
		wg:           &sync.WaitGroup{},
	}
}

func (st *Streamer) Start() {
	go func() {
		defer func() {
			log.Info("streamer done")
			st.DoneCh <- true
		}()
		log.Info("streamer start")
		for {
			select {
			case r := <-st.RowCh:
				log.Debug("handle row")
				st.Put(r)
			case <-st.StopCh:
				st.Flash()
				st.wg.Wait()
				st.workerDoneCh <- true
				log.Debug("stop streamer")
				return
			}
		}
	}()
	go st.startUploaderPutWorker()
}

func (st *Streamer) startUploaderPutWorker() {
	log.Info("worker loop start")
	c := make(chan bool, 100)
	for {
		select {
		case <-st.workerDoneCh:
			return
		case rb := <-st.rowBufCh:
			c <- true
			st.wg.Add(1)
			go func(rb []*Row) {
				defer func() {
					if err := recover(); err != nil {
						st.ErrCh <- errors.WithStack(errors.Errorf("panic: %s", err))
					}
					st.wg.Done()
					<-c
				}()
				var err error
				retryCnt := 3
				sleep := int64(2)
				for retryCnt > 0 {
					ctx := context.Background()
					err = st.Uploader.Put(ctx, rb)
					if err == nil {
						break
					}
					time.Sleep(time.Duration(sleep) * time.Second)
					sleep = sleep * int64(2)
					retryCnt -= 1
				}
				if err != nil {
					log.Errorf("upload error: %s", err)
					st.ErrCh <- errors.WithStack(err)
				}
			}(rb)
		}
	}
}

func (st *Streamer) Put(r *Row) {
	if len(st.rowBuffer) > ROW_BUFFER_SIZE {
		st.rowBufCh <- st.rowBuffer
		st.rowBuffer = make([]*Row, 0)
	}
	st.rowBuffer = append(st.rowBuffer, r)
}

func (st *Streamer) Flash() {
	st.rowBufCh <- st.rowBuffer
	st.rowBuffer = []*Row{}
}
