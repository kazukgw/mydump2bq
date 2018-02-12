package main

import (
	"context"
	"flag"
	"io"
	"sync"

	"cloud.google.com/go/bigquery"
	my2bq "github.com/kazukgw/mydump2bq"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

var confFile string

func main() {
	flag.StringVar(&confFile, "config", "mydump2bq.yml", "config file (ext: yml)")
	conf, err := my2bq.NewConfig(confFile)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	opt := option.WithServiceAccountFile(conf.BigQuery.ServiceAccountJson)
	proj := conf.BigQuery.ProjectID
	ctx := context.Background()
	cli, err := bigquery.NewClient(ctx, proj, opt)
	if err != nil {
		log.Fatalf("failed to initialize bq client: %v", err)
	}

	tmapper := my2bq.NewTableMapper(cli, conf.TableMapper)

	wg := &sync.WaitGroup{}
	for _, tm := range tmapper.TableMaps {
		wg.Add(1)
		go func() {
			var err error
			streamer := my2bq.NewStreamer(cli, tm)
			streamer.Start()

			dumper := my2bq.NewDumper(tm, conf.MySQL, conf.MyDump2BQ)
			err = dumper.Dump(func(r io.Reader) {
				scanner, err := my2bq.NewScanner(r, 1024*64, tm)
				if err != nil {
					log.Error(err)
					streamer.StopCh <- true
					return
				}
				for {
					row, err := scanner.Scan()
					if err != nil {
						log.Error(err)
						streamer.StopCh <- true
						return
					}
					streamer.RowCh <- row
				}
			})
			if err != nil {
				log.Error(err)
				streamer.StopCh <- true
			}

			for {
				select {
				case err := <-streamer.ErrCh:
					log.Error(err)
				case <-streamer.DoneCh:
					break
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
