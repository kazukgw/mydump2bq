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

var (
	VERSION  string
	confFile string
)

func main() {
	log.SetLevel(log.InfoLevel)
	// log.SetLevel(log.DebugLevel)
	log.Infof("mydump2bq (version: %s)", VERSION)
	flag.StringVar(&confFile, "config", "mydump2bq.yml", "config file (yaml formated)")
	log.Info("load config")
	conf, err := my2bq.NewConfig(confFile)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	log.Info("initalize bigquery client")
	opt := option.WithServiceAccountFile(conf.BigQuery.ServiceAccountJson)
	proj := conf.BigQuery.ProjectID
	ctx := context.Background()
	cli, err := bigquery.NewClient(ctx, proj, opt)
	if err != nil {
		log.Fatalf("failed to initialize bq client: %v", err)
	}

	log.Info("create table mapper")
	tmapper := my2bq.NewTableMapper(cli, conf.TableMapper)

	wg := &sync.WaitGroup{}
	for _, tm := range tmapper.TableMaps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("mydump2bq thraed start")
			var err error

			if err := tm.CreateBigQueryTableIfNotExists(); err != nil {
				log.Errorf("failed to create bigquery table: %v", err)
				return
			}

			log.Info("create streamer")
			streamer := my2bq.NewStreamer(cli, tm)
			log.Info("start streamer")
			streamer.Start()

			log.Info("create dumper")
			dumper := my2bq.NewDumper(tm, conf.MySQL, conf.MyDump2BQ)
			log.Info("start dump")
			err = dumper.Dump(func(r io.Reader) {
				log.Info("create scanner")
				scanner, err := my2bq.NewScanner(r, 1024*64, tm)
				if err != nil {
					log.Error(err)
					streamer.StopCh <- true
					return
				}
				log.Info("start scan")
				for {
					row, err := scanner.Scan()
					if err != nil {
						if err != io.EOF {
							log.Error(err)
						} else {
							log.Info("finish mysqldump")
						}
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
					log.Errorf("streamer error: %v", err)
				case <-streamer.DoneCh:
					log.Info("mydump2bq thraed done")
					return
				}
			}
		}()
	}
	wg.Wait()
}
