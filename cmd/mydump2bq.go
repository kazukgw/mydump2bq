package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/kazukgw/mydump2bq"
)

func main() {
	conf := mydump2bq.BigQueryStreamerConfig{}
	err := conf.Load("example_conf.yml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	bqst := NewBigQueryStreamer(conf)
}
