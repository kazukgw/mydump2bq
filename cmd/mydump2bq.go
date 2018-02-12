package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/kazukgw/mydump2bq"
)

func main() {
	conf := mydump2bq.Config{}
	err := conf.Load("example_conf.yml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	bqst := NewStreamer(conf)
}
