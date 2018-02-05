package mydump2bq

import (
	"context"
	"io/ioutil"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

type BQSchema []struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Mode string `yaml:"mode"`
}

type BigQueryStreamerConfig struct {
	BigQuery struct {
		ProjectID          string `yaml:"project_id"`
		ServiceAccountJson string `yaml:"service_account_json"`
	} `yaml:"bigquery"`
	Command  string `yaml:"command"`
	TableMap map[string]struct {
		Dataset  string `yaml:"dataset"`
		Table    string `yaml:"table"`
		BQSchema `yaml:"schema"`
	} `yaml:"table_map"`
}

func (conf *BigQueryStreamerConfig) Load(confFile string) error {
	var err error
	file, err := os.Open(confFile)
	if err != nil {
		return errors.WithStack(err)
	}

	dat, err := ioutil.ReadAll(file)
	if err != nil {
		return errors.WithStack(err)
	}

	err = yaml.Unmarshal(dat, conf)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (conf *BigQueryStreamerConfig) NewClient() (*bigquery.Client, error) {
	var client *bigquery.Client
	var err error
	ctx := context.Background()
	if conf.BigQuery.ServiceAccountJson != "" {
		opt := option.WithServiceAccountFile(conf.BigQuery.ServiceAccountJson)
		client, err = bigquery.NewClient(ctx, conf.BigQuery.ProjectID, opt)
	} else {
		client, err = bigquery.NewClient(ctx, conf.BigQuery.ProjectID)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return client, err
}
