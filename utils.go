package mydump2bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

func NewBigQueryClient(conf *Config) (*bigquery.Client, error) {
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

func GetBigQueryFieldType(ftype string) bigquery.FieldType {
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
