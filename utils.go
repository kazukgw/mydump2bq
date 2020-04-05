package mydump2bq

import (
	"context"
	"strings"

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

var bigqueryFieldTypes []bigquery.FieldType = []bigquery.FieldType{
	bigquery.StringFieldType,
	bigquery.BytesFieldType,
	bigquery.IntegerFieldType,
	bigquery.FloatFieldType,
	bigquery.BooleanFieldType,
	bigquery.TimestampFieldType,
	bigquery.RecordFieldType,
	bigquery.DateFieldType,
	bigquery.TimeFieldType,
	bigquery.DateTimeFieldType,
}

func GetBigQueryFieldType(ftype string) bigquery.FieldType {
	ftypeUpper := strings.ToUpper(ftype)
	for _, t := range bigqueryFieldTypes {
		if ftypeUpper == string(t) {
			return t
		}
	}
	return bigquery.StringFieldType
}
