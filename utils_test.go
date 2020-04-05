package mydump2bq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestGetBigQueryFieldType(t *testing.T) {
	data := []struct {
		input  string
		expect bigquery.FieldType
	}{
		{"string", bigquery.StringFieldType},
		{"String", bigquery.StringFieldType},
		{"StrInG", bigquery.StringFieldType},
		{"STRING", bigquery.StringFieldType},
		{"bytes", bigquery.BytesFieldType},
		{"integer", bigquery.IntegerFieldType},
		{"boolean", bigquery.BooleanFieldType},
		{"timestamp", bigquery.TimestampFieldType},
		{"record", bigquery.RecordFieldType},
		{"date", bigquery.DateFieldType},
		{"time", bigquery.TimeFieldType},
		{"datetime", bigquery.DateTimeFieldType},

		{"xxxxx", bigquery.StringFieldType},
	}
	for _, d := range data {
		assert.Equal(t, d.expect, GetBigQueryFieldType(d.input))
	}
}
