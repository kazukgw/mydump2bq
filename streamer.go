package mydump2bq

import (
	"cloud.google.com/go/bigquery"
)

type BigQueryStreamer struct {
	DatasetID string
	TableID   string
	*MySQLTable
	*bigquery.Uploader
}

func NewBigQueryStreamer(datasetID, tableID string) *Streamer {
	u := client.Dataset(datasetID).Table(tableID).Uploader()
}
