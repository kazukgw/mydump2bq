package mydump2bq

import (
	"cloud.google.com/go/bigquery"
)

type BigQueryTables map[string]*BigQueryTable

type BigQueryTable struct {
	DatasetID string
	TableID   string
	*bigquery.Table
}

func NewBigQueryTable(
	client *bigquery.Client,
	datasetID string,
	tableID string,
) *BigQueryTable {
	t := client.Dataset(datasetID).Table(tableID)
	return &BigQueryTable{
		DatasetID: datasetID,
		TableID:   tableID,
		Table:     t,
	}
}
