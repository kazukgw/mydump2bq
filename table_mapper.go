package mydump2bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
)

type TableMapper struct {
	*bigquery.Client
	TableMaps []*TableMap
}

func NewTableMapper(cli *bigquery.Client, conf TableMapperConfig) *TableMapper {
	tmaps := make([]*TableMap, 0)
	for _, tmConf := range conf {
		tmaps = append(tmaps, NewTableMap(cli, tmConf))
	}
	return &TableMapper{Client: cli, TableMaps: tmaps}
}

type TableMap struct {
	Config TableMapConfig
	*bigquery.Dataset
	*bigquery.Table
	*bigquery.TableMetadata
}

func NewTableMap(
	cli *bigquery.Client,
	tmConf TableMapConfig,
) *TableMap {
	ds := cli.Dataset(tmConf.BigQuery.DatasetID)
	tbl := ds.Table(tmConf.BigQuery.TableID)
	return &TableMap{
		Config:  tmConf,
		Dataset: ds,
		Table:   tbl,
	}
}

func (tm *TableMap) CreateBigQueryTableIfNotExists() error {
	var err error

	ctx := context.Background()
	t := tm.Table
	meta, err := t.Metadata(ctx)
	if err == nil {
		tm.TableMetadata = meta
		return nil
	}

	meta = &bigquery.TableMetadata{
		Name:   t.TableID,
		Schema: tm.BigQuerySchema(),
	}
	err = t.Create(ctx, meta)
	if err != nil {
		return errors.WithStack(err)
	}
	tm.TableMetadata = meta

	return nil

}

func (tm *TableMap) BigQuerySchema() bigquery.Schema {
	schema := make(bigquery.Schema, 0)
	schemaConfig := tm.Config.BigQuery.Schema
	for _, f := range schemaConfig {
		s := &bigquery.FieldSchema{
			Name: f.Name,
			Type: GetBigQueryFieldType(f.Type),
		}
		schema = append(schema, s)
	}
	return bigquery.Schema(schema)
}
