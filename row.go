package mydump2bq

import (
	"cloud.google.com/go/bigquery"
)

// Row imprements ValueSaver interface
// type ValueSaver interface {
// 	Save() (row map[string]Value, insertID string, err error)
// }
type Row struct {
}

func (r *Row) Save() (row map[string]bigquery.Value, insertID string, err error) {
}
