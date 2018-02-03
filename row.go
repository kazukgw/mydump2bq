package mydump2bq

import (
	"strconv"

	"cloud.google.com/go/bigquery"
)

// Row imprements ValueSaver interface
// type ValueSaver interface {
// 	Save() (row map[string]Value, insertID string, err error)
// }
type Row struct {
	*MySQLTable
	InsertID  string
	RawValues []string
}

func NewRow(tableName string, rawValues []string) *Row {
	return nil
}

func (r *Row) Save() (row map[string]bigquery.Value, insertID string, err error) {
	err = nil
	insertID = r.InsertID
	row = make(map[string]bigquery.Value)
	for i, rawVal := range r.RawValues {
		var v interface{}
		if rawVal == "NULL" {
			v = nil
		} else if i, err := strconv.Atoi(rawVal); err == nil {
			v = i
		} else if f, err := strconv.ParseFloat(rawVal, 64); err == nil {
			v = f
		} else {
			v = rawVal
		}
		row[r.MySQLTable.Column[i]] = v
	}
	return
}
