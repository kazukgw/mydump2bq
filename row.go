package mydump2bq

import (
	"fmt"
	"strconv"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
)

// Row imprements ValueSaver interface
// type ValueSaver interface {
// 	Save() (row map[string]Value, insertID string, err error)
// }
type Row struct {
	*TableMap
	InsertID  string
	RawValues []string
}

func NewRow(tm *TableMap, rawValues []string, scannerID string) *Row {
	return &Row{TableMap: tm, RawValues: rawValues}
}

func (r *Row) Save() (row map[string]bigquery.Value, insertID string, err error) {
	var errs = ""
	err = nil
	insertID = r.InsertID
	row = make(map[string]bigquery.Value)
	schema := r.TableMap.TableMetadata.Schema
	for i, rawVal := range r.RawValues {
		var v interface{}
		var e error
		if rawVal == "NULL" {
			v = nil
		} else {
			switch t := schema[i].Type; t {
			case bigquery.StringFieldType:
				v = rawVal
			case bigquery.BytesFieldType:
				v = rawVal
			case bigquery.IntegerFieldType:
				v, e = strconv.ParseInt(rawVal, 10, 64)
			case bigquery.FloatFieldType:
				v, e = strconv.ParseFloat(rawVal, 64)
			case bigquery.BooleanFieldType:
				v = rawVal != ""
			case bigquery.TimestampFieldType:
				v = rawVal
			case bigquery.RecordFieldType:
				v = rawVal
			case bigquery.DateFieldType:
				v = rawVal
			case bigquery.TimeFieldType:
				v = rawVal
			case bigquery.DateTimeFieldType:
				v = rawVal
			default:
				v = rawVal
			}
		}
		if e != nil {
			errs += e.Error() + ";"
		}
		row[schema[i].Name] = v
	}
	if len(errs) > 0 {
		err = errors.New(errs)
	}
	return
}
