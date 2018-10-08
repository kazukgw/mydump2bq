package mydump2bq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewRow(t *testing.T) {
	tableMap := &TableMap{}
	rawValues := []string{"val1", "val2"}
	id := uuid.NewV4()
	scannerID := id.String()
	r := NewRow(tableMap, rawValues, scannerID)
	assert.NotNil(t, r)
}

func TestRowSave(t *testing.T) {
	meta := &bigquery.TableMetadata{}
	meta.Schema = []*bigquery.FieldSchema{
		{Name: "col1", Type: bigquery.StringFieldType},
		{Name: "col2", Type: bigquery.BytesFieldType},
		{Name: "col3", Type: bigquery.IntegerFieldType},
		{Name: "col4", Type: bigquery.FloatFieldType},
		{Name: "col5", Type: bigquery.BooleanFieldType},
		{Name: "col6", Type: bigquery.TimestampFieldType},
		{Name: "col7", Type: bigquery.DateFieldType},
		{Name: "col8", Type: bigquery.TimeFieldType},
	}
	tableMap := &TableMap{TableMetadata: meta}
	rv := []string{
		"string",
		"bytes-data",
		"12345",
		"12.3456789",
		"1",
		"2018-02-12 00:00:00",
		"2018-02-12",
		"11:00",
	}
	r := NewRow(tableMap, rv, "12345")
	rowv, insid, err := r.Save()
	assert.Nil(t, err)
	assert.Equal(t, "", insid)

	v1 := rowv["col1"].(string)
	assert.Equal(t, "string", v1)

	v2 := rowv["col2"].(string)
	assert.Equal(t, "bytes-data", v2)

	v3 := rowv["col3"].(int64)
	assert.Equal(t, int64(12345), v3)

	v4 := rowv["col4"].(float64)
	assert.Equal(t, float64(12.3456789), v4)

	v5 := rowv["col5"].(bool)
	assert.Equal(t, true, v5)

	v6 := rowv["col6"].(string)
	assert.Equal(t, "2018-02-12 00:00:00", v6)

	v7 := rowv["col7"].(string)
	assert.Equal(t, "2018-02-12", v7)

	v8 := rowv["col8"].(string)
	assert.Equal(t, "11:00", v8)
}
