package mydump2bq

import (
	"bytes"
	"io/ioutil"
	"testing"

	// "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestNewScanner(t *testing.T) {
	r := bytes.NewReader([]byte{})
	tmap := &TableMap{}
	sc, err := NewScanner(r, 100, tmap)

	assert.Nil(t, err)
	assert.Equal(t, 100, sc.MaxBufSize)
	assert.NotEmpty(t, sc.ID)
}

func TestScannerScan(t *testing.T) {
	input, err := ioutil.ReadFile("data/dump.sql")
	if err != nil {
		t.Fatalf("failed to load dump.sql: %#v", err)
	}
	r := bytes.NewReader(input)

	tmap := &TableMap{}

	sc, err := NewScanner(r, 1024*64, tmap)
	assert.Nil(t, err)

	row, err := sc.Scan()
	assert.Nil(t, err)

	expectedRawValues := []string{
		"1", "f''\nuga", "address-2-a-b-c", "NULL", "300",
		"3450987", "08099991111", "2016-09-01 00:00:00",
	}
	assert.Equal(t, len(expectedRawValues), len(row.RawValues))
	assert.ElementsMatch(t, expectedRawValues, row.RawValues)
}
