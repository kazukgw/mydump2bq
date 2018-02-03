package mydump2bq

import (
	""
)

type MySQLTables map[string]*MySQLTable

func (tbls *MySQLTables) Init() error {
	return nil
}

type MySQLTable struct {
	Name   string
	Column []string
}
