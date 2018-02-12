package mydump2bq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitConfig(t *testing.T) {
	configData := []byte(
		`MySQL:
  Host: test-host
  Port: 3306
  User: test-user
  Password: test-password

BigQuery:
  ProjectID: test_project
  ServiceAccountJson: test.json

Mydump2bq:
  MaxBufSize: 65536
  MaxConcurrent: 2
  Command: mysqldump

TableMapper:
  - MySQL:
      Database: test
      Table: test_orders
    BigQuery:
      DatasetID: test_mydump2bq
      TableID: test_orders
      Schema:
        - {Name: col1, Type: integer, Mode: nullable}
        - {Name: col2, Type: string, Mode: nullable}
`)

	conf, err := NewConfigWithData(configData)
	if err != nil {
		t.Fatalf("faild to load conf: %s", err)
	}

	assert.Equal(t, "test-host", conf.MySQL.Host)
	assert.Equal(t, 3306, conf.MySQL.Port)
	assert.Equal(t, 1, len(conf.TableMapper))
	assert.Equal(t, "test", conf.TableMapper[0].MySQL.Database)
}
