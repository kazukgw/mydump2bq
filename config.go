package mydump2bq

import (
	"bytes"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	MySQL       MySQLConfig
	BigQuery    BigQueryConfig
	MyDump2BQ   MyDump2BQConfig
	TableMapper TableMapperConfig
}

type MySQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

type BigQueryConfig struct {
	ProjectID          string
	ServiceAccountJson string
}

type MyDump2BQConfig struct {
	MaxBufferSize int
	MaxConcurrent int
	Command       string
}

type TableMapperConfig []TableMapConfig

type TableMapConfig struct {
	MySQL struct {
		Database string
		Table    string
	}
	BigQuery struct {
		DatasetID string
		TableID   string
		Schema    []TableMapFieldConfig
	}
}

type TableMapFieldConfig struct {
	Name string
	Type string
	Mode string
}

func NewConfig(configFile string) (*Config, error) {
	var err error
	dat, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return NewConfigWithData(dat)
}

func NewConfigWithData(configData []byte) (*Config, error) {
	var err error
	viper.SetConfigType("yaml")
	err = viper.ReadConfig(bytes.NewReader(configData))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	conf := &Config{}
	err = viper.Unmarshal(conf)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return conf, nil

}
