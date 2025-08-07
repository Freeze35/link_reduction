package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	DB         DBC        `mapstructure:"db"`
	Server     Server     `mapstructure:"server"`
	Redis      Redis      `mapstructure:"redis"`
	Kafka      Kafka      `mapstructure:"kafka"`
	Prometheus Prometheus `mapstructure:"prometheus"`
	Version    string     `mapstructure:"version"`
}

type DBC struct {
	PostgresDB string `mapstructure:"postgresdb_dsn"`
	LinksDB    string `mapstructure:"linksdb_dsn"`
	Name       string `mapstructure:"name"`
	Migrations string `mapstructure:"migrations"`
}

type Server struct {
	BaseURL string `mapstructure:"base_url"`
}

type Redis struct {
	URL string `mapstructure:"url"`
}

type Kafka struct {
	Brokers string `mapstructure:"brokers"`
}

type Prometheus struct {
	URL string `mapstructure:"url"`
}

func LoadConfig(path string) (cfg Config, err error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	err = viper.ReadInConfig()
	if err != nil {
		return cfg, fmt.Errorf("cannot read config: %w", err)
	}

	err = viper.Unmarshal(&cfg)
	if err != nil {
		return cfg, fmt.Errorf("cannot unmarshal config: %w", err)
	}
	return
}
