package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	DB     DBConfig     `mapstructure:"db"`
	Server ServerConfig `mapstructure:"server"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
}

type DBConfig struct {
	PostgresDB string `mapstructure:"postgresdb_dsn"`
	LinksDB    string `mapstructure:"linksdb_dsn"`
	Name       string `mapstructure:"name"`
	Migrations string `mapstructure:"migrations"`
}

type ServerConfig struct {
	BaseURL string `mapstructure:"base_url"`
}

type RedisConfig struct {
	URL string `mapstructure:"url"`
}

type KafkaConfig struct {
	Brokers string `mapstructure:"brokers"`
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
