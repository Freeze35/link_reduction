package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"linkreduction/internal/config"
	"strings"
	"time"
)

func InitPostgres(cfg *config.Config) (*sql.DB, error) {
	dbURL := cfg.DB.LinksDB
	if dbURL == "" {
		return nil, fmt.Errorf("переменная окружения DB_DSN_LINKSDB не задана")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия базы данных: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("база данных недоступна: %v", err)
	}

	return db, nil
}

func RedisConnect(ctx context.Context, cfg *config.Config) (*redis.Client, error) {
	redisURL := cfg.Redis.URL
	if redisURL == "" {
		return nil, fmt.Errorf("переменная окружения REDIS_URL не задана")
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisURL})

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("Redis недоступен: %v", err)
	}

	return redisClient, nil
}

// GetKafkaBrokers получает список Kafka брокеров из переменной окружения KAFKA_BROKERS.
// Возвращает срез строк с адресами брокеров или ошибку, если переменная не задана или содержит недопустимые значения.
func GetKafkaBrokers(cfg *config.Config) ([]string, error) {
	kafkaEnv := cfg.Kafka.Brokers
	if kafkaEnv == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS не задана")
	}
	brokers := strings.Split(kafkaEnv, ",")
	cleaned := make([]string, 0, len(brokers))
	for _, b := range brokers {
		trimmed := strings.TrimSpace(b)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS не содержит валидных брокеров")
	}

	return cleaned, nil
}

// NewKafkaProducerConfig создаёт и настраивает конфигурацию для Kafka продюсера.
// Устанавливаются параметры подтверждения, количество попыток и задержка между ними.
func NewKafkaProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond
	return cfg
}

// ConnectKafkaProducer пытается подключиться к Kafka брокерам с помощью переданной конфигурации.
// Делает до 10 попыток с задержкой в 2 секунды между ними. Возвращает SyncProducer или ошибку.
func ConnectKafkaProducer(brokers []string, cfg *sarama.Config) (sarama.SyncProducer, error) {
	for i := 0; i < 10; i++ {
		producer, err := sarama.NewSyncProducer(brokers, cfg)
		if err == nil {

			return producer, nil
		}

		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("не удалось подключиться к Kafka после 10 попыток")
}

// InitKafkaProducer объединяет шаги инициализации Kafka продюсера:
func InitKafkaProducer(cfg *config.Config) (sarama.SyncProducer, error) {

	brokers, err := GetKafkaBrokers(cfg)
	if err != nil {
		return nil, err
	}

	config := NewKafkaProducerConfig()

	producer, err := ConnectKafkaProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
