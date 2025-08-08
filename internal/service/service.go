package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/const"
	"linkreduction/internal/models"
	initprometheus "linkreduction/internal/prometheus"
	"net/url"
	"strings"
	"time"
)

// Service - сервис для работы с сокращением ссылок
type Service struct {
	ctx      context.Context
	repo     LinkRepo
	cache    LinkCache
	producer sarama.SyncProducer
	metrics  *initprometheus.PrometheusMetrics
}

// NewLinkService создаёт новый экземпляр Service
func NewLinkService(ctx context.Context, repo LinkRepo, cache LinkCache, producer sarama.SyncProducer, metrics *initprometheus.PrometheusMetrics) *Service {
	return &Service{ctx: ctx, repo: repo, cache: cache, producer: producer, metrics: metrics}
}

// CleanupOldLinks периодически удаляет записи старше 2 недель
func (s *Service) CleanupOldLinks(logger *logrus.Logger) {

	ticker := time.NewTicker(2 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := s.repo.DeleteOldLinks(s.ctx, "2 weeks")
			if err != nil {
				logger.Error(err)
				continue
			}
		}
	}
}

// ShortenURL проверяет URL, ищет в кэше/БД или генерирует новый ключ
func (s *Service) ShortenURL(ctx context.Context, originalURL string, baseUrl string) (string, error) {

	// Валидация URL
	if err := validateURL(originalURL, baseUrl); err != nil {
		return "", err
	}

	// Проверка в кэше

	if cachedShortLink, err := s.cache.GetShortLink(ctx, originalURL); err != nil {
		return "", fmt.Errorf("ошибка чтения из кэша: %v", err)
	} else if cachedShortLink != "" {
		return cachedShortLink, nil
	}

	// Проверка в БД
	shortLink, err := s.repo.FindByOriginalURL(ctx, originalURL)
	if err != nil {
		return "", fmt.Errorf("ошибка проверки URL в базе данных: %w", err)
	}
	if shortLink != "" {
		if err := s.cache.SetShortLink(ctx, originalURL, shortLink, time.Minute*10); err != nil {
			return "", fmt.Errorf("ошибка записи в кэш: %w", err)
		}
		return shortLink, nil
	}

	// Генерация нового ключа
	for i := 0; i < 3; i++ {
		inputURL := originalURL
		if i > 0 {
			inputURL = fmt.Sprintf("%s_%d", originalURL, i)
		}
		shortLink := generateShortLink(inputURL)

		if existing, err := s.repo.FindByShortLink(ctx, shortLink); err != nil {
			return "", fmt.Errorf("ошибка проверки ключа: %v", err)
		} else if existing == "" { // Ключ уникален
			return shortLink, nil
		}

		if i == 2 {
			return "", fmt.Errorf("не удалось сгенерировать уникальный ключ после %d попыток", i+1)
		}
	}

	// Недостижимый код (оставлен для совместимости, но можно удалить)
	return "", fmt.Errorf("не удалось сгенерировать короткую ссылку")
}

// InsertLink вставляет новую ссылку в хранилище
func (s *Service) InsertLink(ctx context.Context, originalURL, shortLink string) error {
	err := s.repo.Insert(ctx, originalURL, shortLink)
	if err != nil {
		return err
	}
	if err := s.cache.SetShortLink(ctx, originalURL, shortLink, time.Minute*10); err != nil {
		return fmt.Errorf("не удалось вставить новую ссылку: %v", err)
	}

	return nil
}

// GetOriginalURL получает оригинальный URL по короткой ссылке
func (s *Service) GetOriginalURL(ctx context.Context, shortLink string) (string, error) {
	// Проверка в кэше
	if cachedURL, err := s.cache.GetOriginalURL(ctx, shortLink); err != nil {
		return "", fmt.Errorf("ошибка чтения из кэша: %v", err)
	} else if cachedURL != "" {
		return cachedURL, nil
	}

	// Проверка в БД
	originalURL, err := s.repo.FindByShortLink(ctx, shortLink)
	if err != nil {
		return "", fmt.Errorf("ошибка базы данных: %v", err)
	}
	if originalURL == "" {
		return "", nil
	}

	// Кэширование результата
	if err := s.cache.SetOriginalURL(ctx, shortLink, originalURL, 10*60); err != nil {
		return "", fmt.Errorf("ошибка записи в кэш: %v", err)
	}

	return originalURL, nil
}

func generateShortLink(originalURL string) string {
	hash := md5.Sum([]byte(originalURL))
	return fmt.Sprintf("%x", hash)[:6]
}

// InsertBatch выполняет пакетную вставку в PostgreSQL
func (s *Service) InsertBatch(ctx context.Context, batch []models.LinkURL) error {
	if len(batch) == 0 {
		return fmt.Errorf("длина батча нулевая")
	}

	rowsAffected, err := s.repo.InsertBatch(ctx, batch)
	if err != nil {
		return fmt.Errorf("ошибка при внедрение батча %v", err)
	}

	for _, link := range batch[:rowsAffected] {
		if err := s.cache.SetShortLink(ctx, link.OriginalURL, link.ShortLink, time.Minute*10); err != nil {
			return fmt.Errorf("ошибка записи в Redis (shorten): %v,%v", link.OriginalURL, err)
		}
	}
	return nil
}

func (s *Service) SendMessageToDB(originalURL string, shortLink string) error {
	// Если Kafka доступна, отправляем сообщение
	if s.producer != nil {
		msg := &message.ShortenMessage{OriginalURL: originalURL, ShortLink: shortLink}
		messageBytes, err := json.Marshal(msg)
		if err != nil {

			if s.metrics != nil && s.metrics.CreateShortLinkTotal != nil {
				s.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_serialization").Inc()
			}

			return fmt.Errorf("kafka metric error")
		}

		_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
			Topic: message.ShortenURLsTopic,
			Value: sarama.ByteEncoder(messageBytes),
		})
		if err != nil {
			if s.metrics != nil && s.metrics.CreateShortLinkTotal != nil {
				s.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_send").Inc()
			}
			return fmt.Errorf("kafka send error")
		}

	} else {

		// Если Kafka недоступна, вставляем напрямую
		if err := s.InsertLink(s.ctx, originalURL, shortLink); err != nil {
			if s.metrics != nil && s.metrics.CreateShortLinkTotal != nil {
				s.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_insert").Inc()
			}
			return nil //fmt.Errorf("unavailable kafka")

		}
	}
	if s.metrics != nil && s.metrics.CreateShortLinkTotal != nil {
		s.metrics.CreateShortLinkTotal.WithLabelValues("success", "none").Inc()
	}
	return nil
}

func validateURL(originalURL, baseUrl string) error {

	parsed, err := url.Parse(originalURL)
	if err != nil {
		return fmt.Errorf("некорректный URL")
	}

	serverURL, err := url.Parse(baseUrl)
	if err != nil {
		return fmt.Errorf("некорректный BaseURL")
	}

	if parsed.Hostname() == serverURL.Hostname() {
		return fmt.Errorf("это ссылка на наш сайт, ты можешь просто перейти по ней")
	}

	if !strings.HasPrefix(originalURL, "http://") && !strings.HasPrefix(originalURL, "https://") {
		return fmt.Errorf("некорректный URL: должен начинаться с http:// или https://")
	}
	parsedURL, err := url.Parse(originalURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("некорректный формат URL")
	}
	return nil
}
