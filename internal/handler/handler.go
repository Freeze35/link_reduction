package handler

import (
	_ "context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/config"
	"linkreduction/internal/kafka"
	"linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"net/http"
)

type Handler struct {
	service  *service.Service
	metrics  *initprometheus.PrometheusMetrics
	logger   *logrus.Logger
	producer sarama.SyncProducer
	cfg      *config.Config
}

type ShortenRequest struct {
	URL string `json:"url"`
}

type ShortenMessage struct {
	OriginalURL string `json:"original_url"`
	ShortLink   string `json:"short_link"`
}

func NewHandler(service *service.Service, metrics *initprometheus.PrometheusMetrics, logger *logrus.Logger, cfg *config.Config) (*Handler, error) {

	return &Handler{
		service: service,
		metrics: metrics,
		logger:  logger,
		cfg:     cfg,
	}, nil
}

func (h *Handler) InitRoutes(app *fiber.App) {
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Post("/createShortLink", h.createShortLink)
	app.Get("/:key", h.redirect)
}

// restrictBodySize проверяет, что размер тела запроса не превышает заданный лимит
func (h *Handler) restrictBodySize(c *fiber.Ctx, maxBodySize int) error {
	bodySize := len(c.Request().Body())
	if bodySize > maxBodySize {
		h.logger.WithFields(logrus.Fields{
			"body_size":  bodySize,
			"client_ip":  c.IP(),
			"request_id": c.Get("X-Request-ID"),
		}).Warn("Слишком большой размер тела запроса")

		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("размер тела запроса (%d байт) превышает лимит (%d байт)", bodySize, maxBodySize),
		})
	}
	return nil
}

func respondError(c *fiber.Ctx, status int, msg string) (string, error) {
	return "", c.Status(status).JSON(fiber.Map{"error": msg})
}

func (h *Handler) checkOriginalURL(c *fiber.Ctx) (string, error) {
	const maxBodySize = 2048

	if err := h.restrictBodySize(c, maxBodySize); err != nil {
		return "", err
	}

	if c.Get("Content-Type") != "application/json" {
		return respondError(c, http.StatusBadRequest, "неверный Content-Type != application/json")
	}

	var req ShortenRequest
	if err := c.BodyParser(&req); err != nil {
		if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "json_parse").Inc()
		}
		return respondError(c, http.StatusBadRequest, fmt.Sprintf("некорректное тело JSON: %v", err))
	}

	if req.URL == "" {
		return respondError(c, http.StatusBadRequest, "URL обязателен")
	}

	return req.URL, nil
}

func (h *Handler) createShortLink(c *fiber.Ctx) error {

	baseURL := h.cfg.Server.BaseURL

	originalURL, err := h.checkOriginalURL(c)

	ctx := c.Context()
	shortLink, err := h.service.ShortenURL(ctx, originalURL)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	shortURL := fmt.Sprintf("%s/%s", baseURL, shortLink)

	// Если Kafka доступна, отправляем сообщение
	if h.producer != nil {
		message := &ShortenMessage{OriginalURL: originalURL, ShortLink: shortLink}
		messageBytes, err := json.Marshal(message)
		if err != nil {

			if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
				h.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_serialization").Inc()
			}
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка сериализации: %v", err),
			})
		}

		_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{
			Topic: kafka.ShortenURLsTopic,
			Value: sarama.ByteEncoder(messageBytes),
		})
		if err != nil {
			if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
				h.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_send").Inc()
			}
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка отправки в Kafka: %v", err),
			})
		}

	} else {
		// Если Kafka недоступна, вставляем напрямую
		if err := h.service.InsertLink(ctx, originalURL, shortLink); err != nil {
			if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
				h.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_insert").Inc()
			}
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": "internal server error",
			})
		}
	}
	if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
		h.metrics.CreateShortLinkTotal.WithLabelValues("success", "none").Inc()
	}
	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"shortURL": shortURL,
	})
}

func (h *Handler) redirect(c *fiber.Ctx) error {

	shortLink := c.Params("key")
	ctx := c.Context()

	originalURL, err := h.service.GetOriginalURL(ctx, shortLink)
	if err != nil {
		if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
			h.metrics.RedirectTotal.WithLabelValues("error", "db_query").Inc()
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": "internal server error",
		})
	}
	if originalURL == "" {
		if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
			h.metrics.RedirectTotal.WithLabelValues("not_found", "none").Inc()
		}
		return c.Status(http.StatusNotFound).JSON(fiber.Map{
			"error": "Короткая ссылка не найдена",
		})
	}

	if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
		h.metrics.RedirectTotal.WithLabelValues("success", "none").Inc()
	}

	return c.Redirect(originalURL, http.StatusMovedPermanently)
}
