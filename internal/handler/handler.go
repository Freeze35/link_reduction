package handler

import (
	"context"
	_ "context"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/config"
	"linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"net/http"
)

type Handler struct {
	ctx     context.Context
	service *service.Service
	metrics *initprometheus.PrometheusMetrics
	logger  *logrus.Logger
	cfg     *config.Config
}

type ShortenRequest struct {
	URL string `json:"url"`
}

type ShortenMessage struct {
	OriginalURL string `json:"original_url"`
	ShortLink   string `json:"short_link"`
}

func NewHandler(ctx context.Context, service *service.Service, metrics *initprometheus.PrometheusMetrics, logger *logrus.Logger, cfg *config.Config) (*Handler, error) {

	return &Handler{
		service: service,
		metrics: metrics,
		logger:  logger,
		cfg:     cfg,
		ctx:     ctx,
	}, nil
}

func (h *Handler) InitRoutes(app *fiber.App) {
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Post("/createShortLink", h.createShortLink)
	app.Get("/:key", h.redirect)
}

func (h *Handler) restrictBodySize(c *fiber.Ctx, maxBodySize int) error {
	bodySize := len(c.Request().Body())
	if bodySize > maxBodySize {
		h.logger.WithFields(logrus.Fields{
			"body_size":  bodySize,
			"client_ip":  c.IP(),
			"request_id": c.Get("X-Request-ID"),
		}).Warn("Слишком большой размер тела запроса")
		return respondError(c, false, h.logger, http.StatusBadRequest,
			fmt.Sprintf("размер тела запроса (%d байт) "+
				"превышает лимит (%d байт)", bodySize, maxBodySize))
	}
	return nil
}

func (h *Handler) checkOriginalURL(c *fiber.Ctx) (string, error) {
	const maxBodySize = 2048

	if err := h.restrictBodySize(c, maxBodySize); err != nil {

		return "", err
	}

	if c.Get("Content-Type") != "application/json" {
		return "", fmt.Errorf("неверный Content-Type != application/json")
	}

	var req ShortenRequest
	if err := c.BodyParser(&req); err != nil {
		if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "json_parse").Inc()
		}
		return "", fmt.Errorf("некорректное тело JSON: %v", err)
	}

	if req.URL == "" {
		return "", fmt.Errorf("URL обязателен")
	}

	return req.URL, nil
}

func (h *Handler) createShortLink(c *fiber.Ctx) error {

	baseURL := h.cfg.Server.BaseURL

	originalURL, err := h.checkOriginalURL(c)
	if err != nil {
		return respondError(c, true, h.logger, http.StatusBadRequest, err.Error())
	}

	shortLink, err := h.service.ShortenURL(h.ctx, originalURL, baseURL)
	if err != nil {
		return respondError(c, true, h.logger, http.StatusBadRequest, err.Error())
	}

	err = h.service.SendMessageToDB(originalURL, shortLink)
	if err != nil {
		return respondError(c, false, h.logger, http.StatusBadRequest, err.Error())
	}

	shortURL := fmt.Sprintf("%s/%s", baseURL, shortLink)

	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"shortURL": shortURL,
	})
}

func (h *Handler) redirect(c *fiber.Ctx) error {

	shortLink := c.Params("key")

	originalURL, err := h.service.GetOriginalURL(h.ctx, shortLink)
	if err != nil {
		if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
			h.metrics.RedirectTotal.WithLabelValues("error", "db_query").Inc()
		}
		return respondError(c, false, h.logger, http.StatusBadRequest, fmt.Sprintf("internal server error get original URL: %v", err))
	}
	if originalURL == "" {
		if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
			h.metrics.RedirectTotal.WithLabelValues("not_found", "none").Inc()
		}
		return respondError(c, false, h.logger, http.StatusBadRequest, "Короткая ссылка не найдена")
	}

	if h.metrics != nil && h.metrics.CreateShortLinkTotal != nil {
		h.metrics.RedirectTotal.WithLabelValues("success", "none").Inc()
	}

	return c.Redirect(originalURL, http.StatusMovedPermanently)
}

func respondError(c *fiber.Ctx, show bool, logger *logrus.Logger, status int, msg string) error {
	logger.Error(msg)
	if show {
		return c.Status(status).JSON(fiber.Map{"error": msg})
	}
	return c.Status(status).JSON(fiber.Map{"error": "internal server error"})
}
