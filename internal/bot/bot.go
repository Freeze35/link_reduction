package bot

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	tele "gopkg.in/telebot.v4"
	"linkreduction/internal/config"
	"linkreduction/internal/kafka"
	initprometheus "linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Bot struct {
	ctx      context.Context
	cfg      *config.Config
	bot      *tele.Bot
	service  *service.Service
	producer sarama.SyncProducer
	metrics  *initprometheus.PrometheusMetrics
}

func StartBot(ctx context.Context, cfg *config.Config, service *service.Service, producer sarama.SyncProducer, metrics *initprometheus.PrometheusMetrics) error {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Timeout: 10 * time.Second,
	}

	pref := tele.Settings{
		Token:  cfg.BotToken,
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
		Client: httpClient, // ← подключаем кастомный http-клиент
	}

	newBot, err := tele.NewBot(pref)
	if err != nil {
		return err
	}
	log.Printf("bot is running")
	b := &Bot{ctx, cfg, newBot, service, producer, metrics}
	b.registerHandlers()
	go newBot.Start() // не блокируем основную горутину
	return nil
}

func (b *Bot) registerHandlers() {
	b.bot.Handle("/start", func(c tele.Context) error {
		return c.Send("Я помогу тебе превратить любую длинную ссылку в короткую 🔗\n\nПросто отправь мне свой URL, и я создам сокращённый адрес, который можно использовать где угодно — в соцсетях, мессенджерах, на сайтах. При переходе по нему пользователь будет перенаправлен на исходную страницу.")
	})

	b.bot.Handle(tele.OnText, b.handleShortenRequest)
}

func (b *Bot) handleShortenRequest(c tele.Context) error {
	originalURL := c.Text()

	err := b.validateOriginalURL(originalURL)

	if err != nil {
		b.registerHandlers()
		return c.Send("Ошибка: " + err.Error())
	}

	baseURL := b.cfg.Server.BaseURL

	shortLink, err := b.service.ShortenURL(b.ctx, originalURL)
	if err != nil {
		b.registerHandlers()
		return c.Send("internal server error")
	}

	shortURL := fmt.Sprintf("%s/%s", baseURL, shortLink)

	// Если Kafka доступна, отправляем сообщение
	if b.producer != nil {
		message := &kafka.ShortenMessage{OriginalURL: originalURL, ShortLink: shortLink}
		messageBytes, err := json.Marshal(message)
		if err != nil {

			if b.metrics != nil && b.metrics.CreateShortLinkTotal != nil {
				b.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_serialization").Inc()
			}
			b.registerHandlers()
			return c.Send("internal server error")
		}

		_, _, err = b.producer.SendMessage(&sarama.ProducerMessage{
			Topic: kafka.ShortenURLsTopic,
			Value: sarama.ByteEncoder(messageBytes),
		})
		if err != nil {
			if b.metrics != nil && b.metrics.CreateShortLinkTotal != nil {
				b.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_send").Inc()
			}
			b.registerHandlers()
			return c.Send("internal server error")
		}

	} else {
		// Если Kafka недоступна, вставляем напрямую
		if err := b.service.InsertLink(b.ctx, originalURL, shortLink); err != nil {
			if b.metrics != nil && b.metrics.CreateShortLinkTotal != nil {
				b.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_insert").Inc()
			}
			b.registerHandlers()
			return c.Send("internal server error")
		}
	}
	if b.metrics != nil && b.metrics.CreateShortLinkTotal != nil {
		b.metrics.CreateShortLinkTotal.WithLabelValues("success", "none").Inc()
	}
	b.registerHandlers()
	return c.Send(shortURL)
}

func (b *Bot) validateOriginalURL(originalURL string) error {

	maxURLLength := 2048

	if originalURL == "" {
		return errors.New("URL обязателен")
	}

	hostName := b.cfg.Server.HostName

	if strings.Contains(originalURL, hostName) {
		return errors.New("это ссылка на наш сайт ты можешь просто перейти по ней")
	}

	// Ограничение по размеру в байтах
	if len(originalURL) > maxURLLength {
		return errors.New("URL превышает максимально допустимую длину 2048 байт")
	}

	// Проверка схемы
	if !strings.HasPrefix(originalURL, "http://") && !strings.HasPrefix(originalURL, "https://") {
		return errors.New("URL должен начинаться с http:// или https://")
	}

	// Проверка валидности URL
	_, err := url.ParseRequestURI(originalURL)
	if err != nil {
		return errors.New("некорректный URL")
	}

	return nil
}
