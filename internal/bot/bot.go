package bot

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	tele "gopkg.in/telebot.v4"
	"linkreduction/internal/config"
	initprometheus "linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"net/http"
	"time"
)

type Bot struct {
	ctx      context.Context
	cfg      *config.Config
	bot      *tele.Bot
	service  *service.Service
	producer sarama.SyncProducer
	metrics  *initprometheus.PrometheusMetrics
	logger   *logrus.Logger
}

func StartBot(ctx context.Context, cfg *config.Config, service *service.Service, producer sarama.SyncProducer, metrics *initprometheus.PrometheusMetrics, logger *logrus.Logger) error {
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
		Client: httpClient,
	}

	newBot, err := tele.NewBot(pref)
	if err != nil {
		return err
	}

	b := &Bot{ctx, cfg, newBot, service, producer, metrics, logger}
	b.registerHandlers()
	go newBot.Start()
	return nil
}

func (b *Bot) registerHandlers() {
	b.bot.Handle("/start", func(c tele.Context) error {
		return c.Send("Я помогу тебе превратить любую длинную ссылку в короткую " +
			"🔗\n\nПросто отправь мне свой URL, и я создам сокращённый адрес, " +
			"который можно использовать где угодно — в соцсетях, мессенджерах, на сайтах. " +
			"При переходе по нему пользователь будет перенаправлен на исходную страницу.")
	})

	b.bot.Handle(tele.OnText, b.handleShortenRequest)
}

func (b *Bot) handleShortenRequest(c tele.Context) error {
	originalURL := c.Text()

	baseURL := b.cfg.Server.BaseURL

	shortLink, err := b.service.ShortenURL(b.ctx, originalURL, baseURL)
	if err != nil {
		err := c.Send(err.Error())
		if err != nil {
			b.logger.Error(err)
		}
		return fmt.Errorf("shorten URL: %w", err)
	}

	err = b.service.SendMessageToDB(originalURL, shortLink)
	if err != nil {
		err := c.Send(err.Error())
		if err != nil {
			b.logger.Error(err)
		}
		return err
	}

	shortURL := fmt.Sprintf("%s/%s", baseURL, shortLink)

	err = c.Send(shortURL)
	if err != nil {
		b.logger.Error(err)
		return err
	}

	return nil
}
