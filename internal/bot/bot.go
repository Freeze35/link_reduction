package bot

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/IBM/sarama"
	tele "gopkg.in/telebot.v4"
	"linkreduction/internal/config"
	initprometheus "linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"log"
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
		Client: httpClient,
	}

	newBot, err := tele.NewBot(pref)
	if err != nil {
		return err
	}
	log.Printf("bot is running")
	b := &Bot{ctx, cfg, newBot, service, producer, metrics}
	b.registerHandlers()
	go newBot.Start()
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

	baseURL := b.cfg.Server.BaseURL

	shortLink, err := b.service.ShortenURL(b.ctx, originalURL, baseURL)
	if err != nil {
		c.Send(err.Error())
		return fmt.Errorf("shorten URL: %w", err)
	}

	shortURL := fmt.Sprintf("%s/%s", baseURL, shortLink)

	err = b.service.SendMessageToDB(originalURL, shortURL)
	if err != nil {
		c.Send(err.Error())
		return err
	}

	c.Send(shortURL)
	return nil
}
