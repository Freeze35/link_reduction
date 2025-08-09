package redis

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"time"
)

type Link struct {
	client *redis.Client
	logger *logrus.Logger
}

func NewLink(client *redis.Client, logger *logrus.Logger) *Link {
	return &Link{client: client, logger: logger}
}

func (c *Link) GetShortLink(ctx context.Context, originalURL string) (string, error) {
	cacheKey := "shorten:" + originalURL
	result, err := c.client.Get(ctx, cacheKey).Result()
	if errors.Is(err, redis.Nil) || err != nil {
		return "", nil
	}
	return result, nil
}

func (c *Link) SetShortLink(ctx context.Context, originalURL, shortLink string, ttl time.Duration) error {
	cacheKey := "shorten:" + originalURL
	if err := c.client.Set(ctx, cacheKey, shortLink, ttl).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Link) GetOriginalURL(ctx context.Context, shortLink string) (string, error) {
	cacheKey := "redirect:" + shortLink
	result, err := c.client.Get(ctx, cacheKey).Result()
	if errors.Is(err, redis.Nil) || err != nil {
		return "", nil
	}
	return result, nil
}

func (c *Link) SetOriginalURL(ctx context.Context, shortLink, originalURL string, ttl time.Duration) error {
	cacheKey := "redirect:" + shortLink
	if err := c.client.Set(ctx, cacheKey, originalURL, ttl).Err(); err != nil {
		return err
	}
	return nil
}
