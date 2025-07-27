package service

import (
	"context"
	"linkreduction/internal/models"
	"time"
)

// Link - интерфейс для работы с хранилищем ссылок
type LinkRepo interface {
	FindByOriginalURL(ctx context.Context, originalURL string) (string, error)
	FindByShortLink(ctx context.Context, shortLink string) (string, error)
	Insert(ctx context.Context, originalURL, shortLink string) error
	InsertBatch(ctx context.Context, links []models.LinkURL) (int64, error)
	DeleteOldLinks(ctx context.Context, threshold string) (int64, error)
}

// LinkCache - интерфейс для работы с кэшем
type LinkCache interface {
	GetShortLink(ctx context.Context, originalURL string) (string, error)
	SetShortLink(ctx context.Context, originalURL, shortLink string, ttl time.Duration) error
	GetOriginalURL(ctx context.Context, shortLink string) (string, error)
	SetOriginalURL(ctx context.Context, shortLink, originalURL string, ttl time.Duration) error
}
