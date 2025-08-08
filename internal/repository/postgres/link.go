package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"linkreduction/internal/models"
	"strings"
)

// Link - реализация Link для PostgreSQL
type Link struct {
	db *sql.DB
}

// NewPostgresLinkRepository создаёт новый экземпляр Link
func NewPostgresLinkRepository(db *sql.DB) *Link {
	return &Link{db: db}
}

// FindByOriginalURL ищет короткую ссылку по оригинальному URL
func (r *Link) FindByOriginalURL(ctx context.Context, originalURL string) (string, error) {
	var shortLink string
	err := r.db.QueryRowContext(ctx, "SELECT short_link FROM links WHERE link = $1", originalURL).Scan(&shortLink)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return shortLink, nil
}

// FindByShortLink ищет оригинальный URL по короткой ссылке
func (r *Link) FindByShortLink(ctx context.Context, shortLink string) (string, error) {
	var originalURL string
	err := r.db.QueryRowContext(ctx, "SELECT link FROM links WHERE short_link = $1", shortLink).Scan(&originalURL)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return originalURL, nil
}

// Insert вставляет новую ссылку в базу данных
func (r *Link) Insert(ctx context.Context, originalURL, shortLink string) error {
	_, err := r.db.ExecContext(ctx, "INSERT INTO links (link, short_link) VALUES ($1, $2) ON CONFLICT (link) DO NOTHING", originalURL, shortLink)
	if err != nil {
		return err
	}
	return nil
}

// InsertBatch выполняет пакетную вставку ссылок
func (r *Link) InsertBatch(ctx context.Context, links []models.LinkURL) (int64, error) {
	if len(links) == 0 {
		return 0, nil
	}

	const batchSize = 10 // Размер подгруппы для пакетной вставки
	var totalRowsAffected int64

	// Разбиваем батч на подгруппы по batchSize
	for i := 0; i < len(links); i += batchSize {
		end := i + batchSize
		if end > len(links) {
			end = len(links)
		}
		batch := links[i:end]

		query := `INSERT INTO links (link, short_link) VALUES %s ON CONFLICT (link) DO NOTHING`
		placeholders := make([]string, 0, len(batch))
		values := make([]interface{}, 0, len(batch)*2)

		for j, link := range batch {
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d)", j*2+1, j*2+2))
			values = append(values, link.OriginalURL, link.ShortLink)
		}

		query = fmt.Sprintf(query, strings.Join(placeholders, ","))

		// Выполняем запрос
		result, err := r.db.ExecContext(ctx, query, values...)
		if err != nil {
			// Логируем ошибку, но продолжаем с следующим батчем
			// Можно добавить метрику или логирование здесь
			continue
		}

		// Суммируем затронутые строки
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			// Можно Логируем ошибку, но продолжаем
			continue
		}
		totalRowsAffected += rowsAffected
	}

	return totalRowsAffected, nil
}

// DeleteOldLinks удаляет ссылки старше указанного порога
func (r *Link) DeleteOldLinks(ctx context.Context, threshold string) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM links WHERE created_at < NOW() - INTERVAL $1", threshold)
	if err != nil {
		return err
	}
	return nil
}
