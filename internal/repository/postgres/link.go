package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"linkreduction/internal/models"
	"slices"
	"strings"
)

type Link struct {
	db *sql.DB
}

func NewPostgresLinkRepository(db *sql.DB) *Link {
	return &Link{db: db}
}

func (r *Link) FindByOriginalURL(ctx context.Context, originalURL string) (string, error) {
	var shortLink string
	err := r.db.QueryRowContext(ctx, "SELECT short_link FROM links WHERE link = $1", originalURL).Scan(&shortLink)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return shortLink, err
}

func (r *Link) FindByShortLink(ctx context.Context, shortLink string) (string, error) {
	var originalURL string
	err := r.db.QueryRowContext(ctx, "SELECT link FROM links WHERE short_link = $1", shortLink).Scan(&originalURL)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return originalURL, err
}

func (r *Link) Insert(ctx context.Context, originalURL, shortLink string) error {
	_, err := r.db.ExecContext(ctx, "INSERT INTO links (link, short_link) VALUES ($1, $2) ON CONFLICT (link) DO NOTHING", originalURL, shortLink)
	return err
}

func (r *Link) InsertBatch(ctx context.Context, links []models.LinkURL) error {
	if len(links) == 0 {
		return nil
	}

	const batchSize = 10

	for batch := range slices.Chunk(links, batchSize) {

		query := `INSERT INTO links (link, short_link) VALUES %s ON CONFLICT (link) DO NOTHING`
		placeholders := make([]string, 0, len(batch))
		values := make([]interface{}, 0, len(batch)*2)

		for j, link := range batch {
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d)", j*2+1, j*2+2))
			values = append(values, link.OriginalURL, link.ShortLink)
		}

		query = fmt.Sprintf(query, strings.Join(placeholders, ","))

		_, err := r.db.ExecContext(ctx, query, values...)
		if err != nil {
			continue
		}

	}

	return nil
}

// DeleteOldLinks удаляет ссылки старше указанного порога
func (r *Link) DeleteOldLinks(ctx context.Context, threshold string) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM links WHERE created_at < NOW() - INTERVAL $1", threshold)
	if err != nil {
		return err
	}
	return nil
}
