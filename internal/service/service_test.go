package service

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/mock"
	"linkreduction/internal/models"
	"testing"

	"linkreduction/internal/mocks"

	"github.com/stretchr/testify/assert"
)

func TestNewLinkService(t *testing.T) {
	_, mockRepo, mockCache, svc := getMocksWithService()

	assert.NotNil(t, svc)
	assert.Equal(t, mockRepo, svc.repo)
	assert.Equal(t, mockCache, svc.cache)
}

// Возврат короткой ссылки из кэша
func TestShortenURL_ReturnsFromCache(t *testing.T) {
	originalURL := "https://example.com"
	expectedShortLink := "cached123"
	baseUrl := "https://localhoist:8080"

	ctx, mockRepo, mockCache, svc := getMocksWithService()

	// Задаем поведение: кэш вернет короткую ссылку без ошибки
	mockCache.On("GetShortLink", ctx, originalURL).Return(expectedShortLink, nil)

	// Вызываем тестируемый метод
	shortLink, err := svc.ShortenURL(ctx, originalURL, baseUrl)

	// Проверяем результат
	assert.NoError(t, err)
	assert.Equal(t, expectedShortLink, shortLink)

	// Проверяем, что был вызван только кэш, а репозиторий — нет
	mockCache.AssertCalled(t, "GetShortLink", ctx, originalURL)
	mockRepo.AssertNotCalled(t, "FindByOriginalURL")
}

func TestService_ShortenURL(t *testing.T) {
	type mockBehavior func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache)

	tests := []struct {
		name         string
		originalURL  string
		baseURL      string
		mockBehavior mockBehavior
		expectedLink string
		expectError  bool
	}{
		{
			name:        "valid - found in cache",
			originalURL: "https://example.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://example.com").Return("cached123", nil)
			},
			expectedLink: "cached123",
			expectError:  false,
		},
		{
			name:        "valid - found in DB",
			originalURL: "https://db.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://db.com").Return("", nil)
				repo.On("FindByOriginalURL", ctx, "https://db.com").Return("db123", nil)
				cache.On("SetShortLink", ctx, "https://db.com", "db123", mock.Anything).Return(nil)
			},
			expectedLink: "db123",
			expectError:  false,
		},
		{
			name:        "valid - not found, generate new",
			originalURL: "https://new.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://new.com").Return("", nil)
				repo.On("FindByOriginalURL", ctx, "https://new.com").Return("", nil)
				repo.On("FindByShortLink", ctx, generateShortLink("https://new.com")).Return("", nil)
			},
			expectedLink: generateShortLink("https://new.com"),
			expectError:  false,
		},
		{
			name:        "invalid URL",
			originalURL: "htp:/invalid",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				// Не вызывается
			},
			expectedLink: "",
			expectError:  true,
		},
		{
			name:        "cache error",
			originalURL: "https://error.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://error.com").Return("", fmt.Errorf("cache down"))
			},
			expectedLink: "",
			expectError:  true,
		},
		{
			name:        "repo.FindByOriginalURL returns error",
			originalURL: "https://errordb.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://errordb.com").Return("", nil)
				repo.On("FindByOriginalURL", ctx, "https://errordb.com").Return("", fmt.Errorf("db error"))
			},
			expectedLink: "",
			expectError:  true,
		},
		{
			name:        "cache.SetShortLink returns error",
			originalURL: "https://setcache.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://setcache.com").Return("", nil)
				repo.On("FindByOriginalURL", ctx, "https://setcache.com").Return("short-set", nil)
				cache.On("SetShortLink", ctx, "https://setcache.com", "short-set", mock.Anything).Return(fmt.Errorf("cache write error"))
			},
			expectedLink: "",
			expectError:  true,
		},
		{
			name:        "repo.FindByShortLink returns error",
			originalURL: "https://shortgenerr.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://shortgenerr.com").Return("", nil)
				repo.On("FindByOriginalURL", ctx, "https://shortgenerr.com").Return("", nil)
				repo.On("FindByShortLink", ctx, generateShortLink("https://shortgenerr.com")).Return("", fmt.Errorf("lookup error"))
			},
			expectedLink: "",
			expectError:  true,
		},
		{
			name:        "all generated keys are collisions",
			originalURL: "https://collide.com",
			baseURL:     "https://localhost:8080",
			mockBehavior: func(ctx context.Context, repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetShortLink", ctx, "https://collide.com").Return("", nil)
				repo.On("FindByOriginalURL", ctx, "https://collide.com").Return("", nil)

				repo.On("FindByShortLink", ctx, generateShortLink("https://collide.com")).Return("taken", nil)
				repo.On("FindByShortLink", ctx, generateShortLink("https://collide.com_1")).Return("taken", nil)
				repo.On("FindByShortLink", ctx, generateShortLink("https://collide.com_2")).Return("taken", nil)
			},
			expectedLink: "",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, repo, cache, svc := getMocksWithService()
			tt.mockBehavior(ctx, repo, cache)

			result, err := svc.ShortenURL(ctx, tt.originalURL, tt.baseURL)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedLink, result)
			}

			cache.AssertExpectations(t)
			repo.AssertExpectations(t)
		})
	}
}

func TestService_InsertLink(t *testing.T) {
	type mockBehavior func(repo *mocks.LinkRepo, cache *mocks.LinkCache)

	tests := []struct {
		name         string
		originalURL  string
		shortLink    string
		mockBehavior mockBehavior
		expectError  bool
	}{
		{
			name:        "success",
			originalURL: "https://example.com",
			shortLink:   "short123",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				repo.On("Insert", mock.Anything, "https://example.com", "short123").Return(nil)
				cache.On("SetShortLink", mock.Anything, "https://example.com", "short123", mock.Anything).Return(nil)
			},
			expectError: false,
		},
		{
			name:        "repo.Insert returns error",
			originalURL: "https://repoerror.com",
			shortLink:   "err123",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				repo.On("Insert", mock.Anything, "https://repoerror.com", "err123").Return(fmt.Errorf("repo error"))
			},
			expectError: true,
		},
		{
			name:        "cache.SetShortLink returns error",
			originalURL: "https://cacheerror.com",
			shortLink:   "cache123",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				repo.On("Insert", mock.Anything, "https://cacheerror.com", "cache123").Return(nil)
				cache.On("SetShortLink", mock.Anything, "https://cacheerror.com", "cache123", mock.Anything).Return(fmt.Errorf("cache error"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, repo, cache, svc := getMocksWithService()
			tt.mockBehavior(repo, cache)

			err := svc.InsertLink(ctx, tt.originalURL, tt.shortLink)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			repo.AssertExpectations(t)
			cache.AssertExpectations(t)
		})
	}
}

func TestService_GetOriginalURL(t *testing.T) {
	type mockBehavior func(repo *mocks.LinkRepo, cache *mocks.LinkCache)

	tests := []struct {
		name         string
		shortLink    string
		mockBehavior mockBehavior
		expectedURL  string
		expectError  bool
	}{
		{
			name:      "found in cache",
			shortLink: "short123",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetOriginalURL", mock.Anything, "short123").Return("https://example.com", nil)
			},
			expectedURL: "https://example.com",
			expectError: false,
		},
		{
			name:      "cache error",
			shortLink: "cacheFail",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetOriginalURL", mock.Anything, "cacheFail").Return("", fmt.Errorf("cache error"))
			},
			expectedURL: "",
			expectError: true,
		},
		{
			name:      "found in DB after cache miss",
			shortLink: "db123",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetOriginalURL", mock.Anything, "db123").Return("", nil)
				repo.On("FindByShortLink", mock.Anything, "db123").Return("https://fromdb.com", nil)
				cache.On("SetOriginalURL", mock.Anything, "db123", "https://fromdb.com", mock.Anything).Return(nil)
			},
			expectedURL: "https://fromdb.com",
			expectError: false,
		},
		{
			name:      "DB returns error",
			shortLink: "dberror",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetOriginalURL", mock.Anything, "dberror").Return("", nil)
				repo.On("FindByShortLink", mock.Anything, "dberror").Return("", fmt.Errorf("db error"))
			},
			expectedURL: "",
			expectError: true,
		},
		{
			name:      "not found in cache or DB",
			shortLink: "notfound",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetOriginalURL", mock.Anything, "notfound").Return("", nil)
				repo.On("FindByShortLink", mock.Anything, "notfound").Return("", nil)
			},
			expectedURL: "",
			expectError: false,
		},
		{
			name:      "cache set fails after DB hit",
			shortLink: "setfail",
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				cache.On("GetOriginalURL", mock.Anything, "setfail").Return("", nil)
				repo.On("FindByShortLink", mock.Anything, "setfail").Return("https://setfail.com", nil)
				cache.On("SetOriginalURL", mock.Anything, "setfail", "https://setfail.com", mock.Anything).Return(fmt.Errorf("cache set error"))
			},
			expectedURL: "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, repo, cache, svc := getMocksWithService()
			tt.mockBehavior(repo, cache)

			url, err := svc.GetOriginalURL(ctx, tt.shortLink)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedURL, url)
			}

			repo.AssertExpectations(t)
			cache.AssertExpectations(t)
		})
	}
}

func TestService_InsertBatch(t *testing.T) {
	type mockBehavior func(repo *mocks.LinkRepo, cache *mocks.LinkCache)

	tests := []struct {
		name         string
		batch        []models.LinkURL
		mockBehavior mockBehavior
		expectError  bool
	}{
		{
			name:         "empty batch",
			batch:        []models.LinkURL{},
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {},
			expectError:  true,
		},
		{
			name: "successful insert and cache set",
			batch: []models.LinkURL{
				{OriginalURL: "https://example.com/1", ShortLink: "short1"},
				{OriginalURL: "https://example.com/2", ShortLink: "short2"},
			},
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				repo.On("InsertBatch", mock.Anything, mock.AnythingOfType("[]models.LinkURL")).
					Return(int64(2), nil) // <- int64 здесь!
				cache.On("SetShortLink", mock.Anything, "https://example.com/1", "short1", mock.Anything).Return(nil)
				cache.On("SetShortLink", mock.Anything, "https://example.com/2", "short2", mock.Anything).Return(nil)
			},
			expectError: false,
		},
		{
			name: "InsertBatch returns error",
			batch: []models.LinkURL{
				{OriginalURL: "https://fail.com/1", ShortLink: "fail1"},
			},
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				repo.On("InsertBatch", mock.Anything, mock.Anything).
					Return(int64(0), fmt.Errorf("insert batch error"))
			},
			expectError: true,
		},
		{
			name: "cache SetShortLink returns error",
			batch: []models.LinkURL{
				{OriginalURL: "https://example.com/1", ShortLink: "short1"},
			},
			mockBehavior: func(repo *mocks.LinkRepo, cache *mocks.LinkCache) {
				repo.On("InsertBatch", mock.Anything, mock.Anything).Return(int64(1), nil)
				cache.On("SetShortLink", mock.Anything, "https://example.com/1", "short1", mock.Anything).
					Return(fmt.Errorf("cache error"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, repo, cache, svc := getMocksWithService()

			tt.mockBehavior(repo, cache)

			err := svc.InsertBatch(ctx, tt.batch)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			repo.AssertExpectations(t)
			cache.AssertExpectations(t)
		})
	}
}

func getMocksWithService() (ctx context.Context, mockRepo *mocks.LinkRepo, mockCache *mocks.LinkCache, svc *Service) {

	ctx = context.Background()

	// Создаем моки
	mockRepo = new(mocks.LinkRepo)
	mockCache = new(mocks.LinkCache)
	svc = NewLinkService(ctx, mockRepo, mockCache, nil, nil)

	return ctx, mockRepo, mockCache, svc
}
