package cmd

import (
	"context"
	"fmt"
	"linkreduction/internal/config"
	"linkreduction/internal/repository/postgres"
	"linkreduction/internal/repository/redis"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"linkreduction/internal/handler"
	"linkreduction/internal/kafka"
	"linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"linkreduction/migrations"
)

var shortenCmd = &cobra.Command{
	Use:   "shorten",
	Short: "Run the link shortening server",
	Run: func(cmd *cobra.Command, args []string) {

		filePath, _ := cmd.Flags().GetString("file")
		if filePath == "" {
			filePath = "internal/config/config.yaml"
		}

		absPath, err := filepath.Abs(filePath)
		if err != nil {
			fmt.Printf("Ошибка разрешения пути к файлу: %v\n", err)
			os.Exit(1)
		}

		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			fmt.Printf("Файл не найден: %s\n", absPath)
			os.Exit(1)
		} else if err != nil {
			fmt.Printf("Ошибка проверки файла: %v\n", err)
			os.Exit(1)
		}

		logger := logrus.New()
		logger.SetFormatter(&logrus.JSONFormatter{})
		logger.SetLevel(logrus.InfoLevel)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg, err := config.LoadConfig(absPath)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Fatal("Ошибка загрузки конфигурационного файла")
		}

		migrations.RunMigrations(logger, &cfg)

		db, err := handler.InitPostgres(&cfg)
		if err != nil {
			logger.Fatal("Ошибка инициализации базы данных")
		}
		defer func() {
			if err := db.Close(); err != nil {
				logger.Fatal("Ошибка при закрытии базы данных")
			}
		}()

		redisClient, err := handler.RedisConnect(ctx, &cfg)
		if err != nil {
			logger.Fatal("Ошибка инициализации Redis")
		}
		defer func() {
			if err := redisClient.Close(); err != nil {
				logger.Fatal("Ошибка при закрытии Redis соединения")
			}
		}()

		kafkaProducer, err := handler.InitKafkaProducer(&cfg)
		if err != nil {
			logger.Info("Ошибка инициализации Kafka. Kafka недоступна")
		}
		if kafkaProducer != nil {
			defer func() {
				if err := kafkaProducer.Close(); err != nil {
					logger.Fatal("Ошибка при закрытии Kafka соединения")
				}
			}()
		}

		metrics := initprometheus.InitPrometheus()

		linkRepo := postgres.NewPostgresLinkRepository(db)
		cache := redis.NewLink(redisClient, logger)

		linkService := service.NewLinkService(linkRepo, cache)

		kafkaConsumer := kafka.NewConsumer(ctx, kafkaProducer,
			linkRepo, cache, logger, linkService, &cfg)

		h, err := handler.NewHandler(linkService, metrics, logger, &cfg)
		if err != nil {
			logger.Fatal("Ошибка инициализации обработчика")
		}

		app := fiber.New()
		h.InitRoutes(app)

		errChan := make(chan error, 1)

		if kafkaConsumer != nil {
			go func() {
				errChan <- kafkaConsumer.ConsumeShortenURLs()
			}()
		}

		go func() {
			select {
			case err := <-errChan:
				if err != nil {
					log.Printf("Kafka consumer завершился с ошибкой: %v", err)
					// Можно попытаться перезапустить consumer или завершить процесс
					// С учётом того что kafka может отсутствовать. Это возможно проигнорировать
				}
			}
		}()

		//
		go linkService.CleanupOldLinks()

		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		serverErr := make(chan error, 1)

		go func() {
			logger.WithField("component", "shorten").Info("Сервер запущен на http://localhost:8080")
			if err := app.Listen(":8080"); err != nil {
				serverErr <- err
			}
		}()

		select {
		case sig := <-quit:
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"signal":    sig,
			}).Info("Получен системный сигнал")
		case err := <-serverErr:
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Error("Ошибка сервера")
		}

		logger.WithField("component", "shorten").Info("Остановка сервера...")
		if err := app.Shutdown(); err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Error("Ошибка при завершении сервера")
		}

		logger.WithField("component", "shorten").Info("Сервер успешно остановлен")
	},
}

func init() {
	rootCmd.AddCommand(shortenCmd)
	shortenCmd.Flags().StringP("file", "f", "", "Путь к файлу конфигурации (по умолчанию: .env в текущей директории)")
}
