package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"io"
	"linkreduction/internal/config"
	"linkreduction/internal/const"
	"linkreduction/internal/models"
	"linkreduction/internal/service"
	"log"
	"strings"
	"time"
)

type Consumer struct {
	ctx         context.Context
	producer    sarama.SyncProducer
	logger      *logrus.Logger
	linkService *service.Service
	cfg         *config.Config
	batchChan   chan models.LinkURL
}

func NewConsumer(ctx context.Context, producer sarama.SyncProducer,
	logger *logrus.Logger, linkService *service.Service, cfg *config.Config) *Consumer {

	batchChan := make(chan models.LinkURL)

	return &Consumer{
		ctx:         ctx,
		producer:    producer,
		logger:      logger,
		linkService: linkService,
		cfg:         cfg,
		batchChan:   batchChan,
	}
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batchSize := 50
	batchTimeout := 10 * time.Second

	go c.processBatchInsert(c.ctx, c.batchChan, batchSize, batchTimeout)

	err := c.processKafkaMessages(session, claim, c.batchChan)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) ConsumeShortenURLs() error {
	kafkaEnv := c.cfg.Kafka.Brokers
	kafkaBrokers := strings.Split(kafkaEnv, ",")

	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		return fmt.Errorf("переменная окружения KAFKA_BROKERS пуста или не задана, пропуск создания consumer group")
	}

	for _, broker := range kafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("обнаружен пустой адрес брокера Kafka, пропуск создания consumer group")
		}
	}

	sconfig := sarama.NewConfig()
	sconfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	var consumerGroup sarama.ConsumerGroup
	var err error
	for i := 0; i < 10; i++ {
		consumerGroup, err = sarama.NewConsumerGroup(kafkaBrokers, message.ShortenURLsGroup, sconfig)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		//sarama logger off
		sarama.Logger = log.New(io.Discard, "", 0)
		return nil
	}

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("Остановка потребления сообщений Kafka")
			err := consumerGroup.Close()
			if err != nil {
				c.logger.WithError(err).Error("Ошибка при закрытии Kafka consumer group")
			}
			return nil
		default:
			err := consumerGroup.Consume(c.ctx, []string{message.ShortenURLsTopic}, c)
			if err != nil {
				c.logger.Error("Ошибка потребления сообщений Kafka")
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (c *Consumer) deserializeMessage(msg *sarama.ConsumerMessage) (message.ShortenMessage, error) {
	var shortenMsg message.ShortenMessage
	if err := json.Unmarshal(msg.Value, &shortenMsg); err != nil {
		c.logger.WithError(err).Error("Ошибка при разборе сообщения Kafka")
		return message.ShortenMessage{}, err
	}
	return shortenMsg, nil
}

func (c *Consumer) processBatchInsert(ctx context.Context, batchChan <-chan models.LinkURL, batchSize int, batchTimeout time.Duration) {
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	childCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch := make([]models.LinkURL, 0, batchSize)
	for {
		select {
		case msg, ok := <-batchChan:
			if !ok {
				if len(batch) > 0 {
					if err := c.linkService.InsertBatch(ctx, batch); err != nil {
						c.logger.WithFields(logrus.Fields{
							"batch_size": len(batch),
						}).Error("Ошибка при вставке последнего батча: ", err)
					}
				}
				c.logger.Info("Канал batchChan закрыт, завершение пакетной вставки")
				return
			}
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				if err := c.linkService.InsertBatch(ctx, batch); err != nil {
					c.logger.WithFields(logrus.Fields{
						"batch_size": len(batch),
					}).Error("Ошибка при вставке батча: ", err)
				}
				batch = batch[:0]
				ticker.Reset(batchTimeout)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := c.linkService.InsertBatch(ctx, batch); err != nil {
					c.logger.WithFields(logrus.Fields{
						"batch_size": len(batch),
					}).Error("Ошибка при вставке батча по таймеру: ", err)
				}
				batch = batch[:0]
				ticker.Reset(batchTimeout)
			}
		case <-ctx.Done():
			if len(batch) > 0 {

				if err := c.linkService.InsertBatch(childCtx, batch); err != nil {
					c.logger.WithFields(logrus.Fields{
						"batch_size": len(batch),
					}).Error("Ошибка при вставке батча при завершении: ", err)
				}
			}
			c.logger.Debug("Контекст отменён, завершение пакетной вставки")
			return
		}
	}
}

func (c *Consumer) processKafkaMessages(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim, batchChan chan<- models.LinkURL) error {
	for consumerMessage := range claim.Messages() {
		shortenMsg, err := c.deserializeMessage(consumerMessage)
		if err != nil {
			continue
		}

		select {
		case batchChan <- models.LinkURL{OriginalURL: shortenMsg.OriginalURL, ShortLink: shortenMsg.ShortLink}:
			session.MarkMessage(consumerMessage, "")
			c.logger.WithFields(logrus.Fields{
				"original_url": shortenMsg.OriginalURL,
				"short_link":   shortenMsg.ShortLink,
			}).Info("Отправлено сообщение в batchChan")
			return nil
		case <-c.ctx.Done():
			c.logger.Info("Контекст отменён, прекращение отправки в batchChan")
			return c.ctx.Err()
		}

	}
	return nil
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) CloseKafka() {
	close(c.batchChan)
}
