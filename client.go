package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Client interface {
	PublishMessageAsync(ctx context.Context, topic string, message []byte, wg *sync.WaitGroup)
	PublishMessage(ctx context.Context, topic string, message []byte) error
	StartConsume(ctx context.Context, messages chan<- []byte) error
	Ping(ctx context.Context) error
	GetConfig() Config
	Close()
}

type client struct {
	c      *kgo.Client
	cfg    Config
	logger kgo.Logger
}

func NewClient(ctx context.Context, options ...Option) (Client, error) {
	config := initConfig(options...)

	o := make([]kgo.Opt, 0)
	o = append(
		o,
		kgo.SeedBrokers(config.brokerList...),
		kgo.ConsumerGroup(config.groupID),
		kgo.ConsumeTopics(config.topics...),
		kgo.FetchMaxBytes(config.maxBytes),
		kgo.ConnIdleTimeout(config.connIdleTimeout),
		kgo.WithLogger(config.logger),
	)
	if config.saslMechanism.Name() != string(SaslPlainMechanismOption) {
		o = append(o, kgo.SASL(config.saslMechanism))
	}

	c, err := kgo.NewClient(o...)
	if err != nil {
		return nil, err
	}

	if err := c.Ping(ctx); err != nil {
		return nil, err
	}
	return &client{
		c:      c,
		cfg:    config,
		logger: config.logger,
	}, nil
}

// PublishMessageAsync - асинхронная отправка сообщения в топик
func (c *client) PublishMessageAsync(ctx context.Context, topic string, message []byte, wg *sync.WaitGroup) {
	c.c.Produce(ctx, &kgo.Record{Topic: topic, Value: message}, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			c.logger.Log(kgo.LogLevelError, "record had a produce error: ", err)
		}
	})
}

// PublishMessage - отправка сообщения в топик
func (c *client) PublishMessage(ctx context.Context, topic string, message []byte) error {
	if err := c.c.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: message}).FirstErr(); err != nil {
		return fmt.Errorf("record had a produce error: %v", err)
	}
	return nil
}

// StartConsume - запуск потребления сообщений
func (c *client) StartConsume(ctx context.Context, message chan<- []byte) error {
	errors := make(chan error, 1)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			fetches := c.c.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				errors <- fmt.Errorf(fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				message <- record.Value
			}
		}
	}()

	select {
	case <-ctx.Done():
		c.logger.Log(kgo.LogLevelInfo, "message consuming has stopped!")
		return nil
	case sig := <-quit:
		return fmt.Errorf("caught signal: %v [terminating]", sig)
	case errorsMsg := <-errors:
		return errorsMsg
	}
}

func (c *client) Ping(ctx context.Context) error {
	return c.c.Ping(ctx)
}

func (c *client) GetConfig() Config {
	return c.cfg
}

func (c *client) Close() {
	c.c.Close()
}
