package kafka

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/klogrus"
)

const (
	connIdleTimeout = 1 * time.Minute
	fetchMaxBytes   = 50 << 10 // 50 KiB
)

type Config interface {
	GetBrokerList() []string
	GetTopics() []string
	GetGroupID() string
}

type cfg struct {
	credentials      auth
	brokerList       []string
	topics           []string
	saslMechanismOpt SaslMechanismOption
	saslMechanism    sasl.Mechanism
	groupID          string
	connIdleTimeout  time.Duration
	maxBytes         int32
	logger           kgo.Logger
}

type auth struct {
	username string
	password string
}

func initConfig(options ...Option) *cfg {
	config := defaultConfig()
	for _, opt := range options {
		opt.apply(config)
	}

	switch config.saslMechanismOpt {
	case SaslPlainMechanismOption:
		config.saslMechanism = plain.Auth{
			User: config.credentials.username,
			Pass: config.credentials.password,
		}.AsMechanism()
	case SaslSha256MechanismOption:
		config.saslMechanism = scram.Auth{
			User: config.credentials.username,
			Pass: config.credentials.password,
		}.AsSha256Mechanism()
	case SaslSha512MechanismOption:
		config.saslMechanism = scram.Auth{
			User: config.credentials.username,
			Pass: config.credentials.password,
		}.AsSha512Mechanism()
	}

	return config
}

func defaultConfig() *cfg {
	return &cfg{
		brokerList:      []string{},
		topics:          []string{},
		groupID:         "",
		connIdleTimeout: connIdleTimeout,
		maxBytes:        fetchMaxBytes,
		logger:          klogrus.New(logrus.New()),
	}
}

func (c *cfg) GetBrokerList() []string {
	return c.brokerList
}

func (c *cfg) GetTopics() []string {
	return c.topics
}

func (c *cfg) GetGroupID() string {
	return c.groupID
}
