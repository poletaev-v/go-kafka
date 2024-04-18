package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Option interface {
	apply(*cfg)
}

type SaslMechanismOption string

const (
	SaslPlainMechanismOption  SaslMechanismOption = "PLAIN"
	SaslSha256MechanismOption SaslMechanismOption = "SCRAM-SHA-256"
	SaslSha512MechanismOption SaslMechanismOption = "SCRAM-SHA-512"
)

type (
	clientOption struct {
		fn func(*cfg)
	}
	producerOption struct {
		fn func(*cfg)
	}
	consumerOption struct {
		fn func(*cfg)
	}
)

func (opt clientOption) apply(cfg *cfg) {
	opt.fn(cfg)
}

func (opt producerOption) apply(cfg *cfg) {
	opt.fn(cfg)
}

func (opt consumerOption) apply(cfg *cfg) {
	opt.fn(cfg)
}

func Auth(username, password string) Option {
	return clientOption{func(cfg *cfg) {
		cfg.credentials = auth{
			username: username,
			password: password,
		}
	}}
}

func SaslMechanism(saslMechanism SaslMechanismOption) Option {
	return clientOption{func(cfg *cfg) {
		cfg.saslMechanismOpt = saslMechanism
	}}
}

func BrokerList(seeds []string) Option {
	return clientOption{func(cfg *cfg) { cfg.brokerList = append(cfg.brokerList[:0], seeds...) }}
}

func ConnIdleTimeout(timeout time.Duration) Option {
	return clientOption{func(cfg *cfg) { cfg.connIdleTimeout = timeout }}
}

func WithLogger(logger kgo.Logger) Option {
	return clientOption{func(cfg *cfg) { cfg.logger = logger }}
}

func ConsumerGroup(groupID string) Option {
	return consumerOption{func(cfg *cfg) { cfg.groupID = groupID }}
}

func ConsumeTopics(topics []string) Option {
	return consumerOption{func(cfg *cfg) { cfg.topics = topics }}
}

func FetchMaxBytes(b int32) Option {
	return consumerOption{func(cfg *cfg) { cfg.maxBytes = b }}
}
