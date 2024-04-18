package kafka

import "context"

type MessageHandler interface {
	HandleMessage(ctx context.Context, message <-chan []byte)
}
