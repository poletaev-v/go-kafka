# go-kafka
___

## Назначение:

Обертка для работы с kafka в Go

### Установка

---
```shell
go get github.com/poletaev-v/go-kafka
```

### Создание клиента

---
```go
package main

import (
	"context"
	"log"

	"github.com/poletaev-v/go-kafka"
)

func main() {
	ctx := context.Background()
	kafkaClient, err := kafka.NewClient(
		ctx,
		kafka.BrokerList([]string{"main-kafka.local:9092"}),
		kafka.ConsumerGroup("group-local"),
		kafka.ConsumeTopics([]string{"testTopic"}),
		kafka.Auth("username", "password"),
		kafka.SaslMechanism(kafka.SaslMechanismOption("SCRAM-SHA-512")),
		kafka.FetchMaxBytes(1048576), // 1 MiB
	)
	if err != nil {
		log.Fatal("kafka client error: ", err)
	}
	defer kafkaClient.Close()
}

```

### Потребление сообщений

---
```go
package main

import (
	"context"
	"log"

	"github.com/poletaev-v/go-kafka"
)

func main() {
	ctx := context.Background()
	kafkaClient, err := kafka.NewClient(
		ctx,
		kafka.BrokerList([]string{"main-kafka.local:9092"}),
		kafka.ConsumerGroup("group-local"),
		kafka.ConsumeTopics([]string{"testTopic"}),
		kafka.Auth("username", "password"),
		kafka.SaslMechanism(kafka.SaslMechanismOption("SCRAM-SHA-512")),
		kafka.FetchMaxBytes(1048576), // 1 MiB
	)
	if err != nil {
		log.Fatal("kafka client error: ", err)
	}
	defer kafkaClient.Close()
	
	messages := make(chan []byte)
	if err := kafkaClient.StartConsume(ctx, messages); err != nil {
		log.Fatal("kafka consuming error: ", err)
	}
}
```

### Отправка сообщения в топик

---
```go
package main

import (
	"context"
	"encoding/json"
	"log"
    
	"github.com/poletaev-v/go-kafka"
)

func main()  {
	ctx := context.Background()
	kafkaClient, err := kafka.NewClient(
		ctx,
		kafka.BrokerList([]string{"main-kafka.local:9092"}),
		kafka.ConsumerGroup("group-local"),
		kafka.ConsumeTopics([]string{"testTopic"}),
		kafka.Auth("username", "password"),
		kafka.SaslMechanism(kafka.SaslMechanismOption("SCRAM-SHA-512")),
		kafka.FetchMaxBytes(1048576), // 1 MiB
	)
	if err != nil {
		log.Fatal("kafka client error: ", err)
	}
	defer kafkaClient.Close()
	
	message := map[string]interface{}{
		"test-key": "test-value",
	}
	rawMessage, err := json.Marshal(message)
	if err != nil {
		log.Fatal("marshal message failed: ", err)
	}

	// publish message into topic
	if err = kafkaClient.PublishMessage(ctx, "testTopicProduce", rawMessage); err != nil {
		log.Fatal("record had a produce error: ", err)
	}
}

```

### Обработка сообщения

---
**Заметка:** для обработки сообщений необходимо реализовать интерфейс ***"kafka.MessageHandler"***

```go
package main

import (
	"context"
	"log"
)

type Service struct {}

func (s *Service) HandleMessage(ctx context.Context, message <-chan []byte) {
loop:
    for {
        select {
        case <-ctx.Done():
            log.Println("Message consume has been stopped!")
            break loop
        case rawMessage := <-message:
            log.Println("Message consumed: ", string(rawMessage))
        }
    }
}
```