package kafkagoconsumer

import (
	"context"
	"log"
	"runtime/debug"

	"github.com/segmentio/kafka-go"
)

func Consuming(config kafka.ReaderConfig, process func(m kafka.Message)) {
	r := kafka.NewReader(config)
	defer closeReader(r)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			panic(err)
		}
		process(m)
	}
}

func closeReader(reader *kafka.Reader) {
	if r := recover(); r != nil {
		log.Println("Error catched", r)
		log.Println("stack trace", string(debug.Stack()))
	}
	if err := reader.Close(); err != nil {
		panic(err)
	}
}
