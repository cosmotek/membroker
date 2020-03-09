package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPublish(t *testing.T) {
	topic := "test123"
	msg := "hello world"

	broker := MemBroker{
		BufferStoreSize: DefaultBufferStoreSize(),
	}

	// cancel will stop the for loop in Run gracefully
	ctx, cancel := context.WithCancel(context.Background())

	go broker.Run(ctx)
	defer cancel()

	// sleep to ensure run has completed init
	time.Sleep(time.Millisecond * 20)

	subscription := broker.Subscribe(topic)
	subscription2 := broker.Subscribe(topic)

	// wait prevents exit until sub2 completes
	wait := make(chan struct{})

	for i := 0; i < 100; i++ {
		broker.Publish(topic, fmt.Sprintf("%s v%d", msg, i))
	}

	go func() {
		for i := 0; i < 100; i++ {
			message := <-subscription2.C()
			if message != fmt.Sprintf("%s v%d", msg, i) {
				t.Errorf("expected message %s but got %s", "hello world", message)
				t.Fail()
			}
		}

		wait <- struct{}{}
	}()

	for i := 0; i < 100; i++ {
		message := <-subscription.C()
		if message != fmt.Sprintf("%s v%d", msg, i) {
			t.Errorf("expected message %s but got %s", "hello world", message)
			t.Fail()
		}
	}

	// block until sub2 completes
	<-wait
}
