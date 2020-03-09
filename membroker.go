package pubsub

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// event is used internally be the broker as the object passed over the ingress channel.
type event struct {
	Topic   string
	Payload interface{}
}

// DefaultBufferStoreSize returns the default recommended buffer store size (100 events).
func DefaultBufferStoreSize() int {
	return 100
}

// MemBroker is an in-memory pubsub broker for communicating many-to-many messages across
// many goroutines.
type MemBroker struct {
	// BufferStoreSize is the size of the ingress channel (how many events it can store at a time),
	// as well as the size of the channel assigned to every subscription.
	BufferStoreSize int
	ingress         chan event

	// topic -> subId -> chan
	channels  map[string]map[string]chan interface{}
	writeLock *sync.Mutex
}

// Subscribe creates and returns subscription to any events
// (returned as interface{}) of the same topic string.
// Note: Currently there is no wildcard or path-based topic routing.
func (m *MemBroker) Subscribe(topic string) Subscription {
	subId := uuid.New().String()
	channel := make(chan interface{}, m.BufferStoreSize)

	_, topicExists := m.channels[topic]
	m.writeLock.Lock()
	if !topicExists {
		m.channels[topic] = make(map[string]chan interface{})
	}

	m.channels[topic][subId] = channel
	m.writeLock.Unlock()

	return Subscription{
		parent:  m,
		id:      subId,
		topic:   topic,
		channel: channel,
	}
}

// Publish sends a event to all subscribers of the provided topic param.
func (m *MemBroker) Publish(topic string, payload interface{}) {
	m.ingress <- event{topic, payload}
}

// deleteSubscription removes the subscription from the broker gracefully,
// preventing the receiving of future messages.
func (m *MemBroker) deleteSubscription(topic, subId string) {
	// lock the mutex on write only to prevent concurrent collision
	m.writeLock.Lock()

	delete(m.channels[topic], subId)
	m.writeLock.Unlock()
}

// Run initializes the broker ingress and subscriber index, and creates
// a blocking-loop which listens for published events, and subsequently
// routes/pushes events to subscribers.
//
// The context parameter of this function may be used with context.WithCancel()
// to gracefully stop the blocking-loop within this function.
func (m *MemBroker) Run(ctx context.Context) {
	m.ingress = make(chan event, m.BufferStoreSize)
	m.channels = make(map[string]map[string]chan interface{})
	m.writeLock = &sync.Mutex{}

	for {
		select {
		case incomingEvent := <-m.ingress:
			subscribers, ok := m.channels[incomingEvent.Topic]
			if ok {
				for _, channel := range subscribers {
					channel <- incomingEvent.Payload
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Subscription is a topic subscription, providing a channel to read events from.
type Subscription struct {
	parent *MemBroker

	id    string
	topic string

	// read only channel
	channel <-chan interface{}
}

// Close unsubscribes from the subscription topic.
func (s *Subscription) Close() {
	// delete the subscriber from the broker
	s.parent.deleteSubscription(s.topic, s.id)
}

// C returns a read-only channel for receiving events.
func (s *Subscription) C() <-chan interface{} {
	return s.channel
}
