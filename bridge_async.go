package opinionatedevents

import (
	"log"
	"sync"
	"time"
)

type asyncBridge struct {
	destinations []destination
	buffer       []*Message
	mutex        *sync.Mutex
	signal       chan struct{}
}

func (b *asyncBridge) take(m *Message) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.buffer = append(b.buffer, m)

	if len(b.buffer) == 1 {
		b.signal <- struct{}{}
	}

	return nil
}

func (b *asyncBridge) drain() {
	// TODO: what the hell to do here?
	time.Sleep(1 * time.Second)
}

func (b *asyncBridge) start() {
	go func() {
		for {
			if len(b.buffer) == 0 {
				<-b.signal
			}

			b.mutex.Lock()

			next := b.buffer[0]
			b.buffer = b.buffer[1:]

			b.mutex.Unlock()

			for _, d := range b.destinations {
				if err := d.deliver(next); err != nil {
					// TODO: somehow queue the message again and track attempts
					log.Fatal(err)
				}
			}
		}
	}()
}

func newAsyncBridge(destinations ...destination) *asyncBridge {
	bridge := &asyncBridge{
		destinations: destinations,
		buffer:       []*Message{},
		mutex:        &sync.Mutex{},
		signal:       make(chan struct{}, 1),
	}

	bridge.start()

	return bridge
}
