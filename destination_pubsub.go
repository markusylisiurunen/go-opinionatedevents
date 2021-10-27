package opinionatedevents

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/pubsub"
)

type PubSubDestination struct {
	client *pubsub.Client
	topics map[string]*pubsub.Topic

	defaultTopic string
	topicMapper  func(msg *Message) string
}

func (d *PubSubDestination) getTopic(msg *Message) string {
	if d.defaultTopic != "" {
		return d.defaultTopic
	}

	return d.topicMapper(msg)
}

func (d *PubSubDestination) deliver(msg *Message) error {
	topic := d.getTopic(msg)

	if _, ok := d.topics[topic]; !ok {
		d.topics[topic] = d.client.Topic(topic)
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ctx := context.Background()
	res := d.topics[topic].Publish(ctx, &pubsub.Message{
		Data: payload,
	})

	if _, err := res.Get(ctx); err != nil {
		return err
	}

	return nil
}

func NewPubSubDestination(projectID string, topic string) (*PubSubDestination, error) {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &PubSubDestination{
		client:       client,
		topics:       map[string]*pubsub.Topic{},
		defaultTopic: topic,
	}, nil
}

func NewPubSubDestinationWithCustomTopics(
	projectID string,
	getTopic func(msg *Message) string,
) (*PubSubDestination, error) {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &PubSubDestination{
		client:      client,
		topics:      map[string]*pubsub.Topic{},
		topicMapper: getTopic,
	}, nil
}
