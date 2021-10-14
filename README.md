# Opinionated Events

**Table of Contents**

1. [The problem](#the-problem)
2. [This solution](#this-solution)
3. [Quickstart](#quickstart)
   1. [Local](#local)
   2. [Cloud Pub/Sub](#cloud-pubsub)
   3. [Custom](#custom)

## The problem

When building real-world applications, we often have situations where we need to perform multiple,
dependent tasks in response to some trigger. For example, when a new user is created, we might want
to do any number of the following actions:

- Create a new user in our database.
- Send the user a verification email.
- Add the user's information to our CRM.
- Add the new user to an emailing list.
- Create a corresponding customer in Stripe.

These were just a few examples of one possible situation. In general, we can describe such a
situation with a graph-like structure of different actions that need to happen in a particular
order. Still, they may not necessarily need to block the "primary" action, nor do they need to be
executed within a single transaction.

Event-driven architectures can help solve this problem by decoupling the primary action from the
secondary or tertiary actions by introducing a layer in the middle. This new layer will take care of
broadcasting events to all interested parties, retrying failed deliveries with a backoff, persisting
events over system outages, and so on. Our responsibility is only to publish the events of interest
to the event bus and implementing the logic for handling these events.

Many existing services can be used as a message broker. In addition to open-source solutions like
[RabbitMQ](https://www.rabbitmq.com), some cloud providers have their own managed services, such as
[AWS SNS](https://aws.amazon.com/sns) and [Cloud Pub/Sub](https://cloud.google.com/pubsub). Without
a proper abstraction layer between our code and the chosen message broker, we might run into issues
when setting up our local development environment or testing our code. It would be good not to
depend on any specific message broker implementation but rather to be able to switch between
different ways of delivering (or not delivering at all) events from one service to another.

This package tries to provide a minimal abstraction between our code and the chosen message broker.
The goal is to allow zero local dependencies and allow testing asynchronous, event-based systems
with ease.

## This solution

> **TODO:** ...

## Quickstart

### Local

```go
func GetLocalPublisher() *events.Publisher {
    // define the local destination(s) (i.e. the services you have running locally, including the current service)
    dest := events.NewHttpDestination()

    dest.AddEndpoint("http://localhost:8080/_events/local")
    dest.AddEndpoint("http://localhost:8081/_events/local")

    // initialise the publisher with an async bridge
    publisher, err := events.NewPublisher(events.WithAsyncBridge(10, 200, dest))
    if err != nil {
        panic(err)
    }

    return publisher
}
```

### Cloud Pub/Sub

```go
func GetCloudPubSubPublisher() *events.Publisher {
    // define the Cloud Pub/Sub destination to one `core` topic
    destOne := events.NewCloudPubSubDestination(events.WithCloudPubSubTopic("core"))

    // or define a custom mapper from the message to a Cloud Pub/Sub topic
    destTwo := events.NewCloudPubSubDestination(
        events.WithCloudPubSubTopicMapper(func(msg *events.Message) string {
            if strings.HasPrefix(msg.Name, "users.") {
                return "users"
            }

            return "core"
        }),
    )

    // initialise the publisher with an async bridge
    publisher, err := events.NewPublisher(events.WithAsyncBridge(10, 200, destOne))
    if err != nil {
        panic(err)
    }

    return publisher
}
```

### Custom

```go
type StdOutDestination struct{}

func (d *StdOutDestination) Deliver(message *events.Message) error {
    fmt.Printf("received a message: %s\n", message.Name)
    return nil
}

func NewStdOutDestination() *StdOutDestination {
    return &StdOutDestination{}
}

func GetCustomPublisher() *events.Publisher {
    dest := NewStdOutDestination()

    publisher, err := events.NewPublisher(events.WithAsyncBridge(10, 200, dest))
    if err != nil {
        panic(err)
    }

    return publisher
}
```
