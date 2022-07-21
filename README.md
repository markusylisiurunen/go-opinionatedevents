> ## ⚠️ Work in Progress
>
> This package is still work in progress and is not recommended to be used.

# Opinionated Events

[![Go Reference](https://pkg.go.dev/badge/github.com/markusylisiurunen/go-opinionatedevents.svg)](https://pkg.go.dev/github.com/markusylisiurunen/go-opinionatedevents)

**Table of Contents**

1. [Install](#install)
2. [The problem](#the-problem)
3. [This solution](#this-solution)
4. [Quickstart](#quickstart)
   1. [Local](#local)
   2. [Cloud Pub/Sub](#cloud-pubsub)
   3. [Postgres](#postgres)
   4. [Custom](#custom)

## Install

```sh
go get github.com/markusylisiurunen/go-opinionatedevents
```

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

This solution is split into three separate concepts, a **publisher**, a **bridge**, and a
**destination**.

**Destination** is the component which receives a single message to be delivered to a destination.
Its only concern is to try to send the received message to the destination and nothing more, making
it a very dumb component. The meaning of "delivered" depends on which destination we are using. For
example, a message is delivered to Cloud Pub/Sub whenever Cloud Pub/Sub has acknowledged of
receiving it.

The **bridge**'s responsibility is to use the given destination to either synchronously or
asynchronously deliver a published message to the destination. Depending on which type of bridge is
used, it may retry failed attempts, buffer messages before sending them, or have any other logic
between publishing the message from the code to actually sending the message to the destination.

The final piece is the **publisher**. This is the client-facing component of which API is used to
publish the actual messages to the event bus.

## Quickstart

### Local

```go
func GetLocalPublisher() *events.Publisher {
    // define the local destination(s) (i.e. the services you have running locally, including the current service)
    destOne := events.NewHTTPDestination("http://localhost:8080/_events/local")
    destTwo := events.NewHTTPDestination("http://localhost:8081/_events/local")

    // initialise the publisher with an async bridge
    publisher, err := events.NewPublisher(
        events.PublisherWithAsyncBridge(10, 200, destOne, destTwo),
    )
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
    destOne := events.NewCloudPubSubDestination("project-id", "core")

    // or define a custom mapper from the message to a Cloud Pub/Sub topic
    destTwo := events.NewPubSubDestinationWithCustomTopics(
        "project-id",
        func(msg *events.Message) string {
            if strings.HasPrefix(msg.Name, "users.") {
                return "users"
            }

            return "core"
        },
    )

    // initialise the publisher with an async bridge
    publisher, err := events.NewPublisher(
        events.PublisherWithAsyncBridge(10, 200, destOne, destTwo),
    )
    if err != nil {
        panic(err)
    }

    return publisher
}
```

### Postgres

For PostgreSQL, there is a little bit of setup that needs to happen for it to work. You need to
create a table to your database for the messages.

```sql
CREATE TABLE events (
    event_id         SERIAL PRIMARY KEY,
    event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    event_deliver_at TIMESTAMP WITH TIME ZONE NOT NULL,
    event_status     TEXT NOT NULL,
    event_topic      TEXT NOT NULL,
    event_queue      TEXT NOT NULL,
    event_uuid       TEXT NOT NULL,
    event_name       TEXT NOT NULL,
    event_data       JSON NOT NULL,

    UNIQUE (event_queue, event_uuid),

    CHECK (event_status IN ('pending', 'processed', 'dropped'))
);

-- index for making ordered queries for a certain set of queues with a `pending` status
CREATE INDEX events_status_queue_timestamp_idx
ON events (event_status, event_queue, event_timestamp);

-- index for making updates to a specific message
CREATE INDEX events_uuid_queue_idx
ON events (event_uuid, event_queue);

CREATE FUNCTION notify_of_new_events() RETURNS TRIGGER AS $$
DECLARE
    notification JSON;
BEGIN
    notification = json_build_object(
        'topic', NEW .event_topic,
        'queue', NEW .event_queue,
        'uuid',  NEW .event_uuid
    );

    PERFORM pg_notify('__events', notification::TEXT);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_of_new_events_trigger AFTER INSERT ON events
FOR EACH ROW EXECUTE PROCEDURE notify_of_new_events();
```

Once this is done, you can setup the publisher like so.

```go
const (
    connectionString string = "<database connection string>"
)

func GetPostgresPublisher() *events.Publisher {
    // create a new database connection
    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        panic(err)
    }

    // make a new postgres destination
    destination, err := events.NewPostgresDestination(connectionString,
        events.PostgresDestinationWithTopicToQueues("test1", "test_queue.1", "test_queue.2"),
        events.PostgresDestinationWithTopicToQueues("test2", "test_queue.1", "test_queue.2"),

        events.PostgresDestinationWithTableName("events"),
        events.PostgresDestinationWithColumnNames(map[string]string{
            "id":        "events_id",
            "name":      "events_name",
            "payload":   "events_payload",
            "queue":     "events_queue",
            "status":    "events_status",
            "timestamp": "events_timestamp",
            "topic":     "events_topic",
            "uuid":      "events_uuid",
        }),
    )

    // initialise the publisher with a sync bridge
    publisher, err := events.NewPublisher(
        // IMPORTANT: you must use the sync bridge with Postgres destination
        events.PublisherWithSyncBridge(destination),
    )
    if err != nil {
        panic(err)
    }

    return publisher
}
```

Now, if you want to publish events within a database transaction, you can do it like so.

```go
func PublishWithTransaction(ctx context.Context, db *sql.DB) {
    publisher := GetPostgresPublisher()
    tx, _ := db.Begin()

    msg, _ := events.NewMessage("test.test", nil)
    publisher.Publish(events.WithTx(ctx, tx), msg)

    tx.Commit()
}
```

### Custom

```go
type StdOutDestination struct{}

func (d *StdOutDestination) Deliver(msg *events.Message) error {
    fmt.Printf("received a message: %s\n", msg.Name)
    return nil
}

func NewStdOutDestination() *StdOutDestination {
    return &StdOutDestination{}
}

func GetCustomPublisher() *events.Publisher {
    dest := NewStdOutDestination()

    publisher, err := events.NewPublisher(
        events.PublisherWithAsyncBridge(10, 200, dest),
    )
    if err != nil {
        panic(err)
    }

    return publisher
}
```
