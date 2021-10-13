# Opinionated Events

## The problem

Quite often when building real-world applications you will have situations where you need to perform
multiple, dependent, tasks when an action occurs. As an example, when a new user is created, you
might want to do any number of the following actions.

- Create a new user in your database
- Send the user a verification email
- Insert a new customer in your CRM
- Add the newly created customer to an emailing list
- Add boilerplate content to the user's account
- Send a notification to your Slack channel
- ... and so on

You might even have multiple layers of these actions where an action `A` happens in your system,
actions `B` and `C` follow action `A`, and action `D` follows action `B`. You end up with a graph of
dependent tasks.

Now, a fairly standard way of solving this kind of flows are event-driven architectures. You have
some _topics_ where you post _events_ and then the attached _queues_ pick the events and send them
to any service that processes these events. At this point, I want to point out that microservices
are **not** a requirement for this. You can very well have a monolithic application but leverage the
benefits of an event-driven architecture.

There are many services that handle the actual broadcasting of the events, such as
[RabbitMQ](https://www.rabbitmq.com), [AWS SQS](https://aws.amazon.com/sqs/), and
[Cloud Pub/Sub](https://cloud.google.com/pubsub). Whatever service you end up picking, you most
likely will want to implement a thin abstraction layer on top of it in your code to allow things
like

- publishing and receiving events locally when developing
- switching the service e.g. from Cloud Pub/Sub to SQS
- enforcing a consistent schema for the events
- ... and so on.

This is what this package is for.

## This solution

This solution allows you to easily setup your service(s) so that you are not locking yourself in to
any specific tool and allows you to develop locally without requiring any extra services.

```go
func GetLocalPublisher() *events.Publisher {
	// define the local destination(s) (i.e. the services you have running locally)
	dest := events.NewHttpDestination()

	dest.AddEndpoint("http://localhost:8080/_events/local")
	dest.AddEndpoint("http://localhost:8081/_events/local")

	// initialise the publisher with the an "async bridge"
	publisher, err := events.NewPublisher(events.WithAsyncBridge(10, 200, dest))
	if err != nil {
		panic(err)
	}

	return publisher
}
```

Now, the only thing you need to change when deploying to e.g. GCP is the following.

```go
func GetCloudPubSubPublisher() *events.Publisher {
    // define the Cloud Pub/Sub destination to one `core` topic...
    dest := events.NewCloudPubSubDestination(events.WithCloudPubSubTopic("core"))

    // ... or you can define your custom event to topic mapper function
    dest = events.NewCloudPubSubDestination(
        events.WithCloudPubSubTopicMapper(func (m *events.Message) string {
            if strings.HasPrefix(m.Name, "users.") {
                return "users"
            }

            return "core"
        })
    )

    // initialise the publisher with the an "async bridge"
    publisher, err := events.NewPublisher(events.WithAsyncBridge(10, 200, dest))
	if err != nil {
		panic(err)
	}

    return publisher
}
```
