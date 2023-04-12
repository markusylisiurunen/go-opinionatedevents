package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	events "github.com/markusylisiurunen/go-opinionatedevents"

	_ "github.com/lib/pq"
)

const (
	connectionString string = "postgres://postgres:password@localhost:6543/dev?sslmode=disable"
)

func onCustomerCreated(_ context.Context, delivery events.Delivery) error {
	msg := delivery.GetMessage()
	fmt.Printf("received a message: %s, %s, %s, %s\n",
		msg.GetPublishedAt().Local().Format(time.RFC3339),
		delivery.GetQueue(),
		msg.GetName(),
		msg.GetUUID(),
	)
	if strings.HasPrefix(strings.ToLower(msg.GetUUID()), "a") {
		return events.Fatal(errors.New("UUID begins with an unacceptable character"))
	}
	if strings.HasPrefix(strings.ToLower(msg.GetUUID()), "b") {
		return errors.New("UUID begins with an unacceptable character")
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	// init the database connection
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}
	// init the receiver
	receiver, err := events.NewReceiver()
	if err != nil {
		panic(err)
	}
	// attach the handlers
	for _, queue := range []string{"svc_1", "svc_2"} {
		receiver.On(queue, "customers.created", events.WithLimit(3)(
			// from the 2nd attempt: 30s, 94s, 566s, 1800s, 1800s...
			events.WithBackoff(events.ExponentialBackoff(30, 10, 2, 30*time.Minute))(
				onCustomerCreated,
			),
		))
	}
	// init the postgres source
	postgresSource, err := events.NewPostgresSource(db,
		events.PostgresSourceWithTableName("events"),
		events.PostgresSourceWithQueues("svc_1", "svc_2"), // `svc_3` will be skipped
		events.PostgresSourceWithIntervalTrigger(1*time.Second),
		events.PostgresSourceWithNotifyTrigger(connectionString, "__events"),
	)
	if err != nil {
		panic(err)
	}
	// start the source
	if err := postgresSource.Start(ctx, receiver); err != nil {
		panic(err)
	}
	// wait for stop signal & stop everything
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
}
