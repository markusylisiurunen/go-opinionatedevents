package main

import (
	"context"
	"database/sql"

	"github.com/markusylisiurunen/go-opinionatedevents"

	_ "github.com/lib/pq"
)

const (
	connectionString string = "postgres://postgres:password@localhost:6543/dev?sslmode=disable"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}

	// make a new postgres destination
	destination, err := opinionatedevents.NewPostgresDestination(connectionString,
		opinionatedevents.PostgresDestinationWithTableName("messages"),
		opinionatedevents.PostgresDestinationWithTopicToQueues("customers", "svc_1", "svc_2", "svc_3"),
	)
	if err != nil {
		panic(err)
	}

	// make a new publisher
	publisher, err := opinionatedevents.NewPublisher(
		opinionatedevents.PublisherWithSyncBridge(destination),
	)
	if err != nil {
		panic(err)
	}

	// publish a message
	msg, _ := opinionatedevents.NewMessage("customers.created", nil)

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	if err := publisher.Publish(opinionatedevents.WithTx(ctx, tx), msg); err != nil {
		panic(err)
	}

	if err := tx.Commit(); err != nil {
		panic(err)
	}

	cancel()
}
