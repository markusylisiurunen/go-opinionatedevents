package main

import (
	"context"
	"database/sql"

	events "github.com/markusylisiurunen/go-opinionatedevents"

	_ "github.com/lib/pq"
)

const (
	connectionString string = "postgres://postgres:password@localhost:6543/dev?sslmode=disable"
)

func publishWithoutTx(ctx context.Context, publisher *events.Publisher) {
	msg, err := events.NewMessage("customers.created", nil)
	if err != nil {
		panic(err)
	}
	if err := publisher.Publish(ctx, msg); err != nil {
		panic(err)
	}
}

func publishWithTx(ctx context.Context, db *sql.DB, publisher *events.Publisher) {
	msg, err := events.NewMessage("customers.created", nil)
	if err != nil {
		panic(err)
	}
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	if err := publisher.Publish(events.WithTx(ctx, tx), msg); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	// init the database connection
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}
	// init the postgres destination
	destination, err := events.NewPostgresDestination(db,
		events.PostgresDestinationWithTableName("events"),
		events.PostgresDestinationWithTopicToQueues("customers", "svc_1", "svc_2", "svc_3"),
	)
	if err != nil {
		panic(err)
	}
	// init the publisher
	publisher, err := events.NewPublisher(
		events.PublisherWithSyncBridge(destination),
	)
	if err != nil {
		panic(err)
	}
	// publish the messages
	publishWithoutTx(ctx, publisher)
	publishWithTx(ctx, db, publisher)
	// cancel the context
	cancel()
}
