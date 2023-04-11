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

func publishWithoutTx(ctx context.Context, publisher *opinionatedevents.Publisher) {
	msg, err := opinionatedevents.NewMessage("customers.created", nil)
	if err != nil {
		panic(err)
	}
	if err := publisher.Publish(ctx, msg); err != nil {
		panic(err)
	}
}

func publishWithTx(ctx context.Context, db *sql.DB, publisher *opinionatedevents.Publisher) {
	msg, err := opinionatedevents.NewMessage("customers.created", nil)
	if err != nil {
		panic(err)
	}
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
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	// init the database connection
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}
	// init the postgres destination
	destination, err := opinionatedevents.NewPostgresDestination(db,
		opinionatedevents.PostgresDestinationWithTableName("events"),
		opinionatedevents.PostgresDestinationWithTopicToQueues("customers", "svc_1", "svc_2", "svc_3"),
	)
	if err != nil {
		panic(err)
	}
	// init the publisher
	publisher, err := opinionatedevents.NewPublisher(
		opinionatedevents.PublisherWithSyncBridge(destination),
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
