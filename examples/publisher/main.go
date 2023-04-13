package main

import (
	"context"
	"database/sql"
	"flag"
	"math/rand"

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
	// read in the count
	var count int
	flag.IntVar(&count, "count", 1, "the number of messages to publish")
	flag.Parse()
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
	// publish the message
	for i := 0; i < count; i += 1 {
		if rand.Float64() < 0.5 {
			publishWithoutTx(ctx, publisher)
		} else {
			publishWithTx(ctx, db, publisher)
		}
	}
	// cancel the context
	cancel()
}
