package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/markusylisiurunen/go-opinionatedevents"
)

const (
	connectionString string = "postgres://postgres:password@localhost:6543/dev?sslmode=disable"
)

func onCustomerCreated(_ context.Context, msg *opinionatedevents.Message) opinionatedevents.Result {
	fmt.Printf("received a message: %s, %s, %s\n",
		msg.Timestamp().Local().Format(time.RFC3339),
		msg.Name(),
		msg.UUID(),
	)

	if strings.HasPrefix(strings.ToLower(msg.UUID()), "a") {
		err := errors.New("UUID begins with an unacceptable character")
		return opinionatedevents.ErrorResult(err, opinionatedevents.ResultWithNoRetries())
	}

	if strings.HasPrefix(strings.ToLower(msg.UUID()), "b") {
		err := errors.New("UUID begins with an unacceptable character")
		return opinionatedevents.ErrorResult(err, opinionatedevents.ResultWithRetryAfter(time.Second))
	}

	return opinionatedevents.SuccessResult()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// make a new receiver
	receiver, err := opinionatedevents.NewReceiver()
	if err != nil {
		panic(err)
	}

	// attach the handlers
	receiver.On("customers.created", opinionatedevents.WithLimit(3)(
		// from the 2nd attempt: 30s, 94s, 566s, 1800s, 1800s...
		opinionatedevents.WithBackoff(opinionatedevents.ExponentialBackoff(30, 10, 2, 30*time.Minute))(
			onCustomerCreated,
		),
	))

	// attach postgres events to the receiver
	_, err = opinionatedevents.MakeReceiveFromPostgres(ctx, receiver, connectionString,
		opinionatedevents.ReceiveFromPostgresWithQueues("svc_1", "svc_2"), // i.e., `svc_3` will be skipped
		opinionatedevents.ReceiveFromPostgresWithTableName("messages"),
		opinionatedevents.ReceiveFromPostgresWithNotifyTrigger("__messages"),
		opinionatedevents.ReceiveFromPostgresWithIntervalTrigger(1*time.Second),
	)
	if err != nil {
		panic(err)
	}

	// wait for stop signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
	cancel()
}
