package opinionatedevents

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
)

type postgresReceiverTrigger interface {
	Start(ctx context.Context) chan struct{}
}

// Interval trigger
// ---

type postgresReceiverIntervalTrigger struct {
	interval time.Duration
}

func newPostgresReceiverIntervalTrigger(interval time.Duration) *postgresReceiverIntervalTrigger {
	return &postgresReceiverIntervalTrigger{interval: interval}
}

func (t *postgresReceiverIntervalTrigger) Start(ctx context.Context) chan struct{} {
	c := make(chan struct{})

	go func(ctx context.Context) {
		for {
			time.Sleep(t.interval)

			select {
			case <-ctx.Done():
				return
			default:
				c <- struct{}{}
			}
		}
	}(ctx)

	return c
}

// Postgres notify trigger
// ---

type postgresReceiverNotifyTrigger struct {
	connectionString string
	channelName      string
}

func newPostgresReceiverNotifyTrigger(connectionString string, channelName string) *postgresReceiverNotifyTrigger {
	return &postgresReceiverNotifyTrigger{
		connectionString: connectionString,
		channelName:      channelName,
	}
}

func (t *postgresReceiverNotifyTrigger) Start(ctx context.Context) chan struct{} {
	c := make(chan struct{})

	// TODO: how to handle errors in this?
	go func(ctx context.Context) {
		listener := pq.NewListener(
			t.connectionString,
			5*time.Second,
			30*time.Second,
			func(_ pq.ListenerEventType, err error) {
				if err != nil {
					// TODO: how to propagate errors? instrument with OpenTelemetry?
					return
				}
			},
		)

		err := listener.Listen(t.channelName)
		if err != nil {
			// FIXME: should not just return...
			return
		}

		for {
			select {
			case <-ctx.Done():
				listener.Close()
				return
			case <-time.After(30 * time.Second):
				if err := listener.Ping(); err != nil {
					// FIXME: should not just return...
					return
				}
			case <-listener.Notify:
				c <- struct{}{}
			}
		}
	}(ctx)

	return c
}

// Aggregate trigger
// ---

type postgresReceiverAggregateTrigger struct {
	triggers []postgresReceiverTrigger
}

func newPostgresReceiverAggregateTrigger(triggers ...postgresReceiverTrigger) *postgresReceiverAggregateTrigger {
	return &postgresReceiverAggregateTrigger{
		triggers: triggers,
	}
}

func (t *postgresReceiverAggregateTrigger) Start(ctx context.Context) chan struct{} {
	c := make(chan struct{})

	for _, trigger := range t.triggers {
		go func(ctx context.Context, trigger postgresReceiverTrigger) {
			ch := trigger.Start(ctx)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
					c <- struct{}{}
				}
			}
		}(ctx, trigger)
	}

	return c
}

// Postgres receiver
// ---

type ReceiveFromPostgres struct {
	db       *sql.DB
	dbURL    string
	schema   *postgresSchema
	queues   []string
	receiver *Receiver
	triggers []postgresReceiverTrigger
}

type receiveFromPostgresOption func(r *ReceiveFromPostgres) error

func MakeReceiveFromPostgres(
	ctx context.Context,
	receiver *Receiver,
	connectionString string,
	options ...receiveFromPostgresOption,
) (*ReceiveFromPostgres, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	postgresReceiver := &ReceiveFromPostgres{
		db:       db,
		dbURL:    connectionString,
		schema:   newPostgresSchema(),
		queues:   []string{"default"},
		receiver: receiver,
		triggers: []postgresReceiverTrigger{},
	}

	for _, apply := range options {
		if err := apply(postgresReceiver); err != nil {
			db.Close()
			return nil, err
		}
	}

	if len(postgresReceiver.triggers) == 0 {
		intervalTrigger := newPostgresReceiverIntervalTrigger(2 * time.Second)
		postgresReceiver.triggers = append(postgresReceiver.triggers, intervalTrigger)
	}

	postgresReceiver.startTriggers(ctx)

	return postgresReceiver, nil
}

func (r *ReceiveFromPostgres) startTriggers(ctx context.Context) {
	trigger := newPostgresReceiverAggregateTrigger(r.triggers...)

	c := trigger.Start(ctx)

	// start receiving the triggers
	go func(ctx context.Context) {
		var mutex sync.Mutex
		processing := false

		for {
			select {
			case <-ctx.Done():
				return
			case <-c:
				mutex.Lock()
				if processing {
					mutex.Unlock()
					continue
				}

				processing = true
				mutex.Unlock()

				go func() {
					defer func() {
						mutex.Lock()
						processing = false
						mutex.Unlock()
					}()

					if err := r.processMessages(); err != nil {
						// TODO: how to propagate errors?
						goto outside
					}
				outside:

					// TODO: what if new triggers have triggered during the processing? does it matter?
				}()
			}
		}
	}(ctx)
}

func (r *ReceiveFromPostgres) processMessages() error {
	// will limit the next message count to 500
	foundMaxLimit, foundCount := 500, 0
	visited := []int64{}

	for {
		tx, err := r.db.Begin()
		if err != nil {
			return err
		}

		id, err := r.processNextMessage(tx, visited)
		if err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				return txErr
			}
			return err
		}

		found := id > -1

		if txErr := tx.Commit(); txErr != nil {
			return txErr
		}

		if found {
			visited = append(visited, id)
			foundCount += 1
		}

		if !found || foundCount >= foundMaxLimit {
			break
		}
	}

	return nil
}

func (r *ReceiveFromPostgres) processNextMessage(tx *sql.Tx, visited []int64) (int64, error) {
	selectQuery := fmt.Sprintf(
		`
		SELECT %s, %s, %s, %s, %s
		FROM %s
		WHERE
			%s = 'pending' AND
			%s = ANY($1) AND
			NOT (%s = ANY($2)) AND
			%s <= $3
		ORDER BY %s ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
		`,
		r.schema.columns[postgresColumnId],
		r.schema.columns[postgresColumnUuid],
		r.schema.columns[postgresColumnQueue],
		r.schema.columns[postgresColumnPayload],
		r.schema.columns[postgresColumnDeliveryAttempts],
		r.schema.table,
		r.schema.columns[postgresColumnStatus],
		r.schema.columns[postgresColumnQueue],
		r.schema.columns[postgresColumnId],
		r.schema.columns[postgresColumnDeliverAt],
		r.schema.columns[postgresColumnTimestamp],
	)

	statusQuery := fmt.Sprintf(
		`UPDATE %s SET %s = $1 WHERE %s = $2 AND %s = $3`,
		r.schema.table,
		r.schema.columns[postgresColumnStatus],
		r.schema.columns[postgresColumnQueue],
		r.schema.columns[postgresColumnUuid],
	)

	incrementDeliveryAttemptsQuery := fmt.Sprintf(
		`UPDATE %s SET %s = %s + 1 WHERE %s = $1 AND %s = $2`,
		r.schema.table,
		r.schema.columns[postgresColumnDeliveryAttempts],
		r.schema.columns[postgresColumnDeliveryAttempts],
		r.schema.columns[postgresColumnQueue],
		r.schema.columns[postgresColumnUuid],
	)

	deliverAtQuery := fmt.Sprintf(
		`UPDATE %s SET %s = $1 WHERE %s = $2 AND %s = $3`,
		r.schema.table,
		r.schema.columns[postgresColumnDeliverAt],
		r.schema.columns[postgresColumnQueue],
		r.schema.columns[postgresColumnUuid],
	)

	row := tx.QueryRow(selectQuery, pq.Array(r.queues), pq.Array(visited), time.Now().UTC())

	var id int64
	var uuid string
	var queue string
	var payload string
	var deliveryAttempts int64

	if err := row.Scan(&id, &uuid, &queue, &payload, &deliveryAttempts); err != nil {
		if err == sql.ErrNoRows {
			return -1, nil
		}

		return -1, err
	}

	receiveResult := r.receiver.Receive(context.Background(),
		Delivery{[]byte(payload), int(deliveryAttempts) + 1},
	)

	// record the delivery attempt
	if result, err := tx.Exec(incrementDeliveryAttemptsQuery, queue, uuid); err != nil {
		return id, err
	} else {
		if rowCount, err := result.RowsAffected(); err != nil {
			return id, err
		} else if rowCount != 1 {
			return id, errors.New("could not increment delivery attempts")
		}
	}

	if receiveResult.error() != nil {
		retryAt := receiveResult.retryAt()

		// if the retry at time has a zero value, the message should be dropped instead of retried
		if retryAt.IsZero() {
			if result, err := tx.Exec(statusQuery, "dropped", queue, uuid); err != nil {
				return id, err
			} else {
				if rowCount, err := result.RowsAffected(); err != nil {
					return id, err
				} else if rowCount != 1 {
					return id, errors.New("could not update message status to 'dropped'")
				}
			}

			// returning `nil` as the error means that the transaction will be committed
			return id, nil
		}

		// otherwise, the message will be rescheduled for retry some time in the future
		if result, err := tx.Exec(deliverAtQuery, retryAt.UTC(), queue, uuid); err != nil {
			return id, err
		} else {
			if rowCount, err := result.RowsAffected(); err != nil {
				return id, err
			} else if rowCount != 1 {
				return id, errors.New("could not update message delivery time")
			}
		}

		// returning `nil` as the error means that the transaction will be committed
		return id, nil
	}

	// processing was successful, mark it as `processed`
	if result, err := tx.Exec(statusQuery, "processed", queue, uuid); err != nil {
		return id, err
	} else {
		if rowCount, err := result.RowsAffected(); err != nil {
			return id, err
		} else if rowCount != 1 {
			return id, errors.New("could not update message status to 'processed'")
		}
	}

	return id, nil
}

func ReceiveFromPostgresWithTableName(name string) receiveFromPostgresOption {
	return func(r *ReceiveFromPostgres) error {
		r.schema.setTable(name)
		return nil
	}
}

func ReceiveFromPostgresWithColumnNames(names map[string]string) receiveFromPostgresOption {
	return func(r *ReceiveFromPostgres) error {
		for name, value := range names {
			key := postgresSchemaColumn(name)
			if _, ok := r.schema.columns[key]; ok {
				r.schema.setColumn(key, value)
			}
		}
		return nil
	}
}

func ReceiveFromPostgresWithIntervalTrigger(interval time.Duration) receiveFromPostgresOption {
	return func(r *ReceiveFromPostgres) error {
		r.triggers = append(r.triggers, newPostgresReceiverIntervalTrigger(interval))
		return nil
	}
}

func ReceiveFromPostgresWithNotifyTrigger(channelName string) receiveFromPostgresOption {
	return func(r *ReceiveFromPostgres) error {
		r.triggers = append(r.triggers, newPostgresReceiverNotifyTrigger(r.dbURL, channelName))
		return nil
	}
}

func ReceiveFromPostgresWithQueues(queues ...string) receiveFromPostgresOption {
	return func(r *ReceiveFromPostgres) error {
		r.queues = queues
		return nil
	}
}
