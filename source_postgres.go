package opinionatedevents

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
)

// postgres source triggers
// ---

type postgresSourceTrigger interface {
	Start(ctx context.Context) (chan struct{}, error)
}

// interval trigger
// ---

type postgresSourceIntervalTrigger struct {
	interval time.Duration
}

func newPostgresSourceIntervalTrigger(interval time.Duration) *postgresSourceIntervalTrigger {
	return &postgresSourceIntervalTrigger{interval: interval}
}

func (t *postgresSourceIntervalTrigger) Start(ctx context.Context) (chan struct{}, error) {
	c := make(chan struct{})
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				close(c)
				return
			case <-time.After(t.interval):
				c <- struct{}{}
			}
		}
	}(ctx)
	return c, nil
}

// notify trigger
// ---

type postgresSourceNotifyTrigger struct {
	connectionString string
	channelName      string
}

func newPostgresSourceNotifyTrigger(connectionString string, channelName string) *postgresSourceNotifyTrigger {
	return &postgresSourceNotifyTrigger{
		connectionString: connectionString,
		channelName:      channelName,
	}
}

func (t *postgresSourceNotifyTrigger) Start(ctx context.Context) (chan struct{}, error) {
	// init the postgres connection
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
	if err := listener.Listen(t.channelName); err != nil {
		return nil, err
	}
	// start listening for the notify signals
	c := make(chan struct{})
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				listener.Close()
				close(c)
				return
			case <-time.After(30 * time.Second):
				if err := listener.Ping(); err != nil {
					// NOTE: the connection will be retried by `*pq.Listener`
					continue
				}
			case <-listener.Notify:
				// TODO: this should only trigger if the message came to a known queue + name
				c <- struct{}{}
			}
		}
	}(ctx)
	return c, nil
}

// aggregate trigger
// ---

type postgresSourceAggregateTrigger struct {
	triggers []postgresSourceTrigger
}

func newPostgresSourceAggregateTrigger(triggers ...postgresSourceTrigger) *postgresSourceAggregateTrigger {
	return &postgresSourceAggregateTrigger{triggers: triggers}
}

func (t *postgresSourceAggregateTrigger) Start(ctx context.Context) (chan struct{}, error) {
	out := make(chan struct{})
	var running atomic.Int32
	// aggregate the triggers
	for _, trigger := range t.triggers {
		in, err := trigger.Start(ctx)
		if err != nil {
			// TODO: what happens to the previously started listeners...? they should be closed?
			return nil, err
		}
		running.Add(1)
		go func(in chan struct{}, out chan struct{}) {
			defer func() {
				running.Add(-1)
				if running.Load() == 0 {
					close(out)
				}
			}()
			for val := range in {
				out <- val
			}
		}(in, out)
	}
	return out, nil
}

// postgres delivery
// ---

type postgresDelivery struct {
	queue   string
	attempt int
	message *Message
}

func newPostgresDelivery(queue string, attempt int, data []byte) (*postgresDelivery, error) {
	message := &Message{}
	if err := json.Unmarshal(data, message); err != nil {
		return nil, err
	}
	return &postgresDelivery{queue: queue, attempt: attempt, message: message}, nil
}

func (d *postgresDelivery) GetAttempt() int {
	return d.attempt
}

func (d *postgresDelivery) GetQueue() string {
	return d.queue
}

func (d *postgresDelivery) GetMessage() *Message {
	return d.message
}

// postgres source
// ---

type postgresSource struct {
	db             *sql.DB
	maxWorkers     int
	receiver       *Receiver
	schema         string
	skipMigrations bool
	triggers       []postgresSourceTrigger
}

type postgresSourceOption func(source *postgresSource) error

func PostgresSourceWithSchema(schema string) postgresSourceOption {
	return func(source *postgresSource) error {
		source.schema = schema
		return nil
	}
}

func PostgresSourceWithMaxWorkers(maxWorkers uint) postgresSourceOption {
	return func(source *postgresSource) error {
		source.maxWorkers = int(maxWorkers)
		return nil
	}
}

func PostgresSourceWithIntervalTrigger(interval time.Duration) postgresSourceOption {
	return func(source *postgresSource) error {
		source.triggers = append(source.triggers, newPostgresSourceIntervalTrigger(interval))
		return nil
	}
}

func PostgresSourceWithNotifyTrigger(connectionString string) postgresSourceOption {
	return func(source *postgresSource) error {
		source.triggers = append(source.triggers, newPostgresSourceNotifyTrigger(connectionString, "__events"))
		return nil
	}
}

func NewPostgresSource(db *sql.DB, options ...postgresSourceOption) (*postgresSource, error) {
	source := &postgresSource{
		db:             db,
		maxWorkers:     8,
		schema:         "opinionatedevents",
		skipMigrations: false,
		triggers:       []postgresSourceTrigger{},
	}
	for _, apply := range options {
		if err := apply(source); err != nil {
			return nil, err
		}
	}
	// configure the default trigger(s)
	if len(source.triggers) == 0 {
		source.triggers = append(source.triggers, newPostgresSourceIntervalTrigger(5*time.Second))
	}
	// make sure the migrations are run
	if !source.skipMigrations {
		if err := migrate(db, source.schema); err != nil {
			return nil, err
		}
	}
	return source, nil
}

type PostgresSourceQueueDeclareParams struct {
	Topic string
	Queue string
}

func (s *postgresSource) QueueDeclare(params *PostgresSourceQueueDeclareParams) error {
	upsertSubscriptionQuery := withSchema(
		`
		INSERT INTO :SCHEMA.routing (topic, queue)
		VALUES ($1, $2)
		ON CONFLICT (topic, queue) DO UPDATE SET
			last_declared_at = now()
		`,
		s.schema,
	)
	if _, err := s.db.Exec(upsertSubscriptionQuery,
		params.Topic,
		params.Queue,
	); err != nil {
		return err
	}
	return nil
}

func (s *postgresSource) Start(ctx context.Context, receiver *Receiver) error {
	if s.receiver != nil {
		return fmt.Errorf("cannot start a source more than once")
	}
	s.receiver = receiver
	// create and start an aggregate trigger
	trigger := newPostgresSourceAggregateTrigger(s.triggers...)
	triggerChan, err := trigger.Start(ctx)
	if err != nil {
		return err
	}
	// fan-out the triggers to worker triggers
	workerTriggerChans := make([]chan struct{}, s.maxWorkers)
	for i := range workerTriggerChans {
		workerTriggerChans[i] = make(chan struct{})
	}
	go func() {
		for {
			val, ok := <-triggerChan
			for _, out := range workerTriggerChans {
				if !ok {
					close(out)
				} else {
					out <- val
				}
			}
			if !ok {
				break
			}
		}
	}()
	// launch the workers
	for i := 0; i < s.maxWorkers; i += 1 {
		go func(ctx context.Context, trigger chan struct{}) {
			var mutex sync.Mutex
			processing := false
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-trigger:
					if !ok {
						return
					}
					mutex.Lock()
					if processing {
						// we were already processing pending messages, just skip the trigger
						mutex.Unlock()
						continue
					}
					// start processing pending messages from this trigger
					processing = true
					mutex.Unlock()
					// process in a goroutine so that the triggers are not blocked
					go func() {
						defer func() {
							mutex.Lock()
							processing = false
							mutex.Unlock()
						}()
						if err := s.processUntilNoneLeft(); err != nil {
							// TODO: the errored messages will be retried on next trigger, but should log somehow
							goto outside
						}
					outside:
					}()
				}
			}
		}(ctx, workerTriggerChans[i])
	}
	return nil
}

func (s *postgresSource) processUntilNoneLeft() error {
	// limit the number of processed messages to `n`
	foundMaxLimit, foundCount := 500, 0
	// process the pending messages one by one, in a transaction
	visitedMessageIds := []int64{}
	nonEmptyQueues := append([]string{}, s.receiver.GetQueuesWithHandlers()...)
	for {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		// pick a random non-empty queue to pull messages from
		selectedQueue := nonEmptyQueues[rand.Intn(len(nonEmptyQueues))]
		messagesWithHandlers := s.receiver.GetMessagesWithHandlers(selectedQueue)
		// attempt to process the next available message from the queue
		id, err := s.processNextMessage(tx, []string{selectedQueue}, messagesWithHandlers, visitedMessageIds)
		if err != nil {
			// a non-nil error means that something very unexpected (e.g. network down) happened -> rollback
			if err := tx.Rollback(); err != nil {
				return err
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		// an id of -1 means that there was no pending messages left
		found := id > -1
		if found {
			visitedMessageIds = append(visitedMessageIds, id)
			foundCount += 1
		} else {
			nonEmptyQueues = filter(nonEmptyQueues, func(item string, _ int) bool {
				return item != selectedQueue
			})
		}
		// check if there are possibly more pending messages
		isBelowLimit := foundCount < foundMaxLimit
		hasNonEmptyQueues := len(nonEmptyQueues) > 0
		if !(isBelowLimit && hasNonEmptyQueues) {
			break
		}
	}
	return nil
}

func (s *postgresSource) processNextMessage(
	tx *sql.Tx,
	queuesToPullFrom []string,
	messagesWithHandlers []string,
	visitedMessageIds []int64,
) (int64, error) {
	// define the required SQL queries
	selectNextEventQuery := withSchema(
		`
		SELECT id, uuid, queue, payload, delivery_attempts
		FROM :SCHEMA.events
		WHERE
			status = 'pending' AND
			queue = ANY($1) AND
			name = ANY($2) AND
			NOT (id = ANY($3)) AND
			deliver_at <= $4
		ORDER BY published_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
		`,
		s.schema,
	)
	updateStatusQuery := withSchema(
		`
		UPDATE :SCHEMA.events
		SET status = $1
		WHERE queue = $2 AND uuid = $3
		`,
		s.schema,
	)
	incrementDeliveryAttemptsQuery := withSchema(
		`
		UPDATE :SCHEMA.events
		SET delivery_attempts = delivery_attempts + 1
		WHERE queue = $1 AND uuid = $2
		`,
		s.schema,
	)
	updateDeliverAtQuery := withSchema(
		`
		UPDATE :SCHEMA.events
		SET deliver_at = $1
		WHERE queue = $2 AND uuid = $3
		`,
		s.schema,
	)
	// attempt to fetch the next pending message from the database
	row := tx.QueryRow(selectNextEventQuery,
		pq.Array(queuesToPullFrom),
		pq.Array(messagesWithHandlers),
		pq.Array(visitedMessageIds),
		time.Now().UTC(),
	)
	var id int64
	var uuid string
	var queue string
	var payload string
	var deliveryAttempts int64
	if err := row.Scan(&id, &uuid, &queue, &payload, &deliveryAttempts); err != nil {
		if err == sql.ErrNoRows {
			// there was no pending messages left -> return the id of -1 to indicate the outcome
			return -1, nil
		}
		return -1, err
	}
	// a pending message was found, construct a delivery and attempt to deliver it to the receiver
	delivery, err := newPostgresDelivery(queue, int(deliveryAttempts)+1, []byte(payload))
	if err != nil {
		return id, err
	}
	ctx := context.Background() // TODO: should something be injected into the context?
	result := s.receiver.Deliver(ctx, delivery)
	// record the delivery attempt regardless of the outcome
	if result, err := tx.Exec(incrementDeliveryAttemptsQuery, queue, uuid); err != nil {
		return id, err
	} else {
		if rowCount, err := result.RowsAffected(); err != nil {
			return id, err
		} else if rowCount != 1 {
			return id, errors.New("could not increment delivery attempts")
		}
	}
	// check if the result was successful
	if result == nil {
		// mark as processed
		if result, err := tx.Exec(updateStatusQuery, "processed", queue, uuid); err != nil {
			return id, err
		} else {
			if rowCount, err := result.RowsAffected(); err != nil {
				return id, err
			} else if rowCount != 1 {
				return id, errors.New(`could not update message status to "processed"`)
			}
		}
		return id, nil
	}
	// check if the result is a fatal error
	var fatalErr *fatalError
	if errors.As(result, &fatalErr) {
		// drop the message
		if result, err := tx.Exec(updateStatusQuery, "dropped", queue, uuid); err != nil {
			return id, err
		} else {
			if rowCount, err := result.RowsAffected(); err != nil {
				return id, err
			} else if rowCount != 1 {
				return id, errors.New(`could not update message status to "dropped"`)
			}
		}
		return id, nil
	}
	// otherwise, the error means the message should be retried later on
	retryAt := time.Now().Add(30 * time.Second)
	// try to override the default retry at time from the error
	var retryErr *retryError
	if errors.As(result, &retryErr) {
		retryAt = retryErr.retryAt
	}
	if result, err := tx.Exec(updateDeliverAtQuery, retryAt.UTC(), queue, uuid); err != nil {
		return id, err
	} else {
		if rowCount, err := result.RowsAffected(); err != nil {
			return id, err
		} else if rowCount != 1 {
			return id, errors.New("could not update message delivery time")
		}
	}
	return id, nil
}
