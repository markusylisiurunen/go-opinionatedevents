package opinionatedevents

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type postgresContextKey string

const (
	postgresContextKeyForTx postgresContextKey = postgresContextKey("tx")
)

// database connection abstractions
// ---

type sqlTx interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Commit() error
	Rollback() error
}

type sqlDB interface {
	Begin() (sqlTx, error)
}

type realTX struct {
	tx *sql.Tx
}

func (rtx *realTX) Exec(query string, args ...any) (sql.Result, error) {
	return rtx.tx.Exec(query, args...)
}

func (rtx *realTX) Query(query string, args ...any) (*sql.Rows, error) {
	return rtx.tx.Query(query, args...)
}

func (rtx *realTX) Commit() error {
	return rtx.tx.Commit()
}

func (rtx *realTX) Rollback() error {
	return rtx.tx.Rollback()
}

type realDB struct {
	db *sql.DB
}

func (rdb *realDB) Begin() (sqlTx, error) {
	tx, err := rdb.db.Begin()
	return &realTX{tx: tx}, err
}

// routing provider
// ---

type postgresRoutingProvider interface {
	queues(tx sqlTx, topic string) ([]string, error)
}

type persistedPostgresRoutingProvider struct {
	schema string
}

func newPersistedPostgresRoutingProvider(schema string) *persistedPostgresRoutingProvider {
	return &persistedPostgresRoutingProvider{schema: schema}
}

func (p *persistedPostgresRoutingProvider) queues(tx sqlTx, topic string) ([]string, error) {
	listSubscribedQueuesQuery := withSchema(
		`SELECT queue FROM :SCHEMA.routing WHERE topic = $1`,
		p.schema,
	)
	rows, err := tx.Query(listSubscribedQueuesQuery, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	queues := []string{}
	for rows.Next() {
		var queue string
		if err := rows.Scan(&queue); err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

// transaction provider which abstracts the underlying database connection away
// ---

type postgresTransactionProvider struct {
	db sqlDB
}

func newPostgresTransactionProvider(db sqlDB) *postgresTransactionProvider {
	return &postgresTransactionProvider{db: db}
}

func (p *postgresTransactionProvider) do(ctx context.Context, action func(tx sqlTx) error) error {
	var tx sqlTx
	var committable bool
	if val, ok := ctx.Value(postgresContextKeyForTx).(sqlTx); ok {
		// the context had a custom user-provided transaction, use it and never commit it on behalf of the user
		tx = val
		committable = false
	} else {
		// otherwise, run the action within a custom transaction
		_tx, err := p.db.Begin()
		defer _tx.Rollback() //nolint the error is not relevant
		if err != nil {
			return err
		}
		tx = _tx
		committable = true
	}
	// run the actual action within the transaction, and possibly manage the transaction
	if err := action(tx); err != nil {
		if committable {
			if txErr := tx.Rollback(); txErr != nil {
				return txErr
			}
		}
		return err
	}
	if committable {
		if txErr := tx.Commit(); txErr != nil {
			return txErr
		}
	}
	return nil
}

// destination for postgres
// ---

type postgresDestination struct {
	db             sqlDB
	routing        postgresRoutingProvider
	schema         string
	skipMigrations bool
	tx             *postgresTransactionProvider
}

type postgresDestinationOption func(dest *postgresDestination) error

func PostgresDestinationWithSchema(schema string) postgresDestinationOption {
	return func(d *postgresDestination) error {
		d.routing = newPersistedPostgresRoutingProvider(schema)
		d.schema = schema
		return nil
	}
}

func NewPostgresDestination(db *sql.DB, options ...postgresDestinationOption) (*postgresDestination, error) {
	// init the dependencies
	_db := &realDB{db: db}
	// init the destination w/ options
	defaultSchema := "opinionatedevents"
	destination := &postgresDestination{
		db:             _db,
		routing:        newPersistedPostgresRoutingProvider(defaultSchema),
		schema:         defaultSchema,
		skipMigrations: false,
		tx:             newPostgresTransactionProvider(_db),
	}
	for _, apply := range options {
		if err := apply(destination); err != nil {
			return nil, err
		}
	}
	// make sure the migrations are run
	if !destination.skipMigrations {
		if err := migrate(db, destination.schema); err != nil {
			return nil, err
		}
	}
	return destination, nil
}

func (d *postgresDestination) setDB(db sqlDB) {
	d.db = db
	d.tx = newPostgresTransactionProvider(db)
}

func (d *postgresDestination) setRouting(routing postgresRoutingProvider) {
	d.routing = routing
}

func (d *postgresDestination) Deliver(ctx context.Context, batch []*Message) error {
	// FIXME: should insert a batch of messages in a single INSERT statement
	return d.tx.do(ctx, func(tx sqlTx) error {
		toBeInserted := []*postgresDestinationInsertMessage{}
		for _, msg := range batch {
			payload, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			queues, err := d.routing.queues(tx, msg.GetTopic())
			if err != nil {
				return err
			}
			for _, queue := range queues {
				toBeInserted = append(toBeInserted, &postgresDestinationInsertMessage{
					name:        msg.GetName(),
					payload:     payload,
					publishedAt: msg.GetPublishedAt(),
					deliverAt:   msg.GetDeliverAt(),
					queue:       queue,
					topic:       msg.GetTopic(),
					uuid:        msg.GetUUID(),
				})

			}
		}
		return d.insertMessages(tx, toBeInserted...)
	})
}

type postgresDestinationInsertMessage struct {
	deliverAt   time.Time
	name        string
	payload     []byte
	publishedAt time.Time
	queue       string
	topic       string
	uuid        string
}

func (d *postgresDestination) insertMessages(tx sqlTx, messages ...*postgresDestinationInsertMessage) error {
	for _, batch := range groupIntoBatches(messages, 128) {
		var paramIdx int = 0
		params := []any{}
		asParam := func(v any) string {
			params = append(params, v)
			paramIdx++
			return fmt.Sprintf("$%d", paramIdx)
		}
		var values = []string{}
		for _, i := range batch {
			values = append(values,
				fmt.Sprintf("('pending', %s, %s, %s, %s, %s, %s, %s)",
					asParam(i.topic),
					asParam(i.queue),
					asParam(i.publishedAt.UTC()),
					asParam(i.deliverAt.UTC()),
					asParam(i.uuid),
					asParam(i.name),
					asParam(i.payload),
				),
			)
		}
		// define the needed SQL queries
		var insertQueryTemplate = `
		INSERT INTO :SCHEMA.events (status, topic, queue, published_at, deliver_at, uuid, name, payload)
		VALUES %s
		ON CONFLICT (queue, uuid) DO NOTHING
		`
		insertQuery := withSchema(
			fmt.Sprintf(insertQueryTemplate, strings.Join(values, ", ")),
			d.schema,
		)
		// insert the event to the table
		if _, err := tx.Exec(insertQuery, params...); err != nil {
			return err
		}
	}
	return nil
}
