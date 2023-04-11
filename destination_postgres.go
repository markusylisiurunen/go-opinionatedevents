package opinionatedevents

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

// routing table to configure the topic -> {queue_1, queue_2, ...} mappings
// ---

type postgresRoutingTable struct {
	topicToQueues map[string][]string
}

func newPostgresRoutingTable() *postgresRoutingTable {
	return &postgresRoutingTable{
		topicToQueues: map[string][]string{},
	}
}

func (t *postgresRoutingTable) appendQueuesForTopic(topic string, queues ...string) {
	if _, ok := t.topicToQueues[topic]; ok {
		t.topicToQueues[topic] = append(t.topicToQueues[topic], queues...)
	} else {
		t.topicToQueues[topic] = queues
	}
}

func (t *postgresRoutingTable) routeToQueues(topic string) []string {
	if queues, ok := t.topicToQueues[topic]; ok {
		return queues
	}
	return []string{"default"}
}

// schema configuration for mapping columns to table column names
// TODO: should be unnecessary once the library handles its own database migrations
// ---

type postgresSchemaColumn string

const (
	postgresColumnDeliverAt        postgresSchemaColumn = postgresSchemaColumn("deliver_at")
	postgresColumnDeliveryAttempts                      = postgresSchemaColumn("delivery_attempts")
	postgresColumnId                                    = postgresSchemaColumn("id")
	postgresColumnName                                  = postgresSchemaColumn("name")
	postgresColumnPayload                               = postgresSchemaColumn("payload")
	postgresColumnPublishedAt                           = postgresSchemaColumn("published_at")
	postgresColumnQueue                                 = postgresSchemaColumn("queue")
	postgresColumnStatus                                = postgresSchemaColumn("status")
	postgresColumnTopic                                 = postgresSchemaColumn("topic")
	postgresColumnUuid                                  = postgresSchemaColumn("uuid")
)

type postgresSchema struct {
	table   string
	columns map[postgresSchemaColumn]string
}

func newPostgresSchema() *postgresSchema {
	return &postgresSchema{
		table: "events",
		columns: map[postgresSchemaColumn]string{
			postgresColumnDeliverAt:        "deliver_at",
			postgresColumnDeliveryAttempts: "delivery_attempts",
			postgresColumnId:               "id",
			postgresColumnName:             "name",
			postgresColumnPayload:          "payload",
			postgresColumnPublishedAt:      "published_at",
			postgresColumnQueue:            "queue",
			postgresColumnStatus:           "status",
			postgresColumnTopic:            "topic",
			postgresColumnUuid:             "uuid",
		},
	}
}

func (s *postgresSchema) setTable(table string) {
	s.table = table
}

func (s *postgresSchema) setColumn(name postgresSchemaColumn, value string) {
	s.columns[name] = value
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
	db         sqlDB
	router     *postgresRoutingTable
	schema     *postgresSchema
	txprovider *postgresTransactionProvider
}

type postgresDestinationOption func(dest *postgresDestination) error

func PostgresDestinationWithTopicToQueues(topic string, queues ...string) postgresDestinationOption {
	return func(d *postgresDestination) error {
		d.router.appendQueuesForTopic(topic, queues...)
		return nil
	}
}

func PostgresDestinationWithTableName(name string) postgresDestinationOption {
	return func(d *postgresDestination) error {
		d.schema.setTable(name)
		return nil
	}
}

func PostgresDestinationWithColumnNames(names map[string]string) postgresDestinationOption {
	return func(d *postgresDestination) error {
		for name, value := range names {
			key := postgresSchemaColumn(name)
			if _, ok := d.schema.columns[key]; ok {
				d.schema.setColumn(key, value)
			}
		}
		return nil
	}
}

func NewPostgresDestination(db *sql.DB, options ...postgresDestinationOption) (*postgresDestination, error) {
	// init the dependencies
	_db := &realDB{db: db}
	txprovider, router, schema := newPostgresTransactionProvider(_db), newPostgresRoutingTable(), newPostgresSchema()
	// init the destination w/ options
	dest := &postgresDestination{db: _db, router: router, schema: schema, txprovider: txprovider}
	for _, apply := range options {
		if err := apply(dest); err != nil {
			return nil, err
		}
	}
	return dest, nil
}

func (d *postgresDestination) setDB(db sqlDB) {
	d.db = db
	d.txprovider = newPostgresTransactionProvider(db)
}

func (d *postgresDestination) Deliver(ctx context.Context, msg *Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return d.txprovider.do(ctx, func(tx sqlTx) error {
		queues := d.router.routeToQueues(msg.GetTopic())
		for _, queue := range queues {
			if err := d.insertMessage(&postgresDestinationInsertMessageArgs{
				name:        msg.GetName(),
				payload:     payload,
				publishedAt: msg.GetPublishedAt(),
				queue:       queue,
				topic:       msg.GetTopic(),
				tx:          tx,
				uuid:        msg.GetUUID(),
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

type postgresDestinationInsertMessageArgs struct {
	name        string
	payload     []byte
	publishedAt time.Time
	queue       string
	topic       string
	tx          sqlTx
	uuid        string
}

func (d *postgresDestination) insertMessage(args *postgresDestinationInsertMessageArgs) error {
	columns := fmt.Sprintf("(%s, %s, %s, %s, %s, %s, %s, %s)",
		d.schema.columns[postgresColumnStatus],
		d.schema.columns[postgresColumnTopic],
		d.schema.columns[postgresColumnQueue],
		d.schema.columns[postgresColumnPublishedAt],
		d.schema.columns[postgresColumnDeliverAt],
		d.schema.columns[postgresColumnUuid],
		d.schema.columns[postgresColumnName],
		d.schema.columns[postgresColumnPayload],
	)
	query := fmt.Sprintf(
		`
		INSERT INTO %s %s
		VALUES ('pending', $1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (%s, %s) DO NOTHING
		`,
		d.schema.table,
		columns,
		d.schema.columns[postgresColumnQueue],
		d.schema.columns[postgresColumnUuid],
	)
	execArgs := []any{args.topic, args.queue, args.publishedAt.UTC(), args.publishedAt.UTC(), args.uuid, args.name, args.payload}
	_, err := args.tx.Exec(query, execArgs...)
	return err
}

func WithTx(ctx context.Context, tx sqlTx) context.Context {
	return context.WithValue(ctx, postgresContextKeyForTx, tx)
}
