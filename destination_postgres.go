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

// Database connection
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

// Routing table
// ---

type postgresRoutingTable struct {
	topicToQueues map[string][]string
}

func newPostgresRoutingTable() *postgresRoutingTable {
	return &postgresRoutingTable{
		topicToQueues: map[string][]string{},
	}
}

func (t *postgresRoutingTable) setTopicQueues(topic string, queues ...string) {
	if _, ok := t.topicToQueues[topic]; ok {
		t.topicToQueues[topic] = append(t.topicToQueues[topic], queues...)
	} else {
		t.topicToQueues[topic] = queues
	}
}

func (t *postgresRoutingTable) route(topic string) []string {
	if queues, ok := t.topicToQueues[topic]; ok {
		return queues
	}

	return []string{"default"}
}

// Database schema
// ---

type postgresSchemaColumn string

const (
	postgresColumnDeliverAt        postgresSchemaColumn = postgresSchemaColumn("deliver_at")
	postgresColumnDeliveryAttempts                      = postgresSchemaColumn("delivery_attempts")
	postgresColumnId                                    = postgresSchemaColumn("id")
	postgresColumnName                                  = postgresSchemaColumn("name")
	postgresColumnPayload                               = postgresSchemaColumn("payload")
	postgresColumnQueue                                 = postgresSchemaColumn("queue")
	postgresColumnStatus                                = postgresSchemaColumn("status")
	postgresColumnTimestamp                             = postgresSchemaColumn("timestamp")
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
			postgresColumnQueue:            "queue",
			postgresColumnStatus:           "status",
			postgresColumnTimestamp:        "timestamp",
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

// Transaction provider
// ---

const (
	postgresTransactionContextKey postgresContextKey = postgresContextKey("tx")
)

type postgresTransactionProvider struct {
	db sqlDB
}

func newPostgresTransactionProvider(db sqlDB) *postgresTransactionProvider {
	return &postgresTransactionProvider{
		db: db,
	}
}

func (tp *postgresTransactionProvider) do(ctx context.Context, action func(tx sqlTx) error) error {
	var tx sqlTx
	var commitable bool

	if val, ok := ctx.Value(postgresTransactionContextKey).(sqlTx); ok {
		tx = val
		commitable = false
	} else {
		_tx, err := tp.db.Begin()
		if err != nil {
			return err
		}
		tx = _tx
		commitable = true
	}

	if err := action(tx); err != nil {
		if commitable {
			if txErr := tx.Rollback(); txErr != nil {
				return txErr
			}
		}

		return err
	}

	if commitable {
		if txErr := tx.Commit(); txErr != nil {
			return txErr
		}
	}

	return nil
}

// Destination
// ---

type postgresDestinationOption func(d *PostgresDestination) error

type PostgresDestination struct {
	db sqlDB

	tx     *postgresTransactionProvider
	router *postgresRoutingTable
	schema *postgresSchema
}

func NewPostgresDestination(connectionString string, options ...postgresDestinationOption) (*PostgresDestination, error) {
	_db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	db := &realDB{db: _db}

	tx, router, schema := newPostgresTransactionProvider(db), newPostgresRoutingTable(), newPostgresSchema()
	destination := &PostgresDestination{db: db, tx: tx, router: router, schema: schema}

	for _, apply := range options {
		if err := apply(destination); err != nil {
			return nil, err
		}
	}

	return destination, nil
}

func (d *PostgresDestination) mockDB(db sqlDB) {
	d.db = db
	d.tx = newPostgresTransactionProvider(db)
}

func (d *PostgresDestination) Deliver(ctx context.Context, msg *Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	topic := strings.Split(msg.name, ".")[0]

	timestamp := msg.meta.timestamp
	uuid := msg.meta.uuid

	return d.tx.do(ctx, func(tx sqlTx) error {
		for _, queue := range d.router.route(topic) {
			if err := d.insertMessage(tx, topic, queue, timestamp, uuid, msg.name, payload); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *PostgresDestination) insertMessage(
	tx sqlTx,
	topic string,
	queue string,
	timestamp time.Time,
	uuid string,
	name string,
	payload []byte,
) error {
	columns := fmt.Sprintf("(%s, %s, %s, %s, %s, %s, %s, %s)",
		d.schema.columns[postgresColumnStatus],
		d.schema.columns[postgresColumnTopic],
		d.schema.columns[postgresColumnQueue],
		d.schema.columns[postgresColumnTimestamp],
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

	args := []any{topic, queue, timestamp.UTC(), timestamp.UTC(), uuid, name, payload}
	_, err := tx.Exec(query, args...)

	return err
}

func PostgresDestinationWithTopicToQueues(topic string, queues ...string) postgresDestinationOption {
	return func(d *PostgresDestination) error {
		d.router.setTopicQueues(topic, queues...)
		return nil
	}
}

func PostgresDestinationWithTableName(name string) postgresDestinationOption {
	return func(d *PostgresDestination) error {
		d.schema.setTable(name)
		return nil
	}
}

func PostgresDestinationWithColumnNames(names map[string]string) postgresDestinationOption {
	return func(d *PostgresDestination) error {
		for name, value := range names {
			key := postgresSchemaColumn(name)
			if _, ok := d.schema.columns[key]; ok {
				d.schema.setColumn(key, value)
			}
		}
		return nil
	}
}

func WithTx(ctx context.Context, tx sqlTx) context.Context {
	return context.WithValue(ctx, postgresTransactionContextKey, tx)
}
