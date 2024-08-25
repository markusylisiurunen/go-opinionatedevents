package opinionatedevents

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresDestination(t *testing.T) {
	t.Run("inserts an event to the database", func(t *testing.T) {
		db := &testDB{}
		destination, err := NewPostgresDestination(nil, skipMigrations())
		assert.NoError(t, err)
		destination.setDB(db)
		destination.setRouting(newTestRouting([]string{"default"}))
		for i := 0; i < 3; i += 1 {
			msg, err := NewMessage("customers.created", nil)
			assert.NoError(t, err)
			err = destination.Deliver(context.Background(), []*Message{msg})
			assert.NoError(t, err)
		}
		// each message should have been sent in its own transaction
		assert.Equal(t, 3, db.beginCount)
		assert.Len(t, db.transactions, 3)
		for _, tx := range db.transactions {
			// each transaction must be committed
			assert.Equal(t, 1, tx.commitCount)
			assert.Equal(t, 0, tx.rollbackCount)
		}
	})

	t.Run("uses the provided transaction from context", func(t *testing.T) {
		db := &testDB{}
		destination, err := NewPostgresDestination(nil, skipMigrations())
		assert.NoError(t, err)
		destination.setDB(db)
		destination.setRouting(newTestRouting([]string{"default"}))
		newDB := &testDB{}
		tx, err := newDB.Begin()
		assert.NoError(t, err)
		ctx := context.Background()
		ctx = WithTx(ctx, tx)
		for i := 0; i < 3; i += 1 {
			msg, err := NewMessage("customers.created", nil)
			assert.NoError(t, err)
			err = destination.Deliver(ctx, []*Message{msg})
			assert.NoError(t, err)
		}
		// the internal db should not have been used...
		assert.Equal(t, 0, db.beginCount)
		assert.Len(t, db.transactions, 0)
		// ...but the external one should have
		assert.Equal(t, 1, newDB.beginCount)
		assert.Len(t, newDB.transactions, 1)
		// make sure that the destination did not commit the transaction
		assert.Equal(t, 3, newDB.transactions[0].execCount)
		assert.Equal(t, 0, newDB.transactions[0].queryCount)
		assert.Equal(t, 0, newDB.transactions[0].commitCount)
		assert.Equal(t, 0, newDB.transactions[0].rollbackCount)
	})

	t.Run("messages to all queues are sent in one transaction", func(t *testing.T) {
		db := &testDB{}
		destination, err := NewPostgresDestination(nil,
			skipMigrations(),
		)
		assert.NoError(t, err)
		destination.setDB(db)
		destination.setRouting(newTestRouting([]string{"topic.1", "topic.2"}))
		msg, err := NewMessage("customers.created", nil)
		assert.NoError(t, err)
		err = destination.Deliver(context.Background(), []*Message{msg})
		assert.NoError(t, err)
		// there should be one transaction from the internal db
		assert.Equal(t, 1, db.beginCount)
		assert.Len(t, db.transactions, 1)
		tx := db.transactions[0]
		// the message should have been inserted twice (to both target queues)
		assert.Equal(t, 2, tx.execCount)
		assert.Equal(t, 0, tx.queryCount)
		assert.Equal(t, 1, tx.commitCount)
		assert.Equal(t, 0, tx.rollbackCount)
	})
}

func skipMigrations() postgresDestinationOption {
	return func(dest *postgresDestination) error {
		dest.skipMigrations = true
		return nil
	}
}

type testTx struct {
	queries []string

	execCount     int
	queryCount    int
	commitCount   int
	rollbackCount int
}

func (ttx *testTx) Exec(query string, args ...any) (sql.Result, error) {
	ttx.queries = append(ttx.queries, query)
	ttx.execCount += 1
	return nil, nil
}

func (ttx *testTx) Query(query string, args ...any) (*sql.Rows, error) {
	ttx.queries = append(ttx.queries, query)
	ttx.queryCount += 1
	return nil, nil
}

func (ttx *testTx) Commit() error {
	if ttx.commitCount+ttx.rollbackCount > 0 {
		panic(errors.New("cannot commit after commit or rollback"))
	}
	ttx.commitCount += 1
	return nil
}

func (ttx *testTx) Rollback() error {
	if ttx.commitCount+ttx.rollbackCount > 0 {
		// a rollback after either a commit or rollback is no-op
		return nil
	}
	ttx.rollbackCount += 1
	return nil
}

type testDB struct {
	beginCount   int
	transactions []*testTx
}

func (tdb *testDB) Begin() (sqlTx, error) {
	tx := &testTx{}
	tdb.transactions = append(tdb.transactions, tx)
	tdb.beginCount += 1
	return tx, nil
}

type testRouting struct {
	_queues []string
}

func newTestRouting(queues []string) *testRouting {
	return &testRouting{_queues: queues}
}

func (tr *testRouting) queues(tx sqlTx, topic string) ([]string, error) {
	return tr._queues, nil
}
