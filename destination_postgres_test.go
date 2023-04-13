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
		destination, err := NewPostgresDestination(nil)
		assert.NoError(t, err)
		destination.setDB(db)
		for i := 0; i < 3; i += 1 {
			msg, err := NewMessage("customers.created", nil)
			assert.NoError(t, err)
			err = destination.Deliver(context.Background(), msg)
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
		destination, err := NewPostgresDestination(nil)
		assert.NoError(t, err)
		destination.setDB(db)
		newDB := &testDB{}
		tx, err := newDB.Begin()
		assert.NoError(t, err)
		ctx := context.Background()
		ctx = WithTx(ctx, tx)
		for i := 0; i < 3; i += 1 {
			msg, err := NewMessage("customers.created", nil)
			assert.NoError(t, err)
			err = destination.Deliver(ctx, msg)
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
		assert.Equal(t, 0, newDB.transactions[0].commitCount)
		assert.Equal(t, 0, newDB.transactions[0].rollbackCount)
	})

	t.Run("messages to all queues are sent in one transaction", func(t *testing.T) {
		db := &testDB{}
		destination, err := NewPostgresDestination(nil,
			PostgresDestinationWithTopicToQueues("customers", "topic.1", "topic.2"),
		)
		assert.NoError(t, err)
		destination.setDB(db)
		msg, err := NewMessage("customers.created", nil)
		assert.NoError(t, err)
		err = destination.Deliver(context.Background(), msg)
		assert.NoError(t, err)
		// there should be one transaction from the internal db
		assert.Equal(t, 1, db.beginCount)
		assert.Len(t, db.transactions, 1)
		tx := db.transactions[0]
		// the message should have been inserted twice (to both target queues)
		assert.Equal(t, 2, tx.execCount)
		assert.Equal(t, 1, tx.commitCount)
		assert.Equal(t, 0, tx.rollbackCount)
	})

	t.Run("table schema can be overridden", func(t *testing.T) {
		db := &testDB{}
		tableName := "thisisthetablename"
		columnNames := map[string]string{
			"id":           "thecolumnname_id",
			"name":         "thecolumnname_name",
			"payload":      "thecolumnname_payload",
			"published_at": "thecolumnname_published_at",
			"queue":        "thecolumnname_queue",
			"status":       "thecolumnname_status",
			"topic":        "thecolumnname_topic",
			"uuid":         "thecolumnname_uuid",
		}
		destination, err := NewPostgresDestination(nil,
			PostgresDestinationWithTopicToQueues("customers", "topic.1", "topic.2"),
			PostgresDestinationWithTableName(tableName),
			PostgresDestinationWithColumnNames(columnNames),
		)
		assert.NoError(t, err)
		destination.setDB(db)
		msg, err := NewMessage("customers.created", nil)
		assert.NoError(t, err)
		err = destination.Deliver(context.Background(), msg)
		assert.NoError(t, err)
		// there should be one transaction from the internal db
		assert.Equal(t, 1, db.beginCount)
		assert.Len(t, db.transactions, 1)
		tx := db.transactions[0]
		// the message should have been inserted twice (to both target queues)
		assert.Equal(t, 2, tx.execCount)
		assert.Equal(t, 1, tx.commitCount)
		assert.Equal(t, 0, tx.rollbackCount)
		// the actual query should include the table and column names
		query := tx.queries[0]
		assert.Contains(t, query, tableName)
		for original, columnName := range columnNames {
			// some of the columns are not supposed to be in the insert query
			if original == "id" {
				continue
			}

			assert.Contains(t, query, columnName)
		}
	})
}

type testTx struct {
	queries []string

	execCount     int
	commitCount   int
	rollbackCount int
}

func (ttx *testTx) Exec(query string, args ...any) (sql.Result, error) {
	ttx.queries = append(ttx.queries, query)
	ttx.execCount += 1
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
