package opinionatedevents

import (
	"database/sql"
	"embed"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

//go:embed migrations/*
var migrationFiles embed.FS

func forEveryMigration(do func(name string, content []byte) error) error {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}
	// TODO: can this be trusted to be in the correct order?
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := migrationFiles.ReadFile(fmt.Sprintf("migrations/%s", entry.Name()))
		if err != nil {
			return err
		}
		if err := do(entry.Name(), content); err != nil {
			return err
		}
	}
	return nil
}

func up(db *sql.DB, schema string, idx int, migration string) error {
	tx, err := db.Begin()
	defer tx.Rollback() //nolint the error is not relevant
	if err != nil {
		return err
	}
	migration = strings.ReplaceAll(migration, ":SCHEMA", schema)
	_, err = tx.Exec(migration)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		fmt.Sprintf("insert into %s.migrations (id) values ($1)", schema),
		idx,
	)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func getLatestMigration(db *sql.DB, schema string) (int, error) {
	row := db.QueryRow(fmt.Sprintf("select id from %s.migrations order by id desc limit 1", schema))
	var id int
	if err := row.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return id, nil
}

func installSchema(db *sql.DB, schema string) error {
	_, err := db.Exec(
		fmt.Sprintf(
			`
			create schema %[1]s;
			create table %[1]s.migrations (
				id int primary key,
				ts timestamptz default now() not null
			);
			`,
			schema,
		),
	)
	return err
}

func migrate(db *sql.DB, schema string) error {
	latestMigration, err := getLatestMigration(db, schema)
	if err != nil {
		if pqerr, ok := err.(*pq.Error); ok {
			if pqerr.Code != "42P01" {
				return pqerr
			}
			if err := installSchema(db, schema); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return forEveryMigration(func(name string, content []byte) error {
		idx, err := strconv.Atoi(name[:6])
		if err != nil {
			return err
		}
		if idx <= latestMigration {
			return nil
		}
		fmt.Printf("running migration %s...\n", name)
		if err := up(db, schema, idx, string(content)); err != nil {
			return err
		}
		return nil
	})
}
