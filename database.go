// Disclaimer: This source code is forked from github.com/jvongxay0308/database-go
// For the purpose of learning and education only with the intention
// to understand how the database transaction works in PostgreSQL.
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/lib/pq"
)

type DB struct {
	db        *sql.DB
	tx        *sql.Tx
	conn      *sql.Conn
	txOptions sql.TxOptions
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(5 * time.Minute)
	return &DB{db: db}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	res, err := db.exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (db *DB) exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.tx != nil {
		return db.tx.ExecContext(ctx, query, args...)
	}
	return db.db.ExecContext(ctx, query, args...)
}

func (db *DB) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if db.tx != nil {
		return db.tx.QueryContext(ctx, query, args...)
	}
	return db.db.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if db.tx != nil {
		return db.tx.QueryRowContext(ctx, query, args...)
	}
	return db.db.QueryRowContext(ctx, query, args...)
}

func (db *DB) Transaction(ctx context.Context, opts *sql.TxOptions, f func(*DB) error) error {
	if canRetry(opts.Isolation) {
		return db.transactionWithRetry(ctx, opts, f)
	}
	return db.transaction(ctx, opts, f)
}

func canRetry(iso sql.IsolationLevel) bool {
	return iso == sql.LevelRepeatableRead || iso == sql.LevelSerializable
}

func (db *DB) transactionWithRetry(ctx context.Context, opts *sql.TxOptions, f func(*DB) error) error {
	const maxRetries = 3
	dur := 150 * time.Millisecond
	for i := 0; i < maxRetries; i++ {
		err := db.transaction(ctx, opts, f)
		if isSerializationFailure(err) {
			time.Sleep(dur)
			dur *= 2
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), serializationFailureCode) {
				return fmt.Errorf("serialization failure")
			}
		}
		return err
	}
	return fmt.Errorf("exceeded max retries")
}

const serializationFailureCode = "40001"

func isSerializationFailure(err error) bool {
	var perr *pq.Error
	if errors.As(err, &perr) && perr.Code == serializationFailureCode {
		return true
	}
	var gerr *pgconn.PgError
	if errors.As(err, &gerr) && gerr.Code == serializationFailureCode {
		return true
	}
	return false
}

func (db *DB) transaction(ctx context.Context, opts *sql.TxOptions, f func(*DB) error) error {
	if db.tx != nil {
		return fmt.Errorf("There is already a transaction in progress")
	}

	conn, err := db.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("db.Conn(): %w", err)
	}
	defer conn.Close()

	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("db.BeginTx(): %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			if err := tx.Commit(); err != nil {
				log.Printf("tx.Commit(): %v", err)
			}
		}
	}()

	dbtx := &DB{db: db.db}
	dbtx.tx = tx
	dbtx.conn = conn
	dbtx.txOptions = *opts
	if err := f(dbtx); err != nil {
		return fmt.Errorf("call f(): %w", err)
	}
	return nil
}
