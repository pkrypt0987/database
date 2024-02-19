// Disclaimer: This source code is forked from github.com/jvongxay0308/database-go
// For the purpose of learning and education only with the intention
// to understand how the database transaction works in PostgreSQL.
package database

import (
	"context"
	"database/sql"
	"time"
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
