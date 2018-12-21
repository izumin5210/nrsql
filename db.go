package nrsql

import (
	"context"
	"database/sql"

	"github.com/izumin5210/isql"
)

type dbWrapper struct {
	original *sql.DB
	isql.Queryer
	isql.Execer

	config *Config
}

// Wrap wraps a *sql.DB object to measure performances and sent them to New Relic.
func Wrap(db *sql.DB, opts ...Option) isql.DB {
	cfg := createConfig(opts)
	return &dbWrapper{
		original: db,
		Queryer:  wrapQueryer(db, cfg),
		Execer:   wrapExecer(db, cfg),
		config:   cfg,
	}
}

func (w *dbWrapper) Prepare(query string) (isql.Stmt, error) {
	return w.PrepareContext(context.Background(), query)
}

func (w *dbWrapper) PrepareContext(ctx context.Context, query string) (isql.Stmt, error) {
	stmt, err := w.original.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return wrapStmt(stmt, w.config, parseQuery(query)), nil
}

func (w *dbWrapper) Ping() error {
	return w.original.Ping()
}

func (w *dbWrapper) PingContext(ctx context.Context) error {
	return w.original.PingContext(ctx)
}

func (w *dbWrapper) Close() error {
	return w.original.Close()
}

func (w *dbWrapper) Begin() (isql.Tx, error) {
	tx, err := w.original.Begin()
	if err != nil {
		return nil, err
	}
	return wrapTx(tx, w.config), nil
}

func (w *dbWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (isql.Tx, error) {
	tx, err := w.original.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return wrapTx(tx, w.config), nil
}

func (w *dbWrapper) DB() *sql.DB {
	return w.original
}
