package nrsql

import (
	"context"
	"database/sql"

	"github.com/izumin5210/isql"
)

type txWrapper struct {
	original *sql.Tx
	isql.Queryer
	isql.Execer

	config *Config
}

func wrapTx(tx *sql.Tx, cfg *Config) isql.Tx {
	return &txWrapper{
		original: tx,
		Queryer:  wrapQueryer(tx, cfg),
		Execer:   wrapExecer(tx, cfg),
		config:   cfg,
	}
}

func (w *txWrapper) StmtContext(ctx context.Context, istmt isql.Stmt) isql.Stmt {
	var q *query
	if stmt, ok := istmt.(Stmt); ok {
		q = stmt.parsedQuery()
	}
	return wrapStmt(w.original.StmtContext(ctx, istmt.Stmt()), w.config, q)
}

func (w *txWrapper) Commit() error {
	return w.original.Commit()
}

func (w *txWrapper) Rollback() error {
	return w.original.Rollback()
}

func (w *txWrapper) Tx() *sql.Tx {
	return w.original
}
