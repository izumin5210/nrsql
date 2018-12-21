package nrsql

import (
	"database/sql"

	"github.com/izumin5210/isql"
)

// Stmt wraps a *sql.Stmt object.
type Stmt interface {
	isql.Stmt
	parsedQuery() *query
}

type stmtWrapper struct {
	original *sql.Stmt
	isql.PreparedQueryer
	isql.PreparedExecer

	query *query
}

func wrapStmt(stmt *sql.Stmt, cfg *Config, query *query) Stmt {
	return &stmtWrapper{
		original:        stmt,
		PreparedQueryer: wrapPreparedQueryer(stmt, query, cfg),
		PreparedExecer:  wrapPreparedExecer(stmt, query, cfg),
		query:           query,
	}
}

func (w *stmtWrapper) Close() error {
	return w.original.Close()
}

func (w *stmtWrapper) Stmt() *sql.Stmt {
	return w.original
}

func (w *stmtWrapper) parsedQuery() *query {
	return w.query
}
