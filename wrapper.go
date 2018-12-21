package nrsql

import (
	"context"
	"database/sql"

	"github.com/izumin5210/isql"
	newrelic "github.com/newrelic/go-agent"
)

func wrapQueryer(contextQueryer isql.ContextQueryer, cfg *Config) isql.Queryer {
	return &queryerWrapper{original: contextQueryer, config: cfg}
}

type queryerWrapper struct {
	original isql.ContextQueryer
	config   *Config
}

func (w *queryerWrapper) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return w.QueryContext(context.Background(), query, args...)
}

func (w *queryerWrapper) QueryRow(query string, args ...interface{}) *sql.Row {
	return w.QueryRowContext(context.Background(), query, args...)
}

func (w *queryerWrapper) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	segment(ctx, w.config, parseQuery(query), args, func() {
		rows, err = w.original.QueryContext(ctx, query, args...)
	})
	return
}

func (w *queryerWrapper) QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sql.Row) {
	segment(ctx, w.config, parseQuery(query), args, func() {
		row = w.original.QueryRowContext(ctx, query, args...)
	})
	return
}

func wrapExecer(execer isql.ContextExecer, cfg *Config) isql.Execer {
	return &execerWrapper{original: execer, config: cfg}
}

type execerWrapper struct {
	original isql.ContextExecer
	config   *Config
}

func (w *execerWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	return w.ExecContext(context.Background(), query, args...)
}

func (w *execerWrapper) ExecContext(ctx context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	segment(ctx, w.config, parseQuery(query), args, func() {
		res, err = w.original.ExecContext(ctx, query, args...)
	})
	return
}

func wrapPreparedQueryer(queryer isql.ContextPreparedQueryer, query *query, cfg *Config) isql.PreparedQueryer {
	return &preparedQueryerWrapper{original: queryer, query: query, config: cfg}
}

type preparedQueryerWrapper struct {
	original isql.ContextPreparedQueryer
	query    *query
	config   *Config
}

func (w *preparedQueryerWrapper) Query(args ...interface{}) (*sql.Rows, error) {
	return w.QueryContext(context.Background(), args...)
}

func (w *preparedQueryerWrapper) QueryRow(args ...interface{}) *sql.Row {
	return w.QueryRowContext(context.Background(), args...)
}

func (w *preparedQueryerWrapper) QueryContext(ctx context.Context, args ...interface{}) (rows *sql.Rows, err error) {
	segment(ctx, w.config, w.query, args, func() {
		rows, err = w.original.QueryContext(ctx, args...)
	})
	return
}

func (w *preparedQueryerWrapper) QueryRowContext(ctx context.Context, args ...interface{}) (row *sql.Row) {
	segment(ctx, w.config, w.query, args, func() {
		row = w.original.QueryRowContext(ctx, args...)
	})
	return
}

func wrapPreparedExecer(execer isql.ContextPreparedExecer, query *query, cfg *Config) isql.PreparedExecer {
	return &preparedExecerWrapper{original: execer, query: query, config: cfg}
}

type preparedExecerWrapper struct {
	original isql.ContextPreparedExecer
	query    *query
	config   *Config
}

func (w *preparedExecerWrapper) Exec(args ...interface{}) (sql.Result, error) {
	return w.ExecContext(context.Background(), args...)
}

func (w *preparedExecerWrapper) ExecContext(ctx context.Context, args ...interface{}) (res sql.Result, err error) {
	segment(ctx, w.config, w.query, args, func() {
		res, err = w.original.ExecContext(ctx, args...)
	})
	return
}

func segment(ctx context.Context, cfg *Config, q *query, args []interface{}, do func()) {
	seg := &newrelic.DatastoreSegment{
		StartTime:          newrelic.StartSegmentNow(newrelic.FromContext(ctx)),
		Product:            cfg.Datastore,
		Collection:         q.TableName,
		Operation:          q.Operation,
		ParameterizedQuery: q.Raw,
		Host:               cfg.Host,
		PortPathOrID:       cfg.PortPathOrID,
		DatabaseName:       cfg.DBName,
	}
	defer seg.End()

	do()
}
