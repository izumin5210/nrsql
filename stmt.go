package nrsql

import (
	"context"
	"database/sql/driver"
)

type nrStmt struct {
	original driver.Stmt
	query    string
	segmenter
}

func wrapStmt(stmt driver.Stmt, query string, segmenter segmenter) driver.Stmt {
	s := &nrStmt{
		original:  stmt,
		query:     query,
		segmenter: segmenter,
	}

	// https://github.com/opencensus-integrations/ocsql/blob/v0.1.2/driver_go1.10.go#L62-L171
	var (
		_, hasExeCtx    = stmt.(driver.StmtExecContext)
		_, hasQryCtx    = stmt.(driver.StmtQueryContext)
		c, hasColConv   = stmt.(driver.ColumnConverter)
		n, hasNamValChk = stmt.(driver.NamedValueChecker)
	)

	switch {
	case !hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
		}{s}
	case !hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
		}{s, s}
	case hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
		}{s, s}
	case hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, s, s}
	case !hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
		}{s, c}
	case !hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, s, c}

	case !hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.NamedValueChecker
		}{s, n}
	case !hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, s, n}
	case !hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, c, n}
	case !hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, s, c, n}
	}
	panic("unreachable")
}

// Close implements database/sql/driver.Stmt interface.
func (s *nrStmt) Close() error { return s.original.Close() }

// NumInput implements database/sql/driver.Stmt interface.
func (s *nrStmt) NumInput() int { return s.original.NumInput() }

// Exec implements database/sql/driver.Stmt interface.
func (s *nrStmt) Exec(args []driver.Value) (driver.Result, error) { return s.original.Exec(args) }

// Query implements database/sql/driver.Stmt interface.
func (s *nrStmt) Query(args []driver.Value) (driver.Rows, error) { return s.original.Query(args) }

// ExecContext implements database/sql/driver.StmtExecContext interface.
func (s *nrStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmt, ok := s.original.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	seg := s.Segment(ctx, &segmentParams{Query: s.query, NamedArgs: args})
	defer seg.End()

	return stmt.ExecContext(ctx, args)
}

// QueryContext implements database/sql/driver.StmtQueryContext interface.
func (s *nrStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stmt, ok := s.original.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	seg := s.Segment(ctx, &segmentParams{Query: s.query, NamedArgs: args})
	defer seg.End()

	return stmt.QueryContext(ctx, args)
}
