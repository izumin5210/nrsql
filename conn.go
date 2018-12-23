package nrsql

import (
	"context"
	"database/sql/driver"
)

type iConn interface {
	driver.Pinger
	driver.Execer
	driver.ExecerContext
	driver.Queryer
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
}

type nrConn struct {
	original driver.Conn
	segmenter
}

func wrapConn(conn driver.Conn, segmenter segmenter) driver.Conn {
	c := &nrConn{
		original:  conn,
		segmenter: segmenter,
	}

	// https://github.com/opencensus-integrations/ocsql/blob/v0.1.2/driver_go1.10.go#L33-L60
	var (
		n, hasNameValueChecker = conn.(driver.NamedValueChecker)
		s, hasSessionResetter  = conn.(driver.SessionResetter)
	)
	switch {
	case !hasNameValueChecker && !hasSessionResetter:
		return c
	case hasNameValueChecker && !hasSessionResetter:
		return struct {
			iConn
			driver.NamedValueChecker
		}{c, n}
	case !hasNameValueChecker && hasSessionResetter:
		return struct {
			iConn
			driver.SessionResetter
		}{c, s}
	case hasNameValueChecker && hasSessionResetter:
		return struct {
			iConn
			driver.NamedValueChecker
			driver.SessionResetter
		}{c, n, s}
	}
	panic("unreachable")
}

// Prepare implements database/sql/driver.Conn interface.
func (c *nrConn) Prepare(query string) (driver.Stmt, error) {
	return c.original.Prepare(query)
}

// Close implements database/sql/driver.Conn interface.
func (c *nrConn) Close() error {
	return c.original.Close()
}

// Begin implements database/sql/driver.Conn interface.
func (c *nrConn) Begin() (driver.Tx, error) {
	return c.original.Begin()
}

// PrepareContext implements database/sql/driver.ConnPrepareContext interface.
func (c *nrConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	conn, ok := c.original.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	seg := c.Segment(ctx, &segmentParams{Operation: "PREPARE", Query: query})
	defer seg.End()

	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return wrapStmt(stmt, query, c.segmenter), nil
}

// BeginTx implements database/sql/driver.ConnBeginTx interface.
func (c *nrConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn, ok := c.original.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}

	seg := c.Segment(ctx, &segmentParams{Operation: "BEGIN"})
	defer seg.End()

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return wrapTx(tx, ctx, c.segmenter), nil
}

// Ping implements database/sql/driver.Pinger interface.
func (c *nrConn) Ping(ctx context.Context) (err error) {
	pinger, ok := c.original.(driver.Pinger)
	if !ok {
		return driver.ErrSkip
	}

	seg := c.Segment(ctx, &segmentParams{Operation: "PING"})
	defer seg.End()

	return pinger.Ping(ctx)
}

// Exec implements database/sql/driver.Execer interface.
func (c *nrConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	execer, ok := c.original.(driver.Execer)
	if !ok {
		return nil, driver.ErrSkip
	}

	return execer.Exec(query, args)
}

// ExecContext implements database/sql/driver.ExecerContext interface.
func (c *nrConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.original.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	seg := c.Segment(ctx, &segmentParams{Query: query, NamedArgs: args})
	defer seg.End()

	return execer.ExecContext(ctx, query, args)
}

// Query implements database/sql/driver.Queryer interface.
func (c *nrConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	queryer, ok := c.original.(driver.Queryer)
	if !ok {
		return nil, driver.ErrSkip
	}

	return queryer.Query(query, args)
}

// QueryContext implements database/sql/driver.QueryerContext interface.
func (c *nrConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.original.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	seg := c.Segment(ctx, &segmentParams{Query: query, NamedArgs: args})
	defer seg.End()

	return queryer.QueryContext(ctx, query, args)
}
