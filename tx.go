package nrsql

import (
	"context"
	"database/sql/driver"
)

type nrTx struct {
	original driver.Tx
	ctx      context.Context
	segmenter
}

func wrapTx(tx driver.Tx, ctx context.Context, s segmenter) driver.Tx {
	wt := &nrTx{
		original:  tx,
		ctx:       ctx,
		segmenter: s,
	}
	return wt
}

func (t *nrTx) Commit() error {
	seg := t.Segment(t.ctx, &segmentParams{Operation: "COMMIT"})
	defer seg.End()

	return t.original.Commit()
}

func (t *nrTx) Rollback() error {
	seg := t.Segment(t.ctx, &segmentParams{Operation: "ROLLBACK"})
	defer seg.End()

	return t.original.Rollback()
}
