package nrsql

import (
	"context"
	"database/sql/driver"
	"strconv"

	newrelic "github.com/newrelic/go-agent"
)

func newSegmenter(cfg *Config) segmenter {
	return &segmenterImpl{
		cfg: cfg,
	}
}

type segmenter interface {
	Segment(ctx context.Context, params *segmentParams) segment
}

type segmenterImpl struct {
	cfg *Config
}

type segment interface {
	End() error
}

func (s *segmenterImpl) Segment(ctx context.Context, params *segmentParams) segment {
	nrtxn := newrelic.FromContext(ctx)
	if nrtxn == nil {
		return &fakeSegment{}
	}

	seg := &newrelic.DatastoreSegment{
		StartTime:    newrelic.StartSegmentNow(nrtxn),
		Product:      s.cfg.Datastore,
		Host:         s.cfg.Host,
		PortPathOrID: s.cfg.PortPathOrID,
		DatabaseName: s.cfg.DBName,
	}
	if params != nil {
		seg.Collection = params.Collection
		seg.ParameterizedQuery = params.Query
		if op := params.Operation; op != "" {
			seg.Operation = params.Operation
		} else if params.Query != "" {
			q := parseQuery(params.Query)
			seg.Operation = q.Operation
		}
		if n := len(params.Args); n > 0 {
			seg.QueryParameters = make(map[string]interface{}, n)
			for i, arg := range params.Args {
				seg.QueryParameters["$"+strconv.Itoa(i+1)] = arg
			}
		}
		if n := len(params.NamedArgs); n > 0 {
			seg.QueryParameters = make(map[string]interface{}, n)
			for _, arg := range params.NamedArgs {
				key := arg.Name
				if key == "" {
					key = "$" + strconv.Itoa(arg.Ordinal)
				}
				seg.QueryParameters[key] = arg.Value
			}
		}
	}
	return seg
}

type segmentParams struct {
	Collection string
	Operation  string
	Query      string
	Args       []driver.Value
	NamedArgs  []driver.NamedValue
}

type fakeSegment struct{}

func (*fakeSegment) End() error { return nil }
