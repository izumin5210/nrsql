package nrsql

import (
	"strings"
	"unicode"
)

type query struct {
	Operation string
	TableName string
	Raw       string
}

func parseQuery(s string) *query {
	q := &query{Raw: s}
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, unicode.IsSpace)
	q.Operation = strings.ToUpper(s[:i])
	return q
}
