package nrsql

import (
	"strings"
)

type query struct {
	Operation string
	TableName string
	Raw       string
}

func parseQuery(queryStr string) *query {
	q := &query{Raw: queryStr}
	q.Operation = strings.ToUpper(strings.Split(queryStr, " ")[0])
	return q
}
