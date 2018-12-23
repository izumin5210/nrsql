package nrsql

import (
	"testing"
)

func Test_parseQuery(t *testing.T) {
	cases := []struct {
		test  string
		query string
		table string
		op    string
	}{
		{
			test:  "simple select",
			query: "select * from books as b where b.category_id = 1 order by b.published_at desc limit 100",
			table: "books",
			op:    "SELECT",
		},
		{
			test:  "simple insert",
			query: "INSERT INTO libraries (location, name) VALUES ('Tokyo', 'Awesome Library')",
			table: "libraries",
			op:    "INSERT",
		},
		{
			test:  "simple update",
			query: "update books set title = 'The Go Programming Language' where id = 100",
			table: "books",
			op:    "UPDATE",
		},
		{
			test:  "simple delete",
			query: "delete from books where title = 'The Go Programming Language' and id = 100",
			table: "books",
			op:    "DELETE",
		},
		{
			test:  "quoted select",
			query: `select * from "books" where "category_id" = 1 order by "published_at" desc limit 100`,
			table: "books",
			op:    "SELECT",
		},
	}

	for _, c := range cases {
		t.Run(c.test, func(t *testing.T) {
			q := parseQuery(c.query)

			if got, want := q.Operation, c.op; got != want {
				t.Errorf("parseQuery() returned an operation %q, want %q", got, want)
			}

			// if got, want := q.TableName, c.table; got != want {
			// 	t.Errorf("parseQuery() returned a table name %q, want %q", got, want)
			// }

			if got, want := q.Raw, c.query; got != want {
				t.Errorf("parseQuery() returned a raw query %q, want %q", got, want)
			}
		})
	}
}
