# nrsql
[![GoDoc](https://godoc.org/github.com/izumin5210/nrsql?status.svg)](https://godoc.org/github.com/izumin5210/nrsql)

SQL database driver wrapper with New Relic instrumentation for Go.


## Usage

```go
driver := &pq.Driver{}
driver = nrsql.Wrap(
	driver,
	nrsql.WithDBName("foobar"),
	nrsql.WithDatastore(newrelic.DatastorePostgres),
)
sql.Register("foobar-postgres", driver)

db, err := sql.Open("foobar-postgres", databaseURL)
// ...
```


## Author
- Masayuki Izumi ([@izumin5210](https://github.com/izumin5210))


## License
licensed under the MIT License. See [LICENSE](./LICENSE)
