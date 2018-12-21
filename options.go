package nrsql

import newrelic "github.com/newrelic/go-agent"

// Config contains metadata to send to New Relic.
type Config struct {
	Datastore    newrelic.DatastoreProduct
	DBName       string
	Host         string
	PortPathOrID string
}

func createConfig(opts []Option) *Config {
	cfg := &Config{}
	for _, f := range opts {
		f(cfg)
	}
	return cfg
}

// Option configures a Config object.
type Option func(*Config)

// WithDatastore sets a datestore type.
func WithDatastore(d newrelic.DatastoreProduct) Option {
	return func(c *Config) {
		c.Datastore = d
	}
}

// WithDBName sets a DB name.
func WithDBName(dbName string) Option {
	return func(c *Config) {
		c.DBName = dbName
	}
}

// WithHost sets a DB host.
func WithHost(host string) Option {
	return func(c *Config) {
		c.Host = host
	}
}

// WithPortPathOrID sets a DB port, path or id.
func WithPortPathOrID(v string) Option {
	return func(c *Config) {
		c.PortPathOrID = v
	}
}
