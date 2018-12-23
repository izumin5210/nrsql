package nrsql

import (
	"context"
	"database/sql/driver"
)

// Wrap returns a wrapped Driver with New Relic insturmentaiton.
func Wrap(d driver.Driver, opts ...Option) driver.Driver {
	cfg := createConfig(opts)
	return wrapDriver(d, cfg)
}

type nrDriver struct {
	original  driver.Driver
	segmenter segmenter
}

func wrapDriver(d driver.Driver, cfg *Config) driver.Driver {
	wd := &nrDriver{original: d, segmenter: newSegmenter(cfg)}
	if _, ok := d.(driver.DriverContext); ok {
		return wd
	}
	return struct{ driver.Driver }{wd}
}

// Open implements database/sql/driver.Driver interface.
func (d *nrDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.original.Open(name)
	if err != nil {
		return nil, err
	}
	return wrapConn(conn, d.segmenter), nil
}

// OpenConnector implements database/sql/driver.OpenConnector interface.
func (d *nrDriver) OpenConnector(name string) (driver.Connector, error) {
	c, err := d.original.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return wrapConnector(c, d, d.segmenter), nil
}

type nrConnector struct {
	original  driver.Connector
	driver    driver.Driver
	segmenter segmenter
}

func wrapConnector(c driver.Connector, d driver.Driver, s segmenter) driver.Connector {
	return &nrConnector{original: c, driver: d, segmenter: s}
}

// Connect implements database/sql/driver.Connector interface.
func (c *nrConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.original.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return wrapConn(conn, c.segmenter), nil
}

// Driver implements database/sql/driver.Connector interface.
func (c *nrConnector) Driver() driver.Driver {
	return c.driver
}
