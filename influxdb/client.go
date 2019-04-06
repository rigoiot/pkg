package influxdb

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client"
	"github.com/rigoiot/pkg/logger"
)

// Point ...
type Point = client.Point

// BatchPoints ...
type BatchPoints = client.BatchPoints

// Response ...
type Response = client.Response

// Client ...
type Client struct {
	*client.Client
	db string
}

// NewClient ...
func NewClient(URL, db, username, password string) (*Client, error) {
	host, err := url.Parse(URL)
	if err != nil {
		logger.Errorf("Fail to parse URL: %s error: %s", URL, err.Error())
		return nil, err
	}

	conn, err := client.NewClient(client.Config{
		URL:      *host,
		Username: username,
		Password: password,
	})
	if err != nil {
		logger.Errorf("Fail to create InfluxDB client on URL: %s error: %s", URL, err.Error())
		return nil, err
	}

	return &Client{
		conn,
		db,
	}, nil
}

// WritePoint ...
func (c *Client) WritePoint(pt Point, rp ...string) error {
	pts := []Point{pt}
	return c.WritePoints(pts, rp...)
}

// WritePoints ...
func (c *Client) WritePoints(pts []Point, rp ...string) error {
	RetentionPolicy := "autogen"
	if len(rp) > 0 {
		RetentionPolicy = rp[0]
	}
	bps := client.BatchPoints{
		Points:          pts,
		Database:        c.db,
		RetentionPolicy: RetentionPolicy,
		Time:            time.Now(),
		Precision:       "s",
	}

	logger.Debugf("Store points:%s to TSDB(%s)", bps, c.db)

	_, err := c.Client.Write(bps)
	return err
}

// QueryData ...
func (c *Client) QueryData(fields []string, measurement, where string, limit uint64, offset uint64) (*Response, error) {

	// Process fields
	f := "*"
	if len(fields) > 0 {
		f = strings.Join(fields, ",")
	}
	cmd := fmt.Sprintf("select %s from %s", f, measurement)
	// process where
	if where != "" {
		cmd = fmt.Sprintf("%s %s", cmd, where)
	}

	if limit != 0 {
		cmd = fmt.Sprintf("limit %d", limit)
	}

	cmd = fmt.Sprintf("offset %d", offset)

	q := client.Query{
		Command:  cmd,
		Database: c.db,
	}
	r, err := c.Client.Query(q)
	if err != nil || r.Error() != nil {
		logger.Errorf("Fail to query(%s), error: %v, %v", cmd, err, r.Error())
		return r, err
	}

	return r, nil
}
