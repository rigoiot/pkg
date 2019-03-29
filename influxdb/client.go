package influxdb

import (
	"net/url"
	"time"

	client "github.com/influxdata/influxdb1-client"
	"github.com/rigoiot/pkg/logger"
)

// Point ...
type Point = client.Point

// BatchPoints ...
type BatchPoints = client.BatchPoints

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
