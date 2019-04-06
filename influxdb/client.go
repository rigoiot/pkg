package influxdb

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/rigoiot/atlas-app-toolkit/query"

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

// Query ...
func (c *Client) Query(measurement string, filter *query.Filtering, orderBy *query.Sorting, fields *query.FieldSelection, paging *query.Pagination) (*Response, error) {
	// Process fields
	var fs []string
	if fields != nil {
		for field := range fields.Fields {
			fs = append(fs, field)
		}
	}
	f := "*"
	if len(fs) > 0 {
		f = strings.Join(fs, ",")
	}
	cmd := fmt.Sprintf("select %s from %s", f, measurement)

	// TODO: Process where

	// Process paging
	if paging != nil {
		limit := uint64(paging.Limit)
		if limit != 0 {
			cmd = fmt.Sprintf("%s limit %d", cmd, limit)
		}
		cmd = fmt.Sprintf("%s offset %d", cmd, uint64(paging.Offset))
	}

	// Process order, Only support time
	order := "time DESC"
	if orderBy != nil {
		for _, c := range orderBy.Criterias {
			if c.Tag == "time" && c.IsAsc() {
				order = "time AESC"
				break
			}
		}
	}
	cmd = fmt.Sprintf("%s order by %s", cmd, order)

	logger.Debugf("cmd: %s", cmd)

	q := client.Query{
		Command:  cmd,
		Database: c.db,
	}
	r, err := c.Client.Query(q)
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", cmd, err.Error())
		return r, err
	}

	if r != nil && r.Error() != nil {
		logger.Errorf("Fail to query(%s), response error: %v", cmd, r.Error())
		return r, r.Error()
	}

	return r, nil
}
