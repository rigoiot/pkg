package influxdb

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rigoiot/atlas-app-toolkit/query"

	client "github.com/influxdata/influxdb1-client"
	v2client "github.com/rigoiot/influxdb-client-go"
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
	v2c *v2client.Client
	db  string
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

	v2c, err := v2client.New(nil, v2client.WithAddress(URL), v2client.WithUserAndPass(username, password))
	if err != nil {
		logger.Errorf("Fail to create InfluxDB v2client on URL: %s error: %s", URL, err.Error())
		return nil, err
	}

	return &Client{
		conn,
		v2c,
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
	cmd := fmt.Sprintf("SELECT %s FROM %s", f, measurement)

	// TODO: Process where

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
	cmd = fmt.Sprintf("%s ORDER BY %s", cmd, order)

	// Process paging
	if paging != nil {
		limit := uint64(paging.Limit)
		if limit != 0 {
			cmd = fmt.Sprintf("%s LIMIT %d", cmd, limit)
		}
		cmd = fmt.Sprintf("%s OFFSET %d", cmd, uint64(paging.Offset))
	}

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

// QueryCSV ...
func (c *Client) QueryCSV(ctx context.Context, flux string) (*v2client.QueryCSVResult, error) {

	r, err := c.v2c.QueryCSV(ctx, flux, "")
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return r, err
	}

	return r, nil
}

// QueryV2 ...
func (c *Client) QueryV2(ctx context.Context, measurement, policy, start, stop string, fields []string, tags map[string]string, keep []string, limit, offset int) ([]map[string]interface{}, error) {
	// check input
	if measurement == "" {
		return nil, fmt.Errorf("measurement should not be empty")
	}
	if start == "" {
		return nil, fmt.Errorf("start time should not be empty")
	}

	// Process from
	retenPolicy := "autogen"
	if policy != "" {
		retenPolicy = policy
	}
	from := fmt.Sprintf(`from(bucket: "%s/%s")`, c.db, retenPolicy)

	// Process range
	var t string
	if stop == "" {
		t = fmt.Sprintf("range(start: %s)", start)
	} else {
		t = fmt.Sprintf("range(start: %s, stop: %s)", start, stop)
	}

	// Process filter
	var filter string
	var fieldFilters []string
	for _, field := range fields {
		fieldFilters = append(fieldFilters, fmt.Sprintf(`r._field == "%s"`, field))
	}
	var tagFilters []string
	if len(tags) > 0 {
		for k, v := range tags {
			tagFilters = append(tagFilters, fmt.Sprintf(`r.%s == "%s"`, k, v))
		}
	}
	if len(tagFilters) > 0 {
		filter = fmt.Sprintf(`(%s) and (%s)`, strings.Join(fieldFilters, " or "), strings.Join(tagFilters, " and "))
	} else {
		filter = fmt.Sprintf(`%s`, strings.Join(fieldFilters, " or "))
	}
	f := fmt.Sprintf(`filter(fn: (r) => r._measurement == "%s" and (%s))`, measurement, filter)

	// Process limit
	l := fmt.Sprintf(`limit(n:%d, offset: %d)`, limit, offset)

	// Process keep
	var ks []string
	for _, item := range keep {
		ks = append(ks, fmt.Sprintf(`"%s"`, item))
	}
	ks = append(ks, []string{`"_start"`, `"_stop"`, `"_time"`, `"_value"`, `"_field"`}...)
	k := fmt.Sprintf(`keep(columns: [%s])`, strings.Join(ks, ","))

	flux := fmt.Sprintf(`%s|>%s|>%s|>%s|>%s`, from, t, f, l, k)

	logger.Debugf("flux: %s", flux)

	r, err := c.v2c.QueryCSV(ctx, flux, "")
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return nil, err
	}

	return parseCSVResult(r)
}

func parseCSVResult(r *v2client.QueryCSVResult) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	var dataType []string
	isComment := true
	var fields []string
	csvReader := csv.NewReader(r)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if strings.HasPrefix(row[0], "#") {
			isComment = true
			if row[0] == "#datatype" {
				dataType = row
				continue
			}
		} else {
			// Is first data
			if isComment {
				isComment = false
				fields = row
				continue
			}
			// data
			rowData := make(map[string]interface{})
			for i, field := range fields {
				switch dataType[i] {
				case "string":
					rowData[field] = row[i]
				case "dateTime:RFC3339":
					t, err := time.Parse(time.RFC3339, row[i])
					if err == nil {
						rowData[field] = t
					} else {
						rowData[field] = row[i]
					}
				case "long":
					v, err := strconv.ParseInt(row[i], 10, 64)
					if err == nil {
						rowData[field] = v
					} else {
						rowData[field] = row[i]
					}
				default:
					rowData[field] = row[i]
				}
			}
			result = append(result, rowData)
		}
	}

	return result, nil
}

// Close ...
func (c *Client) Close() {
	if c.v2c != nil {
		c.v2c.Close()
	}
}
