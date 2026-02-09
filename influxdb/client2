package influxdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	api "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/rigoiot/pkg/logger"
)

// Client2 ...
type Client2 struct {
	influxdb2.Client
	db string
}

type WriteAPI struct {
	api.WriteAPI
}

type WriteAPIBlocking struct {
	api.WriteAPIBlocking
}

type QueryAPI struct {
	api.QueryAPI
	db string
}

// NewClient ...
func NewClient2(URL, org, db, token string, others ...interface{}) (*Client2, error) {
	options := influxdb2.DefaultOptions()
	if len(others) > 0 {
		options.SetBatchSize(others[0].(uint))
	}
	if len(others) > 1 {
		options.SetFlushInterval(others[1].(uint))
	}
	if len(others) > 2 {
		options.SetUseGZip(others[2].(bool))
	}
	client := influxdb2.NewClientWithOptions(URL, token, options)

	return &Client2{
		client,
		db,
	}, nil
}

// NewWriteAPI
func (c *Client2) NewWriteAPI(org, db string) *WriteAPI {
	return &WriteAPI{
		c.Client.WriteAPI(org, db),
	}
}

// NewWriteAPI
func (c *Client2) NewWriteAPIBlocking(org, db string) *WriteAPIBlocking {
	return &WriteAPIBlocking{
		c.Client.WriteAPIBlocking(org, db),
	}
}

// NewQueryAPI
func (c *Client2) NewQueryAPI(org, db string) *QueryAPI {
	return &QueryAPI{
		c.Client.QueryAPI(org),
		db,
	}
}

// WritePoint ...
func (a *WriteAPI) WritePoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) {
	pt := influxdb2.NewPoint(measurement, tags, fields, ts)
	a.WriteAPI.WritePoint(pt)
}

// WritePoint ...
func (a *WriteAPIBlocking) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) {
	pt := influxdb2.NewPoint(measurement, tags, fields, ts)
	a.WriteAPIBlocking.WritePoint(ctx, pt)
}

// Query ...
func (c *QueryAPI) QueryFlux(ctx context.Context, flux string) (*api.QueryTableResult, error) {
	r, err := c.QueryAPI.Query(ctx, flux)
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return r, err
	}

	return r, nil
}

// QueryV2 ...
func (r *QueryAPI) Query(ctx context.Context, measurement, start, stop string, fields []string, tags map[string]string, keep []string, limit, offset int) ([]map[string]interface{}, error) {
	// check input
	if measurement == "" {
		return nil, fmt.Errorf("measurement should not be empty")
	}
	if start == "" {
		return nil, fmt.Errorf("start time should not be empty")
	}

	from := fmt.Sprintf(`from(bucket: "%s")`, r.db)

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

	// sort
	sort := `sort(columns:["_time"], desc:true)`
	flux := fmt.Sprintf(`%s|>%s|>%s|>%s|>%s|>%s`, from, t, f, sort, l, k)

	logger.Debugf("flux: %s", flux)

	d, err := r.QueryFlux(ctx, flux)
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return nil, err
	}

	return r.ParseResult(d)
}

// ParseResult ...
func (a *QueryAPI) ParseResult(result *api.QueryTableResult) ([]map[string]interface{}, error) {

	var data []map[string]interface{}

	for result.Next() {
		if result.TableChanged() {
			// fmt.Printf("table: %s\n", result.TableMetadata().String())
		}
		data = append(data, result.Record().Values())

	}
	if result.Err() != nil {
		fmt.Printf("Query error: %s\n", result.Err().Error())
	}

	return data, nil
}

// Close ...
func (c *Client2) Close() {
	if c.Client != nil {
		c.Client.Close()
	}
}
