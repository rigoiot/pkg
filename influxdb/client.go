package influxdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rigoiot/atlas-app-toolkit/influxdb"
	"github.com/rigoiot/atlas-app-toolkit/query"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	api "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/rigoiot/pkg/logger"
)

type Point2 = write.Point

type WriteAPI = api.WriteAPI

// BatchPoints ...
type BatchPoints = client.BatchPoints

// Response ...
type Response = client.Response

// Client ...
type Client struct {
	client.Client
	V2c influxdb2.Client
	db  string
}

// NewClient ...
func NewClient(URL, db, username, password string, others ...interface{}) (*Client, error) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     URL,
		Username: username,
		Password: password,
		Timeout:  time.Duration(20) * time.Second,
	})
	if err != nil {
		logger.Errorf("Fail to create InfluxDB client on URL: %s error: %s", URL, err.Error())
		return nil, err
	}
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
	v2c := influxdb2.NewClientWithOptions(URL, fmt.Sprintf("%s:%s", username, password), options)

	return &Client{
		conn,
		v2c,
		db,
	}, nil
}

// NewPoint ...
func (c *Client) NewPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) *Point2 {
	return influxdb2.NewPoint(measurement, tags, fields, ts)
}

// NewWrite ...
func (c *Client) NewWrite(rp string, org ...string) WriteAPI {
	or := ""
	if len(org) > 0 {
		or = org[0]
	}
	return c.V2c.WriteAPI(or, fmt.Sprintf("%s/%s", c.db, rp))
}

// WritePoint ...
func (c *Client) WritePoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, rp ...string) error {
	pt, _ := client.NewPoint(measurement, tags, fields, ts)
	pts := []*client.Point{pt}
	return c.writePoints(pts, rp...)
}

// WritePoints ...
func (c *Client) writePoints(pts []*client.Point, rp ...string) error {
	RetentionPolicy := "autogen"
	if len(rp) > 0 {
		RetentionPolicy = rp[0]
	}

	bps, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        c.db,
		RetentionPolicy: RetentionPolicy,
	})

	bps.AddPoints(pts)

	logger.Debugf("Store points:%s to TSDB(%s)", bps, c.db)

	err := c.Client.Write(bps)
	return err
}

// QueryCommand
func (c *Client) QueryCommand(command string) (*client.Response, error) {
	query := client.Query{
		Command:  command,
		Database: c.db,
	}
	return c.Client.Query(query)
}

// Query ...
func (c *Client) Query(measurement string, filter *query.Filtering, orderBy *query.Sorting, fields *query.FieldSelection, paging *query.Pagination) (*Response, *Response, error) {
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
	cmdCount := fmt.Sprintf("SELECT COUNT(%s) FROM %s", f, measurement)

	// Process where
	if filter != nil {
		where, err := influxdb.FilteringToInflux(filter)
		if err != nil {
			logger.Errorf("Invaild filter, error: %s", err.Error())
			return nil, nil, err
		}
		if where != "" {
			cmd = fmt.Sprintf("%s WHERE %s", cmd, where)
			cmdCount = fmt.Sprintf("%s WHERE %s", cmdCount, where)
		}
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
		return nil, nil, err
	}

	if r != nil && r.Error() != nil {
		logger.Errorf("Fail to query(%s), response error: %v", cmd, r.Error())
		return nil, nil, r.Error()
	}

	// Query total count
	q = client.Query{
		Command:  cmdCount,
		Database: c.db,
	}
	tr, err := c.Client.Query(q)
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", cmdCount, err.Error())
		return nil, nil, err
	}

	if tr != nil && tr.Error() != nil {
		logger.Errorf("Fail to query(%s), response error: %v", cmdCount, r.Error())
		return nil, nil, tr.Error()
	}

	return r, tr, nil
}

// QueryCSV ...
func (c *Client) QueryCSV(ctx context.Context, flux string) (*api.QueryTableResult, error) {

	queryAPI := c.V2c.QueryAPI("")

	r, err := queryAPI.Query(ctx, flux)
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return r, err
	}

	return r, nil
}

// QueryV2 ...
func (c *Client) QueryV2(ctx context.Context, measurement, policy, start, stop string, fields []string, tags map[string]string, keep []string, limit, offset int) (*api.QueryTableResult, error) {
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

	// sort
	sort := `sort(columns:["_time"], desc:true)`
	flux := fmt.Sprintf(`%s|>%s|>%s|>%s|>%s|>%s`, from, t, f, sort, l, k)

	logger.Debugf("flux: %s", flux)

	r, err := c.QueryCSV(ctx, flux)
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return nil, err
	}

	return r, nil
}

// ParseCSVResult ...
func ParseCSVResult(result *api.QueryTableResult) ([]map[string]interface{}, error) {

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

	// var dataType []string
	// isComment := true
	// var fields []string
	// csvReader := csv.NewReader(r.Record())
	// for {
	// 	row, err := csvReader.Read()
	// 	if err == io.EOF {
	// 		break
	// 	} else if err != nil {
	// 		return nil, err
	// 	}
	// 	if strings.HasPrefix(row[0], "#") {
	// 		isComment = true
	// 		if row[0] == "#datatype" {
	// 			dataType = row
	// 			continue
	// 		}
	// 	} else {
	// 		// Is first data
	// 		if isComment {
	// 			isComment = false
	// 			fields = row
	// 			continue
	// 		}
	// 		// data
	// 		rowData := make(map[string]interface{})
	// 		for i, field := range fields {
	// 			switch dataType[i] {
	// 			case "string":
	// 				rowData[field] = row[i]
	// 			case "dateTime:RFC3339":
	// 				t, err := time.Parse(time.RFC3339, row[i])
	// 				if err == nil {
	// 					rowData[field] = t
	// 				} else {
	// 					rowData[field] = row[i]
	// 				}
	// 			case "long":
	// 				v, err := strconv.ParseInt(row[i], 10, 64)
	// 				if err == nil {
	// 					rowData[field] = v
	// 				} else {
	// 					rowData[field] = row[i]
	// 				}
	// 			default:
	// 				rowData[field] = row[i]
	// 			}
	// 		}
	// 		result = append(result, rowData)
	// 	}
	// }

	return data, nil
}

// Close ...
func (c *Client) Close() {
	if c.V2c != nil {
		c.V2c.Close()
	}
}
