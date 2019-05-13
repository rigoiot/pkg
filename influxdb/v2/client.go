package influxdb

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	client "github.com/rigoiot/influxdb-client-go"
	"github.com/rigoiot/pkg/logger"
)

// QueryCSVResult ...
type QueryCSVResult = client.QueryCSVResult

// Client ...
type Client struct {
	*client.Client
	db string
}

// NewClient ...
func NewClient(URL, db, username, password string) (*Client, error) {
	conn, err := client.New(nil, client.WithAddress(URL), client.WithUserAndPass(username, password))
	if err != nil {
		logger.Errorf("Fail to create InfluxDB client on URL: %s error: %s", URL, err.Error())
		return nil, err
	}

	return &Client{
		conn,
		db,
	}, nil
}

// QueryCSV ...
func (c *Client) QueryCSV(ctx context.Context, flux string) (*QueryCSVResult, error) {

	r, err := c.Client.QueryCSV(ctx, flux, "")
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return r, err
	}

	return r, nil
}

// Query ...
func (c *Client) Query(ctx context.Context, measurement, policy, start, stop string, fields []string, tags map[string]string, keep []string, limit, offset int) ([]map[string]interface{}, error) {
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

	r, err := c.Client.QueryCSV(ctx, flux, "")
	if err != nil {
		logger.Errorf("Fail to query(%s), error: %s", flux, err.Error())
		return nil, err
	}

	return parseCSVResult(r)
}

func parseCSVResult(r *QueryCSVResult) ([]map[string]interface{}, error) {
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
