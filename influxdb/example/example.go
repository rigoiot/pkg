package main

import (
	"context"
	"fmt"

	influxdb "github.com/rigoiot/pkg/influxdb"
)

func main() {
	influx, err := influxdb.NewClient("http://host1.xmdonsonic.com:8086", "iotc", "iotc", "0TmnLxTJ9B")
	if err != nil {
		panic(err) // error handling here, normally we wouldn't use fmt, but it works for the example
	}
	r, err := influx.QueryV2(context.Background(),
		"properties",
		"",
		"-7d",
		"",
		[]string{"value"},
		map[string]string{
			"identifier": "SOC",
		}, []string{"identifier"}, 10, 0)
	if err != nil {
		panic(err)
	}

	fmt.Println("output:\n\n", r)

	influx.Close() // close the client after this the client is useless.
}
