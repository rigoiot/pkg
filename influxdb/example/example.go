package main

import (
	"context"
	"fmt"

	influxdb "github.com/rigoiot/pkg/influxdb"
)

func main() {
	influx, err := influxdb.NewClient("http://192.168.0.55:8087", "gocloud", "device", "oLnzgKo4DU", uint(1), uint(1000))
	if err != nil {
		panic(err) // error handling here, normally we wouldn't use fmt, but it works for the example
	}
	r, err := influx.QueryV2(context.Background(),
		"stat",
		"",
		"-7d",
		"",
		[]string{"avg"},
		map[string]string{
			"unit": "temperature",
		}, []string{"unit"}, 10, 0)
	if err != nil {
		panic(err)
	}

	result, err := influxdb.ParseCSVResult(r)
	fmt.Println("output:\n\n", result)

	influx.Close() // close the client after this the client is useless.
}
