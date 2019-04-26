# InfluxDB-pusher

[![Build Status](https://travis-ci.org/barasher/influxdb-pusher.svg?branch=master)](https://travis-ci.org/barasher/influxdb-pusher)
[![go report card](https://goreportcard.com/badge/github.com/barasher/influxdb-pusher "go report card")](https://goreportcard.com/report/github.com/barasher/influxdb-pusher)

**influxdb-pusher** is tool that push [InfluxDB line protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/) to [InfluxDB](https://docs.influxdata.com/influxdb/v1.7/).

It is released as :
- a Golang library
- an executable binary

## Golang library

### Install

```
go get github.com/barasher/influxdb-pusher
```

### Documentation and example

``` go
package main

import (
	pusher "github.com/barasher/influxdb-pusher"
)

func main() {
	p, err := pusher.NewPusher("http://127.0.0.1:8086", "myDatabase")
	if err != nil {
		// deal with error
	}
	err = p.Push("/tmp/someData.txt")
	if err != nil {
        // deal with error
	}
}
```
## Executable binary

### Compilation

```
go build -o pusher cmd/pusher_cli.go
```

### Execution

```
barasher@Linux:/tmp/$ ./pusher -h
Usage of Pusher:
  -c string
    	Consistency (any|all|one|quorum)
  -d string
    	Database, required
  -f string
    	File to push, required
  -p string
    	Password
  -pr string
    	Precision (ns|u|ms|s|m|h)
  -r string
    	Retention policy
  -u string
    	URL, required (sample: http://1.2.3.4:8086)
  -us string
    	Username
```

Parameters :
- **-c** specifies the consistency required for the push
- **-d** specifies the database that has to be used
- **-f** specifies the path containing the data
- **-p** specifies the password to use
- **-pr** specifies the precision ot consider for the data
- **-u** specifies the URL of the InfluxDB API
- **-us** specifies the username to use

Return codes :
- **0**: everything was OK
- **1**: configuration failure
- **2**: execution failure