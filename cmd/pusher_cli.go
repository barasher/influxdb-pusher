/*
Package main is a command line executable that push InfluxDB line protocol file to InfluxDB.
*/
package main

import (
	"flag"
	"os"
	"time"

	pusher "github.com/barasher/influxdb-pusher/pkg"
	"github.com/sirupsen/logrus"
)

const (
	retOk          int = 0
	retConfFailure int = 1
	retExecFailure int = 2
)

func main() {
	os.Exit(doMain(os.Args[1:]))
}

func doMain(args []string) int {
	cmd := flag.NewFlagSet("Pusher", flag.ContinueOnError)
	cons := cmd.String("c", "", "Consistency (any|all|one|quorum)")
	user := cmd.String("us", "", "Username")
	pass := cmd.String("p", "", "Password")
	prec := cmd.String("pr", "", "Precision (ns|u|ms|s|m|h)")
	retPol := cmd.String("r", "", "Retention policy")
	url := cmd.String("u", "", "URL, required (sample: http://1.2.3.4:8086)")
	db := cmd.String("d", "", "Database, required")
	data := cmd.String("f", "", "File to push, required")
	timeout := cmd.String("t", "", "Timeout duration (50s, 120ms, 1m, ...)")

	err := cmd.Parse(args)
	if err != nil {
		if err != flag.ErrHelp {
			logrus.Errorf("error while parsing command line arguments: %v", err)
		}
		return retConfFailure
	}

	if *url == "" {
		logrus.Errorf("No URL provided")
		return retConfFailure
	}
	if *db == "" {
		logrus.Errorf("No database provided")
		return retConfFailure
	}
	if *data == "" {
		logrus.Errorf("No data file provided")
		return retConfFailure
	}

	opts := []func(*pusher.Pusher) error{}
	opts = append(opts, pusher.OptWithUserPass(*user, *pass))
	if cons, found := getConsistency(*cons); found {
		opts = append(opts, pusher.OptWithConsistency(cons))
	}
	if prec, found := getPrecision(*prec); found {
		opts = append(opts, pusher.OptWithPrecision(prec))
	}
	if *retPol != "" {
		opts = append(opts, pusher.OptWithRetentionPolicy(*retPol))
	}
	if *timeout != "" {
		td, err := time.ParseDuration(*timeout)
		if err != nil {
			logrus.Errorf("error while parsing duration '%v': %v", *timeout, err)
			return retConfFailure
		}
		opts = append(opts, pusher.OptWithTimeout(td))
	}

	p, err := pusher.NewPusher(*url, *db, opts...)
	if err != nil {
		logrus.Errorf("Error when initializing pusher: %v", err)
		return retExecFailure
	}
	err = p.Push(*data)
	if err != nil {
		logrus.Errorf("Error when pushing data: %v", err)
		return retExecFailure
	}

	return retOk
}

func getPrecision(p string) (pusher.Precision, bool) {
	for curP, curS := range pusher.PrecisionToString {
		if curS == p {
			return curP, true
		}
	}
	return pusher.PrecisionSecond, false
}

func getConsistency(c string) (pusher.Consistency, bool) {
	for curC, curS := range pusher.ConsistencyToString {
		if curS == c {
			return curC, true
		}
	}
	return pusher.ConsistencyOne, false
}
