package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	pusher "github.com/barasher/influxdb-pusher/pkg"

	"github.com/stretchr/testify/assert"
)

func TestDoMainNominal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ret := doMain([]string{"-u", srv.URL, "-d", "db", "-f", "../testdata/sampleData.txt"})
	assert.Equal(t, retOk, ret)
}

func TestDoMainExecutionFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ret := doMain([]string{"-u", srv.URL, "-d", "db", "-f", "../testdata/sampleData.txt"})
	assert.Equal(t, retExecFailure, ret)
}

func TestDoMainFailure(t *testing.T) {
	var tcs = []struct {
		tcID    string
		params  []string
		expCode int
	}{
		{"help", []string{"-h"}, retConfFailure},
		{"noUrl", []string{"-d", "a", "-f", "a"}, retConfFailure},
		{"noDatabase", []string{"-u", "a", "-f", "a"}, retConfFailure},
		{"noFile", []string{"-u", "a", "-d", "a"}, retConfFailure},
		{"parseError", []string{"-turlututu"}, retConfFailure},
		{"unparsableTimeout", []string{"-u", "url", "-d", "db", "-f", "a", "-t", "bla"}, retConfFailure},
	}

	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			ret := doMain(tc.params)
			assert.Equal(t, tc.expCode, ret)
		})
	}
}

func TestGetPrecision(t *testing.T) {
	var tcs = []struct {
		tcID string
		inP  string
		expF bool
		expP pusher.Precision
	}{
		{"ns", "ns", true, pusher.PrecisionNanosecond},
		{"u", "u", true, pusher.PrecisionMicrosecond},
		{"ms", "ms", true, pusher.PrecisionMillisecond},
		{"s", "s", true, pusher.PrecisionSecond},
		{"m", "m", true, pusher.PrecisionMinute},
		{"h", "h", true, pusher.PrecisionHour},
		{"unknown", "unknown", false, pusher.PrecisionHour},
		{"empty", "", false, pusher.PrecisionHour},
	}

	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p, f := getPrecision(tc.inP)
			assert.Equal(t, tc.expF, f)
			if tc.expF {
				assert.Equal(t, tc.expP, p)
			}
		})
	}
}

func TestGetConsistency(t *testing.T) {
	var tcs = []struct {
		tcID string
		inC  string
		expF bool
		expC pusher.Consistency
	}{
		{"any", "any", true, pusher.ConsistencyAny},
		{"all", "all", true, pusher.ConsistencyAll},
		{"one", "one", true, pusher.ConsistencyOne},
		{"quorum", "quorum", true, pusher.ConsistencyQuorum},
		{"unknown", "unknown", false, pusher.ConsistencyAny},
		{"empty", "", false, pusher.ConsistencyAny},
	}

	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			c, f := getConsistency(tc.inC)
			assert.Equal(t, tc.expF, f)
			if tc.expF {
				assert.Equal(t, tc.expC, c)
			}
		})
	}
}
