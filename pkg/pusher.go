/*
Package pusher is library that push InfluxDB line protocol file to InfluxDB.
*/
package pusher

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// Consistency is a type referring to InfluxDb consistencies
type Consistency int

const (
	// ConsistencyAny is constant referring to InfluxDb "any" consistency
	ConsistencyAny Consistency = iota
	// ConsistencyOne is constant referring to InfluxDb "one"consistency
	ConsistencyOne
	// ConsistencyQuorum is constant referring to InfluxDb "quorum"consistency
	ConsistencyQuorum
	// ConsistencyAll is constant referring to InfluxDb "all"consistency
	ConsistencyAll
)

// ConsistencyToString maps available consistencies to their parameter value
var ConsistencyToString = map[Consistency]string{
	ConsistencyAny:    "any",
	ConsistencyAll:    "all",
	ConsistencyOne:    "one",
	ConsistencyQuorum: "quorum",
}

// Precision is a type referring to InfluxDb precisions
type Precision int

const (
	// PrecisionNanosecond is a constant referring to nanosecond precision
	PrecisionNanosecond Precision = iota
	// PrecisionMicrosecond is a constant referring to microsecond precision
	PrecisionMicrosecond
	// PrecisionMillisecond is a constant referring to millisecond precision
	PrecisionMillisecond
	// PrecisionSecond is a constant referring to second precision
	PrecisionSecond
	// PrecisionMinute is a constant referring to minute precision
	PrecisionMinute
	// PrecisionHour is a constant referring to hour precision
	PrecisionHour
)

// PrecisionToString maps available precisions to their parameter value
var PrecisionToString = map[Precision]string{
	PrecisionNanosecond:  "ns",
	PrecisionMicrosecond: "u",
	PrecisionMillisecond: "ms",
	PrecisionSecond:      "s",
	PrecisionMinute:      "m",
	PrecisionHour:        "h",
}

var errLogsForDetails = fmt.Errorf("See logs for more details")

// Pusher is a struct modeling the pusher
type Pusher struct {
	baseURL         string
	db              string
	username        string
	password        string
	consistency     string
	precision       string
	retentionPolicy string
}

// NewPusher instanciate a new pusher, pushing to db database and using
// opts configuration functions.
// An error will be returned if anything wrong happens, otherwise no error
// will be returned.
func NewPusher(baseURL string, db string, opts ...func(*Pusher) error) (*Pusher, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("no url provided")
	}
	if db == "" {
		return nil, fmt.Errorf("no database provided")
	}

	u := baseURL
	if !strings.HasSuffix(u, "/") {
		u += "/"
	}
	u += "write"

	p := Pusher{baseURL: u, db: db}
	for _, opt := range opts {
		if err := opt(&p); err != nil {
			return nil, fmt.Errorf("error when creating new pusher: %v", err)
		}
	}
	return &p, nil
}

// OptWithConsistency is an optional function to specify a consistency
// when pushing data
func OptWithConsistency(c Consistency) func(*Pusher) error {
	return func(p *Pusher) error {
		var v string
		var f bool
		if v, f = ConsistencyToString[c]; !f {
			return fmt.Errorf("Unknown consistency (%v)", c)
		}
		p.consistency = v
		return nil
	}
}

// OptWithUserPass is an optional function to specify a username and a
// password to interact with the database
func OptWithUserPass(user, pass string) func(*Pusher) error {
	return func(p *Pusher) error {
		p.username = user
		p.password = pass
		return nil
	}
}

// OptWithPrecision is an optional function to specify a precision for the
// pushed data
func OptWithPrecision(p Precision) func(*Pusher) error {
	return func(pu *Pusher) error {
		var v string
		var f bool
		if v, f = PrecisionToString[p]; !f {
			return fmt.Errorf("Unknown precision (%v)", p)
		}
		pu.precision = v
		return nil
	}
}

// OptWithRetentionPolicy is an optional function that specifies which
// retention policy to use when pushing data
func OptWithRetentionPolicy(rp string) func(*Pusher) error {
	return func(p *Pusher) error {
		p.retentionPolicy = rp
		return nil
	}
}

type errorType int

const (
	errTypeBadRequest errorType = iota
	errTypeUnauthorized
	errTypeNotFound
	errTypeServerProblem
	errTypePusher
)

var errorTypeToString = map[errorType]string{
	errTypeBadRequest:    "bad request",
	errTypeUnauthorized:  "unauthorized",
	errTypeNotFound:      "not found",
	errTypeServerProblem: "server problem",
	errTypePusher:        "pusher error",
}

type pushError struct {
	errType errorType
	err     error
}

func (e pushError) Error() string {
	return fmt.Sprintf("%v: %v", errorTypeToString[e.errType], e.err)
}

func isErrorType(err error, t errorType) bool {
	if e, ok := err.(pushError); ok {
		return e.errType == t
	}
	return false
}

// IsBadRequestError returns true if the error err is an InfluxDB bad request
// error
func IsBadRequestError(err error) bool {
	return isErrorType(err, errTypeBadRequest)
}

// IsUnauthorizedError returns true if the error err is an InfluxDB unauthorized
// error
func IsUnauthorizedError(err error) bool {
	return isErrorType(err, errTypeUnauthorized)
}

// IsNotFoundError returns true if the error err is an InfluxDB not exist
// error
func IsNotFoundError(err error) bool {
	return isErrorType(err, errTypeNotFound)
}

// IsServerProblemError returns true if the error err is an InfluxDB server problem
// error
func IsServerProblemError(err error) bool {
	return isErrorType(err, errTypeServerProblem)
}

// IsPusherError returns true if the error err is a pusher error
func IsPusherError(err error) bool {
	return isErrorType(err, errTypePusher)
}

func newError(t errorType, err error) error {
	return pushError{t, err}
}

func addQueryParamIfNotEmpty(qps *url.Values, k string, v string) {
	if v != "" {
		qps.Add(k, v)
	}
}

// Push pushes data to InfluxDB, an error will be returned if anything
// wrong happens.
func (p *Pusher) Push(f string) error {
	var err error
	u, err := url.Parse(p.baseURL)
	if err != nil {
		return newError(errTypeBadRequest, fmt.Errorf("error when parsing URL '%v': %v", p.baseURL, err))
	}
	q := u.Query()
	addQueryParamIfNotEmpty(&q, "db", p.db)
	addQueryParamIfNotEmpty(&q, "consistency", p.consistency)
	addQueryParamIfNotEmpty(&q, "u", p.username)
	addQueryParamIfNotEmpty(&q, "p", p.password)
	addQueryParamIfNotEmpty(&q, "precision", p.precision)
	addQueryParamIfNotEmpty(&q, "rp", p.retentionPolicy)
	u.RawQuery = q.Encode()
	uStr := u.String()
	logrus.Debugf("URL: %v", uStr)

	reader, err := os.Open(f)
	if err != nil {
		return newError(errTypePusher, fmt.Errorf("error when reading data file '%v': %v", f, err))
	}
	defer reader.Close()

	resp, err := http.Post(uStr, "text/plain", reader)
	if err != nil {
		return newError(errTypeBadRequest, fmt.Errorf("error when pushing data: %v", err))
	}
	defer resp.Body.Close()

	return dealWithResponse(resp)
}

func dealWithResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusNoContent {
		var err error
		switch resp.StatusCode {
		case http.StatusBadRequest:
			err = newError(errTypeBadRequest, errLogsForDetails)
		case http.StatusInternalServerError:
			err = newError(errTypeServerProblem, errLogsForDetails)
		case http.StatusNotFound:
			err = newError(errTypeNotFound, errLogsForDetails)
		case http.StatusUnauthorized:
			err = newError(errTypeUnauthorized, errLogsForDetails)
		default:
			err = newError(errTypePusher, fmt.Errorf("unexpected http status code (%v)", resp.StatusCode))
		}
		c, err2 := ioutil.ReadAll(resp.Body)
		if err2 != nil {
			return newError(errTypePusher, fmt.Errorf("error while consuming response: %v", err2))
		}
		logrus.Errorf("%v", string(c))
		return err
	}
	return nil
}
