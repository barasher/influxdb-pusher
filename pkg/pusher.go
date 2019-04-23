package pusher

import (
	"fmt"
	"net/url"

	"github.com/sirupsen/logrus"
)

// Consistency is a type refering to InfluxDb consistencies
type Consistency int

const (
	// ConsistencyAny is constant refering to InfluxDb "any" consistency
	ConsistencyAny Consistency = iota
	// ConsistencyOne is constant refering to InfluxDb "one"consistency
	ConsistencyOne
	// ConsistencyQuorum is constant refering to InfluxDb "quorum"consistency
	ConsistencyQuorum
	// ConsistencyAll is constant refering to InfluxDb "all"consistency
	ConsistencyAll
)

var consistencyToString = map[Consistency]string{
	ConsistencyAny:    "any",
	ConsistencyAll:    "all",
	ConsistencyOne:    "one",
	ConsistencyQuorum: "quorum",
}

// Precision is a type refering to InfluxDb precisions
type Precision int

const (
	// PrecisionNanosecond is a constant refering to nanosecond precision
	PrecisionNanosecond Precision = iota
	// PrecisionMicrosecond is a constant refering to microsecond precision
	PrecisionMicrosecond
	// PrecisionMillisecond is a constant refering to millisecond precision
	PrecisionMillisecond
	// PrecisionSecond is a constant refering to second precision
	PrecisionSecond
	// PrecisionMinute is a constant refering to minute precision
	PrecisionMinute
	// PrecisionHour is a constant refering to hour precision
	PrecisionHour
)

var precisionToString = map[Precision]string{
	PrecisionNanosecond:  "ns",
	PrecisionMicrosecond: "u",
	PrecisionMillisecond: "ms",
	PrecisionSecond:      "s",
	PrecisionMinute:      "m",
	PrecisionHour:        "h",
}

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
	p := Pusher{}
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
		if v, f = consistencyToString[c]; !f {
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
		if v, f = precisionToString[p]; !f {
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
	errTypeNotExist
	errTypeServerProblem
)

var errorTypeToString = map[errorType]string{
	errTypeBadRequest:    "bad request",
	errTypeUnauthorized:  "unauthorized",
	errTypeNotExist:      "not exist",
	errTypeServerProblem: "server problem",
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

// IsNotExistError returns true if the error err is an InfluxDB not exist
// error
func IsNotExistError(err error) bool {
	return isErrorType(err, errTypeNotExist)
}

// IsServerProblemError returns true if the error err is an InfluxDB server problem
// error
func IsServerProblemError(err error) bool {
	return isErrorType(err, errTypeServerProblem)
}

func newError(t errorType, err error) error {
	return pushError{t, err}
}

func addQueryParamIfNotEmpty(u *url.URL, k string, v string) {
	if v != "" {
		u.Query().Add(k, v)
	}
}

// Push pushes data to InfluxDB
func (p *Pusher) Push(f string) error {
	u, err := url.Parse(p.baseURL)
	if err != nil {
		return newError(errTypeBadRequest, fmt.Errorf("error when parsing URL '%v': %v", p.baseURL, err))
	}
	addQueryParamIfNotEmpty(u, "db", p.db)
	addQueryParamIfNotEmpty(u, "consistency", p.consistency)
	addQueryParamIfNotEmpty(u, "u", p.username)
	addQueryParamIfNotEmpty(u, "p", p.password)
	addQueryParamIfNotEmpty(u, "precision", p.precision)
	addQueryParamIfNotEmpty(u, "rp", p.retentionPolicy)
	uStr := u.String()
	logrus.Debugf("URL: %v", uStr)

	//resp, err := http.Post(uStr)

	return nil
}
