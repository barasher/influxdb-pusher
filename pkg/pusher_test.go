package pusher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPusherNominal(t *testing.T) {
	o1Invoked := false
	o2Invoked := false
	o1 := func(*Pusher) error {
		o1Invoked = true
		return nil
	}
	o2 := func(*Pusher) error {
		o2Invoked = true
		return nil
	}
	_, err := NewPusher("url", "db", o1, o2)
	assert.Nil(t, err)
	assert.True(t, o1Invoked)
	assert.True(t, o2Invoked)
}

func TestNewPusherNoOpt(t *testing.T) {
	_, err := NewPusher("url", "db")
	assert.Nil(t, err)
}

func TestNewPusherErrorOnOpt(t *testing.T) {
	o := func(*Pusher) error {
		return fmt.Errorf("error")
	}
	_, err := NewPusher("url", "db", o)
	assert.NotNil(t, err)
}

func TestNewPusherErrorNoDatabase(t *testing.T) {
	o := func(*Pusher) error {
		return nil
	}
	_, err := NewPusher("url", "", o)
	assert.NotNil(t, err)
}
func TestNewPusherErrorNoURL(t *testing.T) {
	o := func(*Pusher) error {
		return nil
	}
	_, err := NewPusher("", "db", o)
	assert.NotNil(t, err)
}

func TestOptWithConsistency(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inC    Consistency
		expErr bool
		expC   string
	}{
		{"any", ConsistencyAny, false, "any"},
		{"one", ConsistencyOne, false, "one"},
		{"quorum", ConsistencyQuorum, false, "quorum"},
		{"all", ConsistencyAll, false, "all"},
		{"unknown", Consistency(42), true, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p := Pusher{}
			err := OptWithConsistency(tc.inC)(&p)
			assert.Equal(t, tc.expErr, err != nil)
			if !tc.expErr {
				assert.Equal(t, tc.expC, p.consistency)
			}
		})
	}
}

func TestOptWithUserPass(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inUser string
		inPass string
		expErr bool
	}{
		{"nominal", "a", "b", false},
		{"noUser", "a", "", false},
		{"noPass", "", "b", false},
		{"nothing", "", "", false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p := Pusher{}
			err := OptWithUserPass(tc.inUser, tc.inPass)(&p)
			assert.Equal(t, tc.expErr, err != nil)
			if !tc.expErr {
				assert.Equal(t, tc.inUser, p.username)
				assert.Equal(t, tc.inPass, p.password)
			}
		})
	}
}

func TestOptWithPrecision(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inP    Precision
		expErr bool
		expP   string
	}{
		{"nano", PrecisionNanosecond, false, "ns"},
		{"micro", PrecisionMicrosecond, false, "u"},
		{"milli", PrecisionMillisecond, false, "ms"},
		{"sec", PrecisionSecond, false, "s"},
		{"min", PrecisionMinute, false, "m"},
		{"hour", PrecisionHour, false, "h"},
		{"unknown", Precision(42), true, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p := Pusher{}
			err := OptWithPrecision(tc.inP)(&p)
			assert.Equal(t, tc.expErr, err != nil)
			if !tc.expErr {
				assert.Equal(t, tc.expP, p.precision)
			}
		})
	}
}

func TestOptWithRetentionPolicy(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inRP   string
		expErr bool
	}{
		{"nominal", "a", false},
		{"empty", "", false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p := Pusher{}
			err := OptWithRetentionPolicy(tc.inRP)(&p)
			assert.Equal(t, tc.expErr, err != nil)
			if !tc.expErr {
				assert.Equal(t, tc.inRP, p.retentionPolicy)
			}
		})
	}
}

func TestIsBadRequestError(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inErr  error
		expRes bool
	}{
		{"ok", newError(errTypeBadRequest, fmt.Errorf("e")), true},
		{"otherPushError", newError(errTypeServerProblem, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsBadRequestError(tc.inErr))
		})
	}
}

func TestIsUnauthorizedError(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inErr  error
		expRes bool
	}{
		{"ok", newError(errTypeUnauthorized, fmt.Errorf("e")), true},
		{"otherPushError", newError(errTypeServerProblem, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsUnauthorizedError(tc.inErr))
		})
	}
}

func TestIsNotExistError(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inErr  error
		expRes bool
	}{
		{"ok", newError(errTypeNotExist, fmt.Errorf("e")), true},
		{"otherPushError", newError(errTypeServerProblem, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsNotExistError(tc.inErr))
		})
	}
}

func TestIsServerProblemError(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inErr  error
		expRes bool
	}{
		{"ok", newError(errTypeServerProblem, fmt.Errorf("e")), true},
		{"otherPushError", newError(errTypeNotExist, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsServerProblemError(tc.inErr))
		})
	}
}
