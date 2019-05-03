package pusher

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

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
	p, err := NewPusher("url", "db", o1, o2)
	assert.Nil(t, err)
	assert.Equal(t, "url/write", p.baseURL)
	assert.Equal(t, "db", p.db)
	assert.True(t, o1Invoked)
	assert.True(t, o2Invoked)
}

func TestNewPusherUrlCompletion(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inURL  string
		expURL string
	}{
		{"withoutSlash", "http://1.2.3.4:8086", "http://1.2.3.4:8086/write"},
		{"withSlash", "http://1.2.3.4:8086/", "http://1.2.3.4:8086/write"},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p, err := NewPusher(tc.inURL, "db")
			assert.Nil(t, err)
			assert.Equal(t, tc.expURL, p.baseURL)
		})
	}
}

func TestNewPusherNoOpt(t *testing.T) {
	p, err := NewPusher("url", "db")
	assert.Nil(t, err)
	assert.Equal(t, "url/write", p.baseURL)
	assert.Equal(t, "db", p.db)
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

func TestOptWithTimeout(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inT    time.Duration
		expErr bool
		expT   time.Duration
	}{
		{"5s", 5 * time.Second, false, 5 * time.Second},
		{"0s", 0 * time.Second, false, 0 * time.Second},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			p := Pusher{}
			err := OptWithTimeout(tc.inT)(&p)
			assert.Equal(t, tc.expErr, err != nil)
			if !tc.expErr {
				assert.Equal(t, tc.expT, p.timeout)
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

func TestIsNotFoundError(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inErr  error
		expRes bool
	}{
		{"ok", newError(errTypeNotFound, fmt.Errorf("e")), true},
		{"otherPushError", newError(errTypeServerProblem, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsNotFoundError(tc.inErr))
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
		{"otherPushError", newError(errTypeNotFound, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsServerProblemError(tc.inErr))
		})
	}
}

func TestIsPusherError(t *testing.T) {
	var tcs = []struct {
		tcID   string
		inErr  error
		expRes bool
	}{
		{"ok", newError(errTypePusher, fmt.Errorf("e")), true},
		{"otherPushError", newError(errTypeNotFound, fmt.Errorf("e")), false},
		{"otherError", fmt.Errorf("e"), false},
		{"nil", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			assert.Equal(t, tc.expRes, IsPusherError(tc.inErr))
		})
	}
}

func TestAddQueryParamIfNotEmpty(t *testing.T) {
	var tcs = []struct {
		tcID     string
		inVal    string
		expExist bool
	}{
		{"nonEmpty", "a", true},
		{"empty", "", false},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			qps := url.Values{}
			addQueryParamIfNotEmpty(&qps, "k", tc.inVal)
			assert.Equal(t, tc.inVal, qps.Get("k"))
		})
	}
}

func TestPushNominal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		assert.Equal(t, req.URL.Query().Get("db"), "d")
		assert.Equal(t, req.URL.Query().Get("consistency"), "all")
		assert.Equal(t, req.URL.Query().Get("precision"), "h")
		assert.Equal(t, req.URL.Query().Get("u"), "us")
		assert.Equal(t, req.URL.Query().Get("p"), "pa")
		assert.Equal(t, req.URL.Query().Get("rp"), "r")
		rw.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := NewPusher(srv.URL, "d",
		OptWithConsistency(ConsistencyAll),
		OptWithPrecision(PrecisionHour),
		OptWithUserPass("us", "pa"),
		OptWithRetentionPolicy("r"),
	)
	assert.Nil(t, err)

	err = p.Push("../testdata/sampleData.txt")
	assert.Nil(t, err)
}

func TestPushTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(100 * time.Millisecond)
		rw.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := NewPusher(srv.URL, "d",
		OptWithTimeout(50*time.Millisecond),
	)
	assert.Nil(t, err)

	err = p.Push("../testdata/sampleData.txt")
	assert.NotNil(t, err)
}

func TestPushNonExistingFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := NewPusher(srv.URL, "d")
	assert.Nil(t, err)

	err = p.Push("nonExistingFile.txt")
	assert.True(t, IsPusherError(err))
}

func TestPushUrlProblem(t *testing.T) {
	p, err := NewPusher("{", "d")
	assert.Nil(t, err)
	err = p.Push("../testdata/sampleData.txt")
	assert.True(t, IsBadRequestError(err))
}

func TestPushWrongStatus(t *testing.T) {
	var tcs = []struct {
		tcID               string
		inStatus           int
		expIsBadRequest    bool
		expIsServerProblem bool
		expIsNotFound      bool
		expIsUnauthorized  bool
		expIsPusher        bool
	}{
		{tcID: "badRequest", inStatus: http.StatusBadRequest, expIsBadRequest: true},
		{tcID: "serverProblem", inStatus: http.StatusInternalServerError, expIsServerProblem: true},
		{tcID: "notFound", inStatus: http.StatusNotFound, expIsNotFound: true},
		{tcID: "unauthorized", inStatus: http.StatusUnauthorized, expIsUnauthorized: true},
		{tcID: "pusher", inStatus: http.StatusConflict, expIsPusher: true},
	}
	for _, tc := range tcs {
		t.Run(tc.tcID, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(tc.inStatus)
			}))
			defer srv.Close()

			p, err := NewPusher(srv.URL, "d")
			assert.Nil(t, err)
			err = p.Push("../testdata/sampleData.txt")

			assert.Equal(t, tc.expIsBadRequest, IsBadRequestError(err))
			assert.Equal(t, tc.expIsServerProblem, IsServerProblemError(err))
			assert.Equal(t, tc.expIsNotFound, IsNotFoundError(err))
			assert.Equal(t, tc.expIsUnauthorized, IsUnauthorizedError(err))
			assert.Equal(t, tc.expIsPusher, IsPusherError(err))
		})
	}
}
