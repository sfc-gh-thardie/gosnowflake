// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

type tcIntMinMax struct {
	v1  int
	v2  int
	out int
}

func TestSimpleTokenAccessor(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	token, masterToken, sessionId := accessor.GetTokens()
	if token != "" {
		t.Errorf("unexpected token %v", token)
	}
	if masterToken != "" {
		t.Errorf("unexpected master token %v", masterToken)
	}
	if sessionId != -1 {
		t.Errorf("unexpected session id %v", sessionId)
	}

	expectedToken, expectedMasterToken, expectedSessionId := "token123", "master123", 123
	accessor.SetTokens(expectedToken, expectedMasterToken, expectedSessionId)
	token, masterToken, sessionId = accessor.GetTokens()
	if token != expectedToken {
		t.Errorf("unexpected token %v", token)
	}
	if masterToken != expectedMasterToken {
		t.Errorf("unexpected master token %v", masterToken)
	}
	if sessionId != expectedSessionId {
		t.Errorf("unexpected session id %v", sessionId)
	}
}

func TestGetRequestIDFromContext(t *testing.T) {
	expectedRequestID := "snowflake-request-id"
	ctx := WithRequestID(context.Background(), expectedRequestID)
	requestID := getOrGenerateRequestIDFromContext(ctx)
	if requestID != expectedRequestID {
		t.Errorf("unexpected request id")
	}
}

func TestGenerateRequestID(t *testing.T) {
	firstRequestID := getOrGenerateRequestIDFromContext(context.Background())
	otherRequestID := getOrGenerateRequestIDFromContext(context.Background())
	if firstRequestID == otherRequestID {
		t.Errorf("request id should not be the same")
	}
}

func TestIntMin(t *testing.T) {
	testcases := []tcIntMinMax{
		{1, 3, 1},
		{5, 100, 5},
		{321, 3, 3},
		{123, 123, 123},
	}
	for _, test := range testcases {
		a := intMin(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed int min. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
	}
}
func TestIntMax(t *testing.T) {
	testcases := []tcIntMinMax{
		{1, 3, 3},
		{5, 100, 100},
		{321, 3, 321},
		{123, 123, 123},
	}
	for _, test := range testcases {
		a := intMax(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed int max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
	}
}

type tcDurationMinMax struct {
	v1  time.Duration
	v2  time.Duration
	out time.Duration
}

func TestDurationMin(t *testing.T) {
	testcases := []tcDurationMinMax{
		{1 * time.Second, 3 * time.Second, 1 * time.Second},
		{5 * time.Second, 100 * time.Second, 5 * time.Second},
		{321 * time.Second, 3 * time.Second, 3 * time.Second},
		{123 * time.Second, 123 * time.Second, 123 * time.Second},
	}
	for _, test := range testcases {
		a := durationMin(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed duratoin max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
	}
}

func TestDurationMax(t *testing.T) {
	testcases := []tcDurationMinMax{
		{1 * time.Second, 3 * time.Second, 3 * time.Second},
		{5 * time.Second, 100 * time.Second, 100 * time.Second},
		{321 * time.Second, 3 * time.Second, 321 * time.Second},
		{123 * time.Second, 123 * time.Second, 123 * time.Second},
	}
	for _, test := range testcases {
		a := durationMax(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed duratoin max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
	}
}

type tcNamedValues struct {
	values []driver.Value
	out    []driver.NamedValue
}

func compareNamedValues(v1 []driver.NamedValue, v2 []driver.NamedValue) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	if len(v1) != len(v2) {
		return false
	}
	for i := range v1 {
		if v1[i] != v2[i] {
			return false
		}
	}
	return true
}

func TestToNamedValues(t *testing.T) {
	testcases := []tcNamedValues{
		{
			values: []driver.Value{},
			out:    []driver.NamedValue{},
		},
		{
			values: []driver.Value{1},
			out:    []driver.NamedValue{{Name: "", Ordinal: 1, Value: 1}},
		},
		{
			values: []driver.Value{1, "test1", 9.876, nil},
			out: []driver.NamedValue{
				{Name: "", Ordinal: 1, Value: 1},
				{Name: "", Ordinal: 2, Value: "test1"},
				{Name: "", Ordinal: 3, Value: 9.876},
				{Name: "", Ordinal: 4, Value: nil}},
		},
	}
	for _, test := range testcases {
		a := toNamedValues(test.values)

		if !compareNamedValues(test.out, a) {
			t.Errorf("failed int max. v1: %v, v2: %v, expected: %v, got: %v", test.values, test.out, test.out, a)
		}
	}
}
