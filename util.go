// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"github.com/google/uuid"
	"time"
)

type contextKey string

// SnowflakeRequestIDKey is optional context key to specify request id
const SnowflakeRequestIDKey contextKey = "SNOWFLAKE_REQUEST_ID"

// WithRequestID returns a new context with the specified snowflake request id
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, SnowflakeRequestIDKey, requestID)
}

// Get the request ID from the context if specified, otherwise generate one
func getOrGenerateRequestIDFromContext(ctx context.Context) string {
	requestID, ok := ctx.Value(SnowflakeRequestIDKey).(string)
	if ok && requestID != "" {
		return requestID
	}
	return uuid.New().String()
}

// integer min
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// integer max
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// time.Duration max
func durationMax(d1, d2 time.Duration) time.Duration {
	if d1-d2 > 0 {
		return d1
	}
	return d2
}

// time.Duration min
func durationMin(d1, d2 time.Duration) time.Duration {
	if d1-d2 < 0 {
		return d1
	}
	return d2
}

// toNamedValues converts a slice of driver.Value to a slice of driver.NamedValue for Go 1.8 SQL package
func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}

// TokenAccessor manages the session token and master token
type TokenAccessor interface {
	GetTokens() (token string, masterToken string, sessionId int)
	SetTokens(token string, masterToken string, sessionId int)
}

type simpleTokenAccessor struct {
	token string
	masterToken string
	sessionId int
}

func getSimpleTokenAccessor() TokenAccessor {
	return &simpleTokenAccessor{sessionId: -1}
}

func (sta *simpleTokenAccessor) GetTokens() (token string, masterToken string, sessionId int) {
	return sta.token, sta.masterToken, sta.sessionId
}

func (sta *simpleTokenAccessor) SetTokens(token string, masterToken string, sessionId int) {
	sta.token = token
	sta.masterToken = masterToken
	sta.sessionId = sessionId
}