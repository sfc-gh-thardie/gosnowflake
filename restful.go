// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// HTTP headers
const (
	headerSnowflakeToken   = "Snowflake Token=\"%v\""
	headerAuthorizationKey = "Authorization"

	headerContentTypeApplicationJSON     = "application/json"
	headerAcceptTypeApplicationSnowflake = "application/snowflake"
)

// Snowflake Server Error code
const (
	sessionExpiredCode       = "390112"
	queryInProgressCode      = "333333"
	queryInProgressAsyncCode = "333334"
)

// Snowflake Server Endpoints
const (
	loginRequestPath         = "/session/v1/login-request"
	queryRequestPath         = "/queries/v1/query-request"
	tokenRequestPath         = "/session/token-request"
	abortRequestPath         = "/queries/v1/abort-request"
	authenticatorRequestPath = "/session/authenticator-request"
	sessionRequestPath       = "/session"
	heartBeatPath            = "/session/heartbeat"
)

type snowflakeRestful struct {
	Host           string
	Port           int
	Protocol       string
	LoginTimeout   time.Duration // Login timeout
	RequestTimeout time.Duration // request timeout

	Client      *http.Client
	Token       string
	MasterToken string
	SessionID   int
	HeartBeat   *heartbeat

	Connection          *snowflakeConn
	FuncPostQuery       func(context.Context, *snowflakeRestful, *url.Values, map[string]string, []byte, time.Duration, string) (*execResponse, error)
	FuncPostQueryHelper func(context.Context, *snowflakeRestful, *url.Values, map[string]string, []byte, time.Duration, string) (*execResponse, error)
	FuncPost            func(context.Context, *snowflakeRestful, *url.URL, map[string]string, []byte, time.Duration, bool) (*http.Response, error)
	FuncGet             func(context.Context, *snowflakeRestful, *url.URL, map[string]string, time.Duration) (*http.Response, error)
	FuncRenewSession    func(context.Context, *snowflakeRestful, time.Duration) error
	FuncPostAuth        func(context.Context, *snowflakeRestful, *url.Values, map[string]string, []byte, time.Duration) (*authResponse, error)
	FuncCloseSession    func(context.Context, *snowflakeRestful, time.Duration) error
	FuncCancelQuery     func(context.Context, *snowflakeRestful, string, time.Duration) error

	FuncPostAuthSAML func(context.Context, *snowflakeRestful, map[string]string, []byte, time.Duration) (*authResponse, error)
	FuncPostAuthOKTA func(context.Context, *snowflakeRestful, map[string]string, []byte, string, time.Duration) (*authOKTAResponse, error)
	FuncGetSSO       func(context.Context, *snowflakeRestful, *url.Values, map[string]string, string, time.Duration) ([]byte, error)
}

func (sr *snowflakeRestful) getURL() *url.URL {
	return &url.URL{
		Scheme: sr.Protocol,
		Host:   sr.Host + ":" + strconv.Itoa(sr.Port),
	}
}

func (sr *snowflakeRestful) getFullURL(path string, params *url.Values) *url.URL {
	ret := &url.URL{
		Scheme: sr.Protocol,
		Host:   sr.Host + ":" + strconv.Itoa(sr.Port),
		Path:   path,
	}
	if params != nil {
		ret.RawQuery = params.Encode()
	}
	return ret
}

func (sr *snowflakeRestful) getQueryIDURL(queryID string) *url.URL {
	ret := &url.URL{
		Scheme: sr.Protocol,
		Host:   sr.Host + ":" + strconv.Itoa(sr.Port),
		Path:   "/queries/" + queryID + "/result",
	}
	return ret
}

type renewSessionResponse struct {
	Data    renewSessionResponseMain `json:"data"`
	Message string                   `json:"message"`
	Code    string                   `json:"code"`
	Success bool                     `json:"success"`
}

type renewSessionResponseMain struct {
	SessionToken        string        `json:"sessionToken"`
	ValidityInSecondsST time.Duration `json:"validityInSecondsST"`
	MasterToken         string        `json:"masterToken"`
	ValidityInSecondsMT time.Duration `json:"validityInSecondsMT"`
	SessionID           int           `json:"sessionId"`
}

type cancelQueryResponse struct {
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
	Code    string      `json:"code"`
	Success bool        `json:"success"`
}

func postRestful(
	ctx context.Context,
	sr *snowflakeRestful,
	fullURL *url.URL,
	headers map[string]string,
	body []byte,
	timeout time.Duration,
	raise4XX bool) (
	*http.Response, error) {
	return newRetryHTTP(
		ctx, sr.Client, http.NewRequest, fullURL, headers, timeout).doPost().setBody(body).doRaise4XX(raise4XX).execute()
}

func getRestful(
	ctx context.Context,
	sr *snowflakeRestful,
	fullURL *url.URL,
	headers map[string]string,
	timeout time.Duration) (
	*http.Response, error) {
	return newRetryHTTP(
		ctx, sr.Client, http.NewRequest, fullURL, headers, timeout).execute()
}

func postRestfulQuery(
	ctx context.Context,
	sr *snowflakeRestful,
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration,
	requestID string) (
	data *execResponse, err error) {

	data, err = sr.FuncPostQueryHelper(ctx, sr, params, headers, body, timeout, requestID)

	// errors other than context timeout and cancel would be returned to upper layers
	if err != context.Canceled && err != context.DeadlineExceeded {
		return data, err
	}

	err = sr.FuncCancelQuery(context.TODO(), sr, requestID, timeout)
	if err != nil {
		return nil, err
	}
	return nil, ctx.Err()
}

func postRestfulQueryHelper(
	ctx context.Context,
	sr *snowflakeRestful,
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration,
	requestID string) (
	data *execResponse, err error) {
	logger.Infof("params: %v", params)
	params.Add(requestIDKey, requestID)
	params.Add("clientStartTime", strconv.FormatInt(time.Now().Unix(), 10))
	params.Add(requestGUIDKey, uuid.New().String())
	if sr.Token != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)
	}

	var resp *http.Response
	var fullURL *url.URL
	if queryID := getResumeQueryID(ctx); queryID == "" {
		fullURL = sr.getFullURL(queryRequestPath, params)
		resp, err = sr.FuncPost(ctx, sr, fullURL, headers, body, timeout, false)
	} else {
		respd := execResponse{}
		rows := new(snowflakeRows)
		rows.status = QueryStatusInProgress
		rows.statusChannel = make(chan queryStatus)
		respd.Data.AsyncRows = rows
		respd.Success = true // needed to return exec properly

		// kick off retrieval and return incomplete object for user
		go getAsync(ctx, sr, headers, sr.getQueryIDURL(queryID), timeout, rows)
		return &respd, nil
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		logger.WithContext(ctx).Infof("postQuery: resp: %v", resp)
		var respd execResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
			return nil, err
		}
		if respd.Code == sessionExpiredCode {
			err = sr.FuncRenewSession(ctx, sr, timeout)
			if err != nil {
				return nil, err
			}
			return sr.FuncPostQuery(ctx, sr, params, headers, body, timeout, requestID)
		}

		var resultURL string
		isSessionRenewed := false

		if queryIDChan := getQueryIDChan(ctx); queryIDChan != nil {
			queryIDChan <- respd.Data.QueryID
			close(queryIDChan)
			ctx = WithQueryIDChan(ctx, nil)
		}

		// if asynchronous query in progress, kick off retrieval but return object
		if respd.Code == queryInProgressAsyncCode {
			// placeholder object to return to user while retrieving results
			// TODO snowflake results
			rows := new(snowflakeRows)
			rows.status = QueryStatusInProgress
			rows.statusChannel = make(chan queryStatus)
			respd.Data.AsyncRows = rows

			// spawn goroutine to retrieve asynchronous results
			go getAsync(ctx, sr, headers, sr.getFullURL(respd.Data.GetResultURL, nil), timeout, rows)
			return &respd, nil
		}
		for isSessionRenewed || respd.Code == queryInProgressCode ||
			respd.Code == queryInProgressAsyncCode {
			if !isSessionRenewed {
				resultURL = respd.Data.GetResultURL
			}

			logger.Info("ping pong")
			headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)
			fullURL := sr.getFullURL(resultURL, nil)

			resp, err = sr.FuncGet(ctx, sr, fullURL, headers, timeout)
			if err != nil {
				logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
				return nil, err
			}
			respd = execResponse{} // reset the response
			err = json.NewDecoder(resp.Body).Decode(&respd)
			resp.Body.Close()
			if err != nil {
				logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
				return nil, err
			}
			if respd.Code == sessionExpiredCode {
				err = sr.FuncRenewSession(ctx, sr, timeout)
				if err != nil {
					return nil, err
				}
				isSessionRenewed = true
			} else {
				isSessionRenewed = false
			}
		}
		return &respd, nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to extract HTTP response body. err: %v", err)
		return nil, err
	}
	logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	logger.WithContext(ctx).Infof("Header: %v", resp.Header)
	return nil, &SnowflakeError{
		Number:      ErrFailedToPostQuery,
		SQLState:    SQLStateConnectionFailure,
		Message:     errMsgFailedToPostQuery,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func closeSession(ctx context.Context, sr *snowflakeRestful, timeout time.Duration) error {
	logger.WithContext(ctx).Info("close session")
	params := &url.Values{}
	params.Add("delete", "true")
	params.Add(requestIDKey, getOrGenerateRequestIDFromContext(ctx))
	params.Add(requestGUIDKey, uuid.New().String())
	fullURL := sr.getFullURL(sessionRequestPath, params)

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)

	resp, err := sr.FuncPost(ctx, sr, fullURL, headers, nil, 5*time.Second, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var respd renewSessionResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
			return err
		}
		if !respd.Success && respd.Code != sessionExpiredCode {
			c, err := strconv.Atoi(respd.Code)
			if err != nil {
				return err
			}
			return &SnowflakeError{
				Number:  c,
				Message: respd.Message,
			}
		}
		return nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to extract HTTP response body. err: %v", err)
		return err
	}
	logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	logger.WithContext(ctx).Infof("Header: %v", resp.Header)
	return &SnowflakeError{
		Number:      ErrFailedToCloseSession,
		SQLState:    SQLStateConnectionFailure,
		Message:     errMsgFailedToCloseSession,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func renewRestfulSession(ctx context.Context, sr *snowflakeRestful, timeout time.Duration) error {
	logger.WithContext(ctx).Info("start renew session")
	params := &url.Values{}
	params.Add(requestIDKey, getOrGenerateRequestIDFromContext(ctx))
	params.Add(requestGUIDKey, uuid.New().String())
	fullURL := sr.getFullURL(tokenRequestPath, params)

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.MasterToken)

	body := make(map[string]string)
	body["oldSessionToken"] = sr.Token
	body["requestType"] = "RENEW"

	var reqBody []byte
	reqBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := sr.FuncPost(ctx, sr, fullURL, headers, reqBody, timeout, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var respd renewSessionResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
			return err
		}
		if !respd.Success {
			c, err := strconv.Atoi(respd.Code)
			if err != nil {
				return err
			}
			return &SnowflakeError{
				Number:  c,
				Message: respd.Message,
			}
		}
		sr.Token = respd.Data.SessionToken
		sr.MasterToken = respd.Data.MasterToken
		return nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to extract HTTP response body. err: %v", err)
		return err
	}
	logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	logger.WithContext(ctx).Infof("Header: %v", resp.Header)
	return &SnowflakeError{
		Number:      ErrFailedToRenewSession,
		SQLState:    SQLStateConnectionFailure,
		Message:     errMsgFailedToRenew,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func cancelQuery(ctx context.Context, sr *snowflakeRestful, requestID string, timeout time.Duration) error {
	logger.WithContext(ctx).Info("cancel query")
	params := &url.Values{}
	params.Add(requestIDKey, getOrGenerateRequestIDFromContext(ctx))
	params.Add(requestGUIDKey, uuid.New().String())

	fullURL := sr.getFullURL(abortRequestPath, params)

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)

	req := make(map[string]string)
	req[requestIDKey] = requestID

	reqByte, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := sr.FuncPost(ctx, sr, fullURL, headers, reqByte, timeout, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var respd cancelQueryResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
			return err
		}
		if !respd.Success && respd.Code == sessionExpiredCode {
			err := sr.FuncRenewSession(ctx, sr, timeout)
			if err != nil {
				return err
			}
			return sr.FuncCancelQuery(ctx, sr, requestID, timeout)
		} else if respd.Success {
			return nil
		} else {
			c, err := strconv.Atoi(respd.Code)
			if err != nil {
				return err
			}
			return &SnowflakeError{
				Number:  c,
				Message: respd.Message,
			}
		}
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to extract HTTP response body. err: %v", err)
		return err
	}
	logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	logger.WithContext(ctx).Infof("Header: %v", resp.Header)
	return &SnowflakeError{
		Number:      ErrFailedToCancelQuery,
		SQLState:    SQLStateConnectionFailure,
		Message:     errMsgFailedToCancelQuery,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func getAsync(
	ctx context.Context,
	sr *snowflakeRestful,
	headers map[string]string,
	URL *url.URL,
	timeout time.Duration,
	rows *snowflakeRows,
) {
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)
	resp, err := sr.FuncGet(ctx, sr, URL, headers, timeout)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
		return
	}

	respd := execResponse{} // reset the response
	err = json.NewDecoder(resp.Body).Decode(&respd)
	resp.Body.Close()
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return
	}

	if respd.Success {
		sc := &snowflakeConn{rest: sr}
		rows.sc = sc
		rows.RowType = respd.Data.RowType
		rows.ChunkDownloader = &snowflakeChunkDownloader{
			sc:                 sc,
			ctx:                ctx,
			CurrentChunk:       make([]chunkRowType, len(respd.Data.RowSet)),
			ChunkMetas:         respd.Data.Chunks,
			Total:              respd.Data.Total,
			TotalRowIndex:      int64(-1),
			CellCount:          len(respd.Data.RowType),
			Qrmk:               respd.Data.Qrmk,
			QueryResultFormat:  respd.Data.QueryResultFormat,
			ChunkHeader:        respd.Data.ChunkHeaders,
			FuncDownload:       downloadChunk,
			FuncDownloadHelper: downloadChunkHelper,
			FuncGet:            getChunk,
			RowSet: rowSetType{RowType: respd.Data.RowType,
				JSON:         respd.Data.RowSet,
				RowSetBase64: respd.Data.RowSetBase64,
			},
		}
		rows.queryID = respd.Data.QueryID
		rows.ChunkDownloader.start()
		rows.statusChannel <- QueryStatusComplete // mark query status complete
	} else {
		rows.statusChannel <- QueryFailed
		return
	}
}
