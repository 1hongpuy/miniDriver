package minidriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL          *url.URL
	clusterToken     string
	servicePrincipal string
	httpClient       *http.Client
	dataHTTPClient   *http.Client
	dataNodeTimeout  time.Duration
}

func NewClient(config Config) (*Client, error) {
	baseURL, err := url.Parse(config.GatewayURL)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("minidriver: invalid GatewayURL")
	}
	if config.ClusterInternalToken == "" || config.ServicePrincipal == "" {
		return nil, fmt.Errorf("minidriver: cluster token and service principal are required")
	}
	timeout := config.HTTPClientTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	dataTimeout := config.DataNodeTimeout
	if dataTimeout == 0 {
		dataTimeout = 60 * time.Second
	}
	return &Client{
		baseURL: baseURL, clusterToken: config.ClusterInternalToken,
		servicePrincipal: config.ServicePrincipal,
		httpClient:       &http.Client{Timeout: timeout},
		dataHTTPClient:   &http.Client{Timeout: dataTimeout}, dataNodeTimeout: dataTimeout,
	}, nil
}

func (c *Client) gatewayURL(path string) string {
	u := *c.baseURL
	pathOnly, rawQuery, hasQuery := strings.Cut(path, "?")
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + pathOnly
	if hasQuery {
		u.RawQuery = rawQuery
	} else {
		u.RawQuery = ""
	}
	return u.String()
}

func (c *Client) controlRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.gatewayURL(path), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Cluster-Internal-Token", c.clusterToken)
	req.Header.Set("X-Service-Principal", c.servicePrincipal)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.httpClient.Do(req)
}

func (c *Client) request(ctx context.Context, method, path string, body []byte,
	headers map[string]string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.gatewayURL(path), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	responseBody, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		return response, nil, readErr
	}
	return response, responseBody, nil
}

func responseError(operation string, response *http.Response, body []byte) error {
	return fmt.Errorf("minidriver: %s HTTP %d: %s", operation, response.StatusCode,
		strings.TrimSpace(string(body)))
}

func escapePath(value string) string { return url.PathEscape(value) }

func validateRef(ref ObjectRef) error {
	if ref.ObjectID == "" || ref.ObjectVersion == 0 {
		return fmt.Errorf("minidriver: objectId and objectVersion are required")
	}
	return nil
}

func decodeHints(raw json.RawMessage) (ObjectReadHints, error) {
	var wire struct {
		ObjectID      string `json:"objectId"`
		ObjectVersion uint64 `json:"objectVersion"`
		FileSize      uint64 `json:"fileSize"`
		Candidates    []struct {
			NodeID           string `json:"nodeId"`
			LocalBytes       uint64 `json:"localBytes"`
			CoveragePermille uint64 `json:"coveragePermille"`
			Health           string `json:"health"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return ObjectReadHints{}, err
	}
	if wire.ObjectID == "" || wire.ObjectVersion == 0 {
		return ObjectReadHints{}, fmt.Errorf("minidriver: invalid read-hints response")
	}
	hints := ObjectReadHints{ObjectRef: ObjectRef{ObjectID: wire.ObjectID, ObjectVersion: wire.ObjectVersion}, Size: wire.FileSize}
	for _, candidate := range wire.Candidates {
		if candidate.NodeID == "" {
			return ObjectReadHints{}, fmt.Errorf("minidriver: invalid read-hints candidate")
		}
		hints.Candidates = append(hints.Candidates, NodeReadHint{NodeID: candidate.NodeID, LocalBytes: candidate.LocalBytes, CoverageRatio: float64(candidate.CoveragePermille) / 1000, Health: candidate.Health})
	}
	return hints, nil
}
