// Package client provides a Go SDK for interacting with a Consus distributed KV store.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Config holds client connection parameters.
type Config struct {
	// Endpoints is a list of Consus server HTTP addresses (e.g., "http://localhost:8080").
	Endpoints []string

	// Timeout is the per-request timeout. Default: 5s.
	Timeout time.Duration
}

// Client is a Consus KV store client.
type Client struct {
	endpoints []string
	http      *http.Client
	current   int // index into endpoints for simple round-robin
}

// NewClient creates a new Consus client.
func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("consus: at least one endpoint is required")
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		endpoints: cfg.Endpoints,
		http:      &http.Client{Timeout: timeout},
	}, nil
}

// Close releases any resources held by the client.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// Put stores a key-value pair.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	u := fmt.Sprintf("%s/api/put?key=%s&value=%s",
		c.endpoint(), url.QueryEscape(key), url.QueryEscape(string(value)))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("consus put: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("consus put: %s", string(body))
	}
	return nil
}

// Get retrieves the value for a key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	u := fmt.Sprintf("%s/api/get?key=%s", c.endpoint(), url.QueryEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("consus get: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consus get: %s", string(body))
	}

	// Parse JSON response {"key":"...","value":"..."}
	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &result); err == nil {
		return []byte(result.Value), nil
	}

	return body, nil
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	u := fmt.Sprintf("%s/api/delete?key=%s", c.endpoint(), url.QueryEscape(key))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("consus delete: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("consus delete: %s", string(body))
	}
	return nil
}

// PutJSON marshals obj to JSON and stores it under key.
func (c *Client) PutJSON(ctx context.Context, key string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("consus: marshal: %w", err)
	}
	return c.Put(ctx, key, data)
}

// GetJSON retrieves a key and unmarshals the value into obj.
func (c *Client) GetJSON(ctx context.Context, key string, obj interface{}) error {
	data, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, obj)
}

// ClusterStatus returns the raw cluster state as a map.
func (c *Client) ClusterStatus(ctx context.Context) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/api/state", c.endpoint())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("consus status: %w", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// BatchPut stores multiple key-value pairs.
func (c *Client) BatchPut(ctx context.Context, kvPairs map[string][]byte) error {
	for k, v := range kvPairs {
		if err := c.Put(ctx, k, v); err != nil {
			return fmt.Errorf("batch put key %q: %w", k, err)
		}
	}
	return nil
}

// BatchGet retrieves multiple keys. Missing keys are omitted from the result.
func (c *Client) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	results := make(map[string][]byte, len(keys))
	for _, k := range keys {
		val, err := c.Get(ctx, k)
		if err != nil {
			continue // skip missing keys
		}
		results[k] = val
	}
	return results, nil
}

// endpoint returns the current server address (simple round-robin).
func (c *Client) endpoint() string {
	ep := c.endpoints[c.current%len(c.endpoints)]
	c.current++
	return ep
}

