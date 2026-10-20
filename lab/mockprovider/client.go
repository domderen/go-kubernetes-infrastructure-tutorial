package mockprovider

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

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
)

// Client propagates each activity's context into its HTTP call. It never retries
// internally: activity retries reuse the same provider-owned operation identity.
type Client struct {
	baseURL string
	http    *http.Client
}

var _ lab.HostAllocator = (*Client)(nil)

func NewClient(baseURL string) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 15 * time.Second}}
}

func (c *Client) call(ctx context.Context, method, path string, request, result any) error {
	var body io.Reader
	if request != nil {
		data, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("encode provider request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("create provider request: %w", err)
	}
	if request != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("provider %s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil {
		return fmt.Errorf("read provider response: %w", err)
	}
	if len(data) > 4<<20 {
		return fmt.Errorf("provider response exceeds 4 MiB")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem wireError
		if err := json.Unmarshal(data, &problem); err != nil {
			return fmt.Errorf("provider HTTP %d: invalid error response", response.StatusCode)
		}
		for _, class := range errorClasses {
			if class.code == problem.Code {
				return fmt.Errorf("provider HTTP %d: %s: %w", response.StatusCode, problem.Message, class.err)
			}
		}
		return fmt.Errorf("provider HTTP %d (%s): %s", response.StatusCode, problem.Code, problem.Message)
	}
	if result != nil {
		if err := json.Unmarshal(data, result); err != nil {
			return fmt.Errorf("decode provider response: %w", err)
		}
	}
	return nil
}

func (c *Client) ListHosts(ctx context.Context, ownerUID string) ([]lab.HostAllocation, error) {
	var result []lab.HostAllocation
	err := c.call(ctx, http.MethodGet, "/v1/hosts?ownerUID="+url.QueryEscape(ownerUID), nil, &result)
	return result, err
}

func (c *Client) EnsureHost(ctx context.Context, ownerUID string, slot int) (lab.HostAllocation, error) {
	var result lab.HostAllocation
	err := c.call(ctx, http.MethodPost, "/v1/hosts/ensure", struct {
		OwnerUID string `json:"ownerUID"`
		Slot     int    `json:"slot"`
	}{ownerUID, slot}, &result)
	return result, err
}

func (c *Client) ReleaseHosts(ctx context.Context, ownerUID string) error {
	return c.call(ctx, http.MethodPost, "/v1/hosts/release", struct {
		OwnerUID string `json:"ownerUID"`
	}{ownerUID}, nil)
}

func (c *Client) EnsureImageJob(ctx context.Context, request lab.ImageRequest) (lab.ImageJob, error) {
	var result lab.ImageJob
	err := c.call(ctx, http.MethodPost, "/v1/images/ensure", request, &result)
	return result, err
}

func (c *Client) GetImageJob(ctx context.Context, ownerUID, jobID string) (lab.ImageJob, error) {
	var result lab.ImageJob
	err := c.call(ctx, http.MethodGet, "/v1/images/"+url.PathEscape(jobID)+"?ownerUID="+url.QueryEscape(ownerUID), nil, &result)
	return result, err
}

func (c *Client) JoinCluster(ctx context.Context, request lab.HostOperation) error {
	return c.call(ctx, http.MethodPost, "/v1/hosts/join", request, nil)
}

func (c *Client) ValidateGPU(ctx context.Context, request lab.HostOperation) error {
	return c.call(ctx, http.MethodPost, "/v1/hosts/validate", request, nil)
}
