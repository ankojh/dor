// Package embed is the local Text Embeddings Inference client (bge-small,
// 384 dimensions). The matcher uses it for the prompt prefilter; when it is
// unavailable the matcher falls back to lexical similarity and flags scores as
// approximate.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Dimensions is the bge-small vector size.
const Dimensions = 384

// Client calls a TEI /embed endpoint.
type Client struct {
	url    string
	http   *http.Client
	enable bool
}

// New builds a client. An empty URL disables embedding.
func New(baseURL string, timeout time.Duration) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	return &Client{
		url:    baseURL,
		http:   &http.Client{Timeout: timeout},
		enable: baseURL != "",
	}
}

// Enabled reports whether a TEI endpoint is configured.
func (c *Client) Enabled() bool { return c != nil && c.enable }

// Embed returns one vector per input. A disabled client returns an error so
// callers take the lexical path.
func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if !c.Enabled() {
		return nil, errors.New("embed: no TEI endpoint configured")
	}
	if len(texts) == 0 {
		return nil, nil
	}
	if len(texts) > 16 {
		texts = texts[:16]
	}
	payload, err := json.Marshal(map[string]any{"inputs": texts})
	if err != nil {
		return nil, err
	}
	endpoint := c.url
	if !strings.HasSuffix(endpoint, "/embed") {
		endpoint += "/embed"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("embed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body[:min(200, len(body))])))
	}
	var out [][]float32
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("embed: decode: %w", err)
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
