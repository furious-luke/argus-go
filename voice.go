package argus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// VoiceConfig is the optional, server-set text-to-speech voice configuration for
// a stream. It is validated against a catalog version and resolved into an
// immutable per-provider plan when the stream is created; it never travels
// through the browser. Discover the available voices and parameters with
// GetVoices and send the version it returns as CatalogVersion.
type VoiceConfig struct {
	// CatalogVersion is required whenever any selection field is set. Send the
	// version returned by GetVoices; a stale version is rejected so a stream is
	// never created from stale discovery data.
	CatalogVersion string `json:"catalogVersion,omitempty"`
	// Name selects a neutral catalog voice that maps to a concrete vendor voice
	// for every provider in the chain, so it survives provider fallback.
	Name string `json:"name,omitempty"`
	// ByProvider attaches a vendor-specific voice override to individual
	// providers. It applies only when that provider synthesizes.
	ByProvider map[string]VoiceProviderRef `json:"byProvider,omitempty"`
	// Params tunes synthesis: normalized Common parameters mapped onto every
	// provider, plus per-provider vendor-specific bags. The two namespaces are
	// disjoint.
	Params *VoiceParams `json:"params,omitempty"`
}

// VoiceProviderRef is a vendor-specific voice override for one provider.
type VoiceProviderRef struct {
	VoiceID string `json:"voiceId"`
}

// VoiceParams carries voice parameters in the two disjoint namespaces.
type VoiceParams struct {
	Common     map[string]any            `json:"common,omitempty"`
	ByProvider map[string]map[string]any `json:"byProvider,omitempty"`
}

// VoiceCatalog is the account-scoped, versioned voice catalog served by
// GET /api/voices: the normative schema for both discovery and stream creation.
type VoiceCatalog struct {
	Version      string               `json:"version"`
	Voices       []CatalogVoice       `json:"voices"`
	Providers    []CatalogProvider    `json:"providers"`
	CommonParams []CatalogCommonParam `json:"commonParams"`
}

// CatalogVoice is a neutral voice: Providers lists the providers it maps to.
type CatalogVoice struct {
	Name              string   `json:"name"`
	Label             string   `json:"label"`
	Status            string   `json:"status"`
	NewStreamDeadline string   `json:"newStreamDeadline,omitempty"`
	Languages         []string `json:"languages,omitempty"`
	Tags              []string `json:"tags,omitempty"`
	Providers         []string `json:"providers"`
}

// CatalogProvider is one provider's selectable voices and vendor-specific params.
type CatalogProvider struct {
	Name              string                 `json:"name"`
	Status            string                 `json:"status"`
	NewStreamDeadline string                 `json:"newStreamDeadline,omitempty"`
	Voices            []CatalogProviderVoice `json:"voices"`
	Params            []CatalogParam         `json:"params,omitempty"`
}

// CatalogProviderVoice is one vendor-specific, selectable voice.
type CatalogProviderVoice struct {
	ID                string   `json:"id"`
	Label             string   `json:"label"`
	Status            string   `json:"status"`
	NewStreamDeadline string   `json:"newStreamDeadline,omitempty"`
	Languages         []string `json:"languages,omitempty"`
}

// CatalogParam is the type-and-constraint schema of one tunable parameter.
type CatalogParam struct {
	Name      string   `json:"name"`
	Type      string   `json:"type"`
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	Default   any      `json:"default,omitempty"`
	Unit      string   `json:"unit,omitempty"`
	MaxLength *int     `json:"maxLength,omitempty"`
	Enum      []string `json:"enum,omitempty"`
}

// CatalogCommonParam is a normalized parameter plus its provider mapping coverage.
type CatalogCommonParam struct {
	CatalogParam
	Providers []string `json:"providers"`
}

// GetVoices fetches the current voice catalog for the calling account. Use its
// Version as the CatalogVersion of a VoiceConfig you send to JoinStreamWithOptions.
func (c *Client) GetVoices(ctx context.Context) (*VoiceCatalog, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/voices", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(message))
	}
	var catalog VoiceCatalog
	if err := json.NewDecoder(resp.Body).Decode(&catalog); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &catalog, nil
}
