package minidriver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

func (c *Client) GetReadPlan(ctx context.Context, ref ObjectRef) (ObjectReadPlan, error) {
	if err := validateRef(ref); err != nil { return ObjectReadPlan{}, err }
	path := "/internal/v3/objects/" + escapePath(ref.ObjectID) + "/versions/" + strconv.FormatUint(ref.ObjectVersion, 10) + "/read-plan"
	response, err := c.controlRequest(ctx, http.MethodPost, path, nil)
	if err != nil { return ObjectReadPlan{}, err }
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body); if err != nil { return ObjectReadPlan{}, err }
	if response.StatusCode != http.StatusOK { return ObjectReadPlan{}, responseError("read-plan", response, body) }
	var plan ObjectReadPlan
	if err := json.Unmarshal(body, &plan); err != nil { return ObjectReadPlan{}, err }
	if plan.ObjectID != ref.ObjectID || plan.ObjectVersion != ref.ObjectVersion || plan.FileSize == 0 || plan.ChunkSize == 0 || len(plan.Chunks) == 0 {
		return ObjectReadPlan{}, fmt.Errorf("minidriver: invalid read-plan response")
	}
	for expected, chunk := range plan.Chunks {
		if chunk.Index != uint32(expected) || chunk.StorageIdentity == "" || chunk.Size == 0 || chunk.ReadCapability == "" || len(chunk.Replicas) == 0 {
			return ObjectReadPlan{}, fmt.Errorf("minidriver: invalid chunk read plan")
		}
	}
	return plan, nil
}

func (c *Client) HeadObject(ctx context.Context, ref ObjectRef) (ObjectInfo, error) {
	if err := validateRef(ref); err != nil { return ObjectInfo{}, err }
	path := "/internal/v3/objects/" + escapePath(ref.ObjectID) + "/versions/" + strconv.FormatUint(ref.ObjectVersion, 10) + "/head"
	response, err := c.controlRequest(ctx, http.MethodGet, path, nil); if err != nil { return ObjectInfo{}, err }
	defer response.Body.Close(); body, err := io.ReadAll(response.Body); if err != nil { return ObjectInfo{}, err }
	if response.StatusCode != http.StatusOK { return ObjectInfo{}, responseError("object head", response, body) }
	var info ObjectInfo
	if err := json.Unmarshal(body, &info); err != nil { return ObjectInfo{}, err }
	if info.ObjectID != ref.ObjectID || info.ObjectVersion != ref.ObjectVersion { return ObjectInfo{}, fmt.Errorf("minidriver: invalid object head response") }
	return info, nil
}

func (c *Client) DeleteObject(ctx context.Context, ref ObjectRef) error {
	if err := validateRef(ref); err != nil { return err }
	path := "/internal/v3/objects/" + escapePath(ref.ObjectID) + "/versions/" + strconv.FormatUint(ref.ObjectVersion, 10) + "/delete"
	response, err := c.controlRequest(ctx, http.MethodDelete, path, nil); if err != nil { return err }
	defer response.Body.Close(); body, err := io.ReadAll(response.Body); if err != nil { return err }
	if response.StatusCode != http.StatusAccepted { return responseError("object delete", response, body) }
	return nil
}

func (c *Client) GetObjectReadHints(ctx context.Context, ref ObjectRef) (ObjectReadHints, error) {
	if err := validateRef(ref); err != nil { return ObjectReadHints{}, err }
	path := "/internal/v3/objects/" + escapePath(ref.ObjectID) + "/versions/" + strconv.FormatUint(ref.ObjectVersion, 10) + "/read-hints"
	response, err := c.controlRequest(ctx, http.MethodPost, path, nil); if err != nil { return ObjectReadHints{}, err }
	defer response.Body.Close(); body, err := io.ReadAll(response.Body); if err != nil { return ObjectReadHints{}, err }
	if response.StatusCode != http.StatusOK { return ObjectReadHints{}, responseError("object read hints", response, body) }
	hints, err := decodeHints(body); if err != nil { return ObjectReadHints{}, err }
	if hints.ObjectRef != ref { return ObjectReadHints{}, fmt.Errorf("minidriver: mismatched read-hints response") }
	return hints, nil
}

func (c *Client) BatchGetObjectReadHints(ctx context.Context, refs []ObjectRef) ([]ObjectReadHints, error) {
	if len(refs) == 0 { return nil, fmt.Errorf("minidriver: at least one object is required") }
	for _, ref := range refs { if err := validateRef(ref); err != nil { return nil, err } }
	body, err := json.Marshal(struct { Objects []ObjectRef `json:"objects"` }{refs}); if err != nil { return nil, err }
	response, err := c.controlRequest(ctx, http.MethodPost, "/internal/v3/objects/read-hints:batch", bytes.NewReader(body)); if err != nil { return nil, err }
	defer response.Body.Close(); responseBody, err := io.ReadAll(response.Body); if err != nil { return nil, err }
	if response.StatusCode != http.StatusOK { return nil, responseError("batch object read hints", response, responseBody) }
	var wire struct { Objects []json.RawMessage `json:"objects"` }
	if err := json.Unmarshal(responseBody, &wire); err != nil { return nil, err }
	if len(wire.Objects) != len(refs) { return nil, fmt.Errorf("minidriver: incomplete batch read-hints response") }
	out := make([]ObjectReadHints, len(refs))
	for index, raw := range wire.Objects {
		hints, err := decodeHints(raw); if err != nil { return nil, err }
		if hints.ObjectRef != refs[index] { return nil, fmt.Errorf("minidriver: mismatched batch read-hints response") }
		out[index] = hints
	}
	return out, nil
}

func verifyChunk(chunk ChunkReadPlan, body []byte) error {
	if uint64(len(body)) != chunk.Size { return fmt.Errorf("minidriver: chunk length mismatch") }
	switch chunk.ChecksumType {
	case "crc32c":
		digest := fmt.Sprintf("%08x", crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
		if digest != chunk.ChecksumDigest { return fmt.Errorf("minidriver: chunk CRC32C mismatch") }
	case "sha256":
		digest := sha256.Sum256(body)
		if hex.EncodeToString(digest[:]) != chunk.ChecksumDigest { return fmt.Errorf("minidriver: chunk SHA-256 mismatch") }
	default: return fmt.Errorf("minidriver: unsupported checksum type %q", chunk.ChecksumType)
	}
	return nil
}

func (c *Client) readChunk(ctx context.Context, chunk ChunkReadPlan, offset, length uint64, options ReadOptions, stats *TransferStats) ([]byte, IntegrityStatus, error) {
	if offset+length > chunk.Size { return nil, UnverifiedPartialRange, fmt.Errorf("minidriver: chunk range exceeds chunk") }
	whole := offset == 0 && length == chunk.Size
	attempts := 1
	if options.AllowReplicaRetry { attempts = options.MaxReplicaAttempts; if attempts < 1 { attempts = 2 } }
	if attempts > len(chunk.Replicas) { attempts = len(chunk.Replicas) }
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		replica := chunk.Replicas[attempt]
		endpoint := "http://" + replica.Address + ":" + strconv.Itoa(int(replica.HTTPPort)) + "/internal/v3/chunks/" + url.PathEscape(chunk.StorageIdentity)
		requestContext, cancel := context.WithTimeout(ctx, c.dataNodeTimeout)
		req, err := http.NewRequestWithContext(requestContext, http.MethodGet, endpoint, nil)
		if err != nil { cancel(); return nil, UnverifiedPartialRange, err }
		req.Header.Set("X-Read-Token", chunk.ReadCapability)
		if !whole { req.Header.Set("Range", "bytes="+strconv.FormatUint(offset, 10)+"-"+strconv.FormatUint(offset+length-1, 10)) }
		response, err := c.dataHTTPClient.Do(req)
		if err != nil { cancel(); lastErr = err; continue }
		body, readErr := io.ReadAll(response.Body); response.Body.Close(); cancel()
		stats.DataRequests++
		expected := http.StatusPartialContent; if whole { expected = http.StatusOK }
		if readErr != nil { lastErr = readErr; continue }
		if response.StatusCode != expected { lastErr = responseError("DataNode read", response, body); continue }
		if uint64(len(body)) != length { lastErr = fmt.Errorf("minidriver: DataNode range length mismatch"); continue }
		if whole && !options.DisableChecksum {
			if err := verifyChunk(chunk, body); err != nil { lastErr = err; continue }
			stats.BytesVerified += uint64(len(body)); if attempt > 0 { stats.ReplicaFallbacks++ }; return body, VerifiedWholeChunk, nil
		}
		if attempt > 0 { stats.ReplicaFallbacks++ }
		if whole { return body, UnverifiedChecksumOff, nil }
		return body, UnverifiedPartialRange, nil
	}
	return nil, UnverifiedPartialRange, fmt.Errorf("minidriver: cannot read chunk %d: %w", chunk.Index, lastErr)
}

func normalizedReadOptions(options ReadOptions) ReadOptions {
	if options.MaxReplicaAttempts == 0 { options.MaxReplicaAttempts = 2 }
	return options
}

func (c *Client) GetObject(ctx context.Context, ref ObjectRef, options ReadOptions, sink func([]byte) error) (TransferStats, error) {
	if sink == nil { return TransferStats{}, fmt.Errorf("minidriver: object sink is required") }
	options = normalizedReadOptions(options)
	plan, err := c.GetReadPlan(ctx, ref); if err != nil { return TransferStats{}, err }
	var stats TransferStats; var delivered uint64
	for _, chunk := range plan.Chunks {
		body, _, err := c.readChunk(ctx, chunk, 0, chunk.Size, options, &stats); if err != nil { return stats, err }
		if err := sink(body); err != nil { return stats, err }; delivered += uint64(len(body))
	}
	if delivered != plan.FileSize { return stats, fmt.Errorf("minidriver: object length mismatch") }
	return stats, nil
}

func (c *Client) GetRange(ctx context.Context, ref ObjectRef, offset, length uint64, options ReadOptions, sink func([]byte) error) (RangeReadResult, TransferStats, error) {
	if sink == nil || length == 0 { return RangeReadResult{}, TransferStats{}, fmt.Errorf("minidriver: sink and positive range length are required") }
	options = normalizedReadOptions(options)
	plan, err := c.GetReadPlan(ctx, ref); if err != nil { return RangeReadResult{}, TransferStats{}, err }
	if offset >= plan.FileSize || length > plan.FileSize-offset { return RangeReadResult{}, TransferStats{}, fmt.Errorf("minidriver: range exceeds object") }
	var result RangeReadResult; var stats TransferStats; start := uint64(0); allWhole := true; end := offset+length
	for _, chunk := range plan.Chunks {
		stop := start + chunk.Size; begin := maxUint64(start, offset); finish := minUint64(stop, end)
		if begin < finish {
			body, integrity, err := c.readChunk(ctx, chunk, begin-start, finish-begin, options, &stats); if err != nil { return result, stats, err }
			if err := sink(body); err != nil { return result, stats, err }; result.BytesRead += uint64(len(body)); allWhole = allWhole && integrity == VerifiedWholeChunk
		}
		start = stop
	}
	if result.BytesRead != length { return result, stats, fmt.Errorf("minidriver: range length mismatch") }
	result.Integrity = UnverifiedPartialRange; if allWhole { result.Integrity = VerifiedWholeChunk }
	return result, stats, nil
}

func minUint64(a, b uint64) uint64 { if a < b { return a }; return b }
func maxUint64(a, b uint64) uint64 { if a > b { return a }; return b }
