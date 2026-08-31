package minidriver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type uploadSession struct {
	SessionID     string   `json:"sessionId"`
	ChunkSize     uint64   `json:"chunkSize"`
	TotalChunks   uint32   `json:"totalChunks"`
	ChecksumType  string   `json:"checksumType"`
	Completed     []uint32 `json:"completed"`
}

type uploadRoute struct {
	PrimaryNodeID  string `json:"primaryNodeId"`
	PrimaryAddress string `json:"primaryAddress"`
	PrimaryPort    uint16 `json:"primaryPort"`
	StorageIdentity string `json:"storageIdentity"`
	UploadToken    string `json:"uploadToken"`
	Chain []struct { NodeID string `json:"nodeId"`; Address string `json:"address"`; HTTPPort uint16 `json:"httpPort"` } `json:"chain"`
}

func normalizedPutOptions(options PutOptions) (PutOptions, error) {
	if options.ChecksumType == "" { options.ChecksumType = "crc32c" }
	if options.ChecksumType != "crc32c" && options.ChecksumType != "sha256" { return options, fmt.Errorf("minidriver: checksum type must be crc32c or sha256") }
	if options.ChunkWindow == 0 { options.ChunkWindow = 2 }
	return options, nil
}

// PutFile uploads a replayable local file. A non-seekable io.Reader is not
// accepted because a route/body retry must replay exactly the same Chunk.
func (c *Client) PutFile(ctx context.Context, path string, options PutOptions) (ObjectRef, error) {
	file, err := os.Open(path); if err != nil { return ObjectRef{}, err }
	defer file.Close()
	stat, err := file.Stat(); if err != nil { return ObjectRef{}, err }
	return c.PutObject(ctx, file, stat.Size(), options)
}

// PutObject accepts a replayable source. source must also be safe for
// concurrent ReadAt calls because chunks are uploaded with a bounded window.
func (c *Client) PutObject(ctx context.Context, source io.ReaderAt, size int64, options PutOptions) (ObjectRef, error) {
	if source == nil || size <= 0 { return ObjectRef{}, fmt.Errorf("minidriver: replayable non-empty source is required") }
	options, err := normalizedPutOptions(options); if err != nil { return ObjectRef{}, err }
	name, err := opaqueObjectName(); if err != nil { return ObjectRef{}, err }
	createdBody, err := json.Marshal(struct { FileName string `json:"fileName"`; DirPath string `json:"dirPath"`; FileSize int64 `json:"fileSize"` }{name, "/", size})
	if err != nil { return ObjectRef{}, err }
	created, body, err := c.request(ctx, http.MethodPost, "/api/v2/upload/sessions", createdBody, map[string]string{"Content-Type":"application/json"})
	if err != nil { return ObjectRef{}, err }; if created.StatusCode < 200 || created.StatusCode >= 300 { return ObjectRef{}, responseError("create upload session", created, body) }
	var session uploadSession; if err := json.Unmarshal(body, &session); err != nil { return ObjectRef{}, err }
	if session.SessionID == "" { return ObjectRef{}, fmt.Errorf("minidriver: Gateway returned no sessionId") }
	sessionResponse, sessionBody, err := c.request(ctx, http.MethodGet, "/api/v2/upload/sessions/"+escapePath(session.SessionID), nil, nil)
	if err != nil { return ObjectRef{}, err }; if sessionResponse.StatusCode != http.StatusOK { return ObjectRef{}, responseError("get upload session", sessionResponse, sessionBody) }
	if err := json.Unmarshal(sessionBody, &session); err != nil { return ObjectRef{}, err }
	if session.ChunkSize == 0 || session.TotalChunks == 0 || session.ChecksumType != options.ChecksumType { return ObjectRef{}, fmt.Errorf("minidriver: Gateway upload checksum/chunking policy mismatch") }
	completed := make(map[uint32]bool, len(session.Completed)); for _, index := range session.Completed { completed[index] = true }
	pending := make(chan uint32); var wg sync.WaitGroup; var firstErr error; var errorMu sync.Mutex
	workerCount := options.ChunkWindow; if workerCount > int(session.TotalChunks) { workerCount = int(session.TotalChunks) }
	for worker := 0; worker < workerCount; worker++ { wg.Add(1); go func() { defer wg.Done(); for index := range pending {
		errorMu.Lock(); stopped := firstErr != nil; errorMu.Unlock(); if stopped { continue }
		if err := c.putChunk(ctx, source, size, session, index); err != nil { errorMu.Lock(); if firstErr == nil { firstErr = fmt.Errorf("minidriver: chunk %d: %w", index, err) }; errorMu.Unlock() }
	} }() }
	for index := uint32(0); index < session.TotalChunks; index++ { if !completed[index] { pending <- index } }
	close(pending); wg.Wait(); if firstErr != nil { return ObjectRef{}, firstErr }
	committed, commitBody, err := c.request(ctx, http.MethodPost, "/api/v2/upload/sessions/"+escapePath(session.SessionID)+"/commit", []byte("{}"), map[string]string{"Content-Type":"application/json"})
	if err != nil { return ObjectRef{}, err }; if committed.StatusCode < 200 || committed.StatusCode >= 300 { return ObjectRef{}, responseError("commit upload session", committed, commitBody) }
	var ref ObjectRef; if err := json.Unmarshal(commitBody, &ref); err != nil { return ObjectRef{}, err }
	if err := validateRef(ref); err != nil { return ObjectRef{}, fmt.Errorf("minidriver: incomplete commit identity") }
	return ref, nil
}

func (c *Client) putChunk(ctx context.Context, source io.ReaderAt, fileSize int64, session uploadSession, index uint32) error {
	offset := int64(index) * int64(session.ChunkSize); size := int64(session.ChunkSize); if remaining := fileSize-offset; remaining < size { size = remaining }
	data := make([]byte, size); if _, err := source.ReadAt(data, offset); err != nil && err != io.EOF { return err }
	checksum := checksumDigest(session.ChecksumType, data)
	routeKey := "upload:"+session.SessionID+":"+strconv.FormatUint(uint64(index), 10); if session.ChecksumType == "sha256" { routeKey = checksum }
	routeBody, _ := json.Marshal(struct { Chunks []struct { Index uint32 `json:"index"`; Hash string `json:"hash"`; Size int64 `json:"size"`; ChecksumType string `json:"checksumType"`; ChecksumDigest string `json:"checksumDigest"` } `json:"chunks"` }{Chunks: []struct { Index uint32 `json:"index"`; Hash string `json:"hash"`; Size int64 `json:"size"`; ChecksumType string `json:"checksumType"`; ChecksumDigest string `json:"checksumDigest"` }{{index, routeKey, size, session.ChecksumType, checksum}}})
	var route uploadRoute
	var routeErr error
	for attempt := 0; attempt < 6; attempt++ {
		response, body, err := c.request(ctx, http.MethodPost, "/api/v2/upload/sessions/"+escapePath(session.SessionID)+"/routes", routeBody, map[string]string{"Content-Type":"application/json"})
		if err == nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			var wire struct { Routes []uploadRoute `json:"routes"` }; err = json.Unmarshal(body, &wire); if err == nil && len(wire.Routes) == 1 { route = wire.Routes[0]; break }; routeErr = fmt.Errorf("minidriver: invalid route response")
		} else if err == nil && response.StatusCode != http.StatusServiceUnavailable { return responseError("route chunk", response, body) } else if err != nil { routeErr = err } else { routeErr = fmt.Errorf("minidriver: route admission stayed full") }
		select { case <-ctx.Done(): return ctx.Err(); case <-time.After(time.Duration(attempt+1)*25*time.Millisecond): }
	}
	if route.PrimaryAddress == "" || route.PrimaryPort == 0 || route.PrimaryNodeID == "" || route.UploadToken == "" || len(route.Chain) == 0 { return routeErr }
	chain := ""; for position, node := range route.Chain { if position > 0 { chain += ";" }; chain += node.NodeID+"@"+node.Address+":"+strconv.Itoa(int(node.HTTPPort)) }
	identity := route.StorageIdentity; if identity == "" { identity = routeKey }
	endpoint := "http://"+route.PrimaryAddress+":"+strconv.Itoa(int(route.PrimaryPort))+"/v2/chunks/"+escapePath(identity)
	requestContext, cancel := context.WithTimeout(ctx, c.dataNodeTimeout); defer cancel()
	req, err := http.NewRequestWithContext(requestContext, http.MethodPut, endpoint, bytes.NewReader(data)); if err != nil { return err }
	req.ContentLength = size; req.Header.Set("Content-Type", "application/octet-stream"); req.Header.Set("X-Session-Id", session.SessionID); req.Header.Set("X-Chunk-Index", strconv.FormatUint(uint64(index), 10)); req.Header.Set("X-Commit-Owner", route.PrimaryNodeID); req.Header.Set("X-Gateway-Address", c.baseURL.Hostname()); gatewayPort := c.baseURL.Port(); if gatewayPort == "" { gatewayPort = "80" }; req.Header.Set("X-Gateway-Port", gatewayPort); req.Header.Set("X-Replica-Chain", chain); req.Header.Set("X-Replica-Position", "0"); req.Header.Set("X-Upload-Token", route.UploadToken)
	response, err := c.dataHTTPClient.Do(req); if err != nil { return err }; defer response.Body.Close(); responseBody, err := io.ReadAll(response.Body); if err != nil { return err }
	if response.StatusCode != http.StatusOK { return responseError("DataNode PUT", response, responseBody) }
	return nil
}

func checksumDigest(kind string, data []byte) string { if kind == "sha256" { digest := sha256.Sum256(data); return hex.EncodeToString(digest[:]) }; return fmt.Sprintf("%08x", crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))) }
func opaqueObjectName() (string, error) { bytes := make([]byte, 16); if _, err := rand.Read(bytes); err != nil { return "", err }; return "_object_"+hex.EncodeToString(bytes), nil }
