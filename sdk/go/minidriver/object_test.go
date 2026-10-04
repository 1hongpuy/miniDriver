package minidriver

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func TestObjectReadAndControlAPI(t *testing.T) {
	data := []byte("MiniDriver Go SDK data")
	digest := fmt.Sprintf("%08x", crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v3/chunks/chunk-1" || r.Header.Get("X-Read-Token") != "capability" {
			http.Error(w, "unexpected DataNode request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer dataServer.Close()
	dataURL, err := url.Parse(dataServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, portString, err := net.SplitHostPort(dataURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Cluster-Internal-Token") != "secret" || r.Header.Get("X-Service-Principal") != "go-test" {
			http.Error(w, "auth", http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/internal/v3/objects/object-1/versions/1/read-plan":
			if r.Method != http.MethodPost {
				http.Error(w, "method", http.StatusMethodNotAllowed)
				return
			}
			fmt.Fprintf(w, `{"objectId":"object-1","objectVersion":1,"fileSize":%d,"chunkSize":%d,"chunks":[{"index":0,"chunkId":"chunk-1","storageIdentity":"chunk-1","size":%d,"checksumType":"crc32c","checksumDigest":"%s","readCapability":"capability","replicas":[{"nodeId":"node-1","address":"%s","port":%d}]}]}`,
				len(data), len(data), len(data), digest, host, port)
		case "/internal/v3/objects/object-1/versions/1/head":
			fmt.Fprint(w, `{"objectId":"object-1","objectVersion":1,"metadataVersion":2,"fileSize":23,"state":"COMMITTED"}`)
		case "/internal/v3/objects/object-1/versions/1/read-hints":
			fmt.Fprint(w, `{"objectId":"object-1","objectVersion":1,"fileSize":23,"candidates":[{"nodeId":"node-1","localBytes":23,"coveragePermille":1000,"health":"healthy"}]}`)
		case "/api/v2/objects/object-1/layout":
			if r.URL.Query().Get("version") != "1" {
				http.Error(w, "version", http.StatusBadRequest)
				return
			}
			fmt.Fprint(w, `{"objectId":"object-1","version":1,"size":23,"chunks":[{"index":0,"chunkId":"chunk-1","offset":0,"size":23,"checksumType":"crc32c","checksumDigest":"`+digest+`","replicas":["node-1"]}]}`)
		case "/internal/v3/objects/read-hints:batch":
			fmt.Fprint(w, `{"objects":[{"objectId":"object-1","objectVersion":1,"fileSize":23,"candidates":[{"nodeId":"node-1","localBytes":23,"coveragePermille":1000,"health":"healthy"}]}]}`)
		case "/internal/v3/objects/object-1/versions/1/delete":
			if r.Method != http.MethodDelete {
				http.Error(w, "method", http.StatusMethodNotAllowed)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "unexpected Gateway request", http.StatusNotFound)
		}
	}))
	defer gateway.Close()
	client, err := NewClient(Config{GatewayURL: gateway.URL, ClusterInternalToken: "secret", ServicePrincipal: "go-test"})
	if err != nil {
		t.Fatal(err)
	}
	ref := ObjectRef{ObjectID: "object-1", ObjectVersion: 1}
	var received []byte
	stats, err := client.GetObject(context.Background(), ref, ReadOptions{}, func(body []byte) error { received = append(received, body...); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if string(received) != string(data) || stats.BytesVerified != uint64(len(data)) {
		t.Fatalf("unexpected read result: %q / %+v", received, stats)
	}
	info, err := client.HeadObject(context.Background(), ref)
	if err != nil || info.State != "COMMITTED" {
		t.Fatalf("head: %+v, %v", info, err)
	}
	hints, err := client.GetObjectReadHints(context.Background(), ref)
	if err != nil || len(hints.Candidates) != 1 || hints.Candidates[0].CoverageRatio != 1 {
		t.Fatalf("hints: %+v, %v", hints, err)
	}
	layout, err := client.GetObjectLayout(context.Background(), ref)
	if err != nil || len(layout.Chunks) != 1 || layout.Chunks[0].Offset != 0 || layout.Chunks[0].Replicas[0] != "node-1" {
		t.Fatalf("layout: %+v, %v", layout, err)
	}
	batch, err := client.BatchGetObjectReadHints(context.Background(), []ObjectRef{ref})
	if err != nil || len(batch) != 1 {
		t.Fatalf("batch: %+v, %v", batch, err)
	}
	if err := client.DeleteObject(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestGetObjectRejectsBadChecksum(t *testing.T) {
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("wrong")) }))
	defer dataServer.Close()
	dataURL, _ := url.Parse(dataServer.URL)
	host, portString, _ := net.SplitHostPort(dataURL.Host)
	port, _ := strconv.Atoi(portString)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "read-plan") {
			fmt.Fprintf(w, `{"objectId":"object-1","objectVersion":1,"fileSize":5,"chunkSize":5,"chunks":[{"index":0,"storageIdentity":"chunk-1","size":5,"checksumType":"crc32c","checksumDigest":"00000000","readCapability":"capability","replicas":[{"nodeId":"node-1","address":"%s","port":%d}]}]}`, host, port)
			return
		}
		http.Error(w, "unexpected", http.StatusNotFound)
	}))
	defer gateway.Close()
	client, _ := NewClient(Config{GatewayURL: gateway.URL, ClusterInternalToken: "secret", ServicePrincipal: "go-test"})
	_, err := client.GetObject(context.Background(), ObjectRef{ObjectID: "object-1", ObjectVersion: 1}, ReadOptions{}, func([]byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "CRC32C") {
		t.Fatalf("expected checksum rejection, got %v", err)
	}
}
