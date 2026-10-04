// Package minidriver provides the Go object-storage client for MiniDriver v3.
package minidriver

import "time"

// ObjectRef is the only persistent identity for an immutable object version.
type ObjectRef struct {
	ObjectID      string `json:"objectId"`
	ObjectVersion uint64 `json:"objectVersion"`
}

type Config struct {
	GatewayURL           string
	ClusterInternalToken string
	ServicePrincipal     string
	HTTPClientTimeout    time.Duration
	DataNodeTimeout      time.Duration
}

type PutOptions struct {
	// ChecksumType must match the Gateway session policy: crc32c (v3 default)
	// or sha256 (legacy/CAS compatibility).
	ChecksumType string
	// ChunkWindow is per object. It is not a process-wide concurrency limit.
	ChunkWindow int
}

type ReadOptions struct {
	// DisableChecksum is opt-out. The zero value verifies every whole Chunk.
	DisableChecksum    bool
	AllowReplicaRetry  bool
	MaxReplicaAttempts int
}

type ObjectInfo struct {
	ObjectRef
	MetadataVersion uint64 `json:"metadataVersion"`
	Size            uint64 `json:"fileSize"`
	Name            string `json:"name"`
	ParentPath      string `json:"parentPath"`
	State           string `json:"state"`
}

type NodeReadHint struct {
	NodeID        string  `json:"nodeId"`
	LocalBytes    uint64  `json:"localBytes"`
	CoverageRatio float64 `json:"-"`
	Health        string  `json:"health"`
}

type ObjectReadHints struct {
	ObjectRef
	Size       uint64         `json:"fileSize"`
	Candidates []NodeReadHint `json:"candidates"`
}

// ObjectLayout is static placement metadata. It contains no short-lived
// DataNode read capability and is safe for a scheduler to persist as a hint.
type ObjectLayout struct {
	ObjectRef
	Version uint64              `json:"version"`
	Size    uint64              `json:"size"`
	Chunks  []ObjectLayoutChunk `json:"chunks"`
}

type ObjectLayoutChunk struct {
	Index          uint32   `json:"index"`
	ChunkID        string   `json:"chunkId"`
	Offset         uint64   `json:"offset"`
	Size           uint64   `json:"size"`
	ChecksumType   string   `json:"checksumType"`
	ChecksumDigest string   `json:"checksumDigest"`
	Replicas       []string `json:"replicas"`
}

type ReplicaTarget struct {
	NodeID   string `json:"nodeId"`
	Address  string `json:"address"`
	HTTPPort uint16 `json:"port"`
}

type ChunkReadPlan struct {
	Index           uint32          `json:"index"`
	ChunkID         string          `json:"chunkId"`
	StorageIdentity string          `json:"storageIdentity"`
	Size            uint64          `json:"size"`
	ChecksumType    string          `json:"checksumType"`
	ChecksumDigest  string          `json:"checksumDigest"`
	ReadCapability  string          `json:"readCapability"`
	Replicas        []ReplicaTarget `json:"replicas"`
}

type ObjectReadPlan struct {
	ObjectRef
	FileSize  uint64          `json:"fileSize"`
	ChunkSize uint32          `json:"chunkSize"`
	Chunks    []ChunkReadPlan `json:"chunks"`
}

type IntegrityStatus string

const (
	VerifiedWholeChunk     IntegrityStatus = "verified-whole-chunk"
	UnverifiedPartialRange IntegrityStatus = "unverified-partial-range"
	UnverifiedChecksumOff  IntegrityStatus = "unverified-checksum-disabled"
)

type RangeReadResult struct {
	BytesRead uint64
	Integrity IntegrityStatus
}

type TransferStats struct {
	DataRequests     uint64
	ReplicaFallbacks uint64
	BytesVerified    uint64
}
