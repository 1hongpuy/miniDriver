# MiniDriver Go SDK

`minidriver-go` is the native Go counterpart of the MiniDriver object SDK. It
uses the Gateway/DataNode HTTP contract directly; it does not use cgo and it
never reads DataNode extent files.

```go
client, err := minidriver.NewClient(minidriver.Config{
    GatewayURL:          "http://gateway:18280",
    ClusterInternalToken: os.Getenv("MINIKV_V2_CLUSTER_SECRET"),
    ServicePrincipal:    "openclip-worker",
})

ref, err := client.PutFile(ctx, "/work/input.jpg", minidriver.PutOptions{
    ChecksumType: "crc32c", ChunkWindow: 2,
})
err = client.GetObject(ctx, ref, minidriver.ReadOptions{}, func(bytes []byte) error {
    return decoder.Feed(bytes)
})
```

Use one `Client` per Go process. Its `http.Client` is safe for concurrent use;
object uploads use a bounded per-object Chunk window. Do not persist a
ReadPlan or DataNode read token: persist only `ObjectRef`.

The SDK currently needs Go 1.18 or later and only uses the standard library.
