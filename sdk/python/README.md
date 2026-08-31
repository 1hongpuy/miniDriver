# MiniDriver Python SDK

`minidriver` is a native Python implementation of the MiniDriver object
protocol. It uses only the Python standard library, never uses DataNode extent
files, and persists only `ObjectRef`.

```python
from minidriver import Client, ClientConfig, PutOptions

client = Client(ClientConfig(
    gateway_url="http://gateway:18280",
    cluster_internal_token="<injected-secret>",
    service_principal="openclip-worker",
))

ref = client.put_file("/work/input.jpg", PutOptions(chunk_window=2))
client.get_object(ref, lambda chunk: decoder.feed(chunk))
```

Install for local development:

```bash
cd sdk/python
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
```

The SDK uses a bounded thread pool only for chunks of one upload. The current
standard-library HTTP transport opens an independent request for each HTTP
operation; a process-wide Python connection pool is a separate future
optimization, not an implied guarantee of this first adapter.
