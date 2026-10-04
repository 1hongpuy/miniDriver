import json
import socketserver
import threading
import unittest
from http.server import BaseHTTPRequestHandler

from minidriver import Client, ClientConfig, ObjectRef, ReadOptions
from minidriver.client import MiniDriverError, _checksum


DATA = b"MiniDriver Python SDK"


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _start(handler):
    server = _Server(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


class ClientTest(unittest.TestCase):
    def setUp(self):
        digest = _checksum("crc32c", DATA)

        class DataHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/internal/v3/chunks/chunk-1" or self.headers.get("X-Read-Token") != "capability":
                    self.send_error(400)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(DATA)))
                self.end_headers()
                self.wfile.write(DATA)

            def log_message(self, *_):
                pass

        self.data = _start(DataHandler)
        data_port = self.data.server_address[1]

        class GatewayHandler(BaseHTTPRequestHandler):
            def _authorized(self):
                return (self.headers.get("X-Cluster-Internal-Token") == "secret" and
                        self.headers.get("X-Service-Principal") == "python-test")

            def _json(self, status, value):
                encoded = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def do_POST(self):
                if not self._authorized():
                    self._json(403, {"error": "auth"})
                    return
                if self.path.endswith("/read-plan"):
                    self._json(200, {"objectId": "object-1", "objectVersion": 1, "fileSize": len(DATA),
                                     "chunkSize": len(DATA), "chunks": [{"index": 0, "storageIdentity": "chunk-1",
                                     "size": len(DATA), "checksumType": "crc32c", "checksumDigest": digest,
                                     "readCapability": "capability", "replicas": [{"nodeId": "node-1",
                                     "address": "127.0.0.1", "httpPort": data_port}]}]})
                elif self.path.endswith("/read-hints"):
                    self._json(200, _hints())
                elif self.path.endswith("read-hints:batch"):
                    self._json(200, {"objects": [_hints()]})
                else:
                    self._json(404, {"error": "unexpected"})

            def do_GET(self):
                if not self._authorized() or not self.path.endswith("/head"):
                    if self._authorized() and self.path == "/api/v2/objects/object-1/layout?version=1":
                        self._json(200, {"objectId": "object-1", "version": 1, "size": len(DATA),
                                         "chunks": [{"index": 0, "chunkId": "chunk-1", "offset": 0,
                                         "size": len(DATA), "checksumType": "crc32c", "checksumDigest": digest,
                                         "replicas": ["node-1"]}]})
                    else:
                        self._json(403, {"error": "auth"})
                    return
                self._json(200, {"objectId": "object-1", "objectVersion": 1, "metadataVersion": 2,
                                 "fileSize": len(DATA), "state": "COMMITTED"})

            def do_DELETE(self):
                if not self._authorized() or not self.path.endswith("/delete"):
                    self._json(403, {"error": "auth"})
                    return
                self._json(202, {"status": "deleting"})

            def log_message(self, *_):
                pass

        def _hints():
            return {"objectId": "object-1", "objectVersion": 1, "fileSize": len(DATA),
                    "candidates": [{"nodeId": "node-1", "localBytes": len(DATA),
                                    "coveragePermille": 1000, "health": "healthy"}]}

        self.gateway = _start(GatewayHandler)
        self.client = Client(ClientConfig(f"http://127.0.0.1:{self.gateway.server_address[1]}", "secret", "python-test"))

    def tearDown(self):
        self.gateway.shutdown()
        self.gateway.server_close()
        self.data.shutdown()
        self.data.server_close()

    def test_object_read_controls_and_hints(self):
        reference = ObjectRef("object-1", 1)
        received = bytearray()
        stats = self.client.get_object(reference, received.extend)
        self.assertEqual(bytes(received), DATA)
        self.assertEqual(stats.bytes_verified, len(DATA))
        self.assertEqual(self.client.head_object(reference).state, "COMMITTED")
        self.assertEqual(self.client.get_object_read_hints(reference).candidates[0].coverage_ratio, 1.0)
        layout = self.client.get_object_layout(reference)
        self.assertEqual(layout.chunks[0].replicas, ("node-1",))
        self.assertEqual(layout.chunks[0].offset + layout.chunks[0].size, layout.size)
        self.assertEqual(len(self.client.batch_get_object_read_hints([reference])), 1)
        self.client.delete_object(reference)

    def test_crc32c_and_bad_checksum(self):
        self.assertEqual(_checksum("crc32c", b"123456789"), "e3069283")
        original = _checksum
        import minidriver.client as module
        module._checksum = lambda *_: "00000000"
        try:
            with self.assertRaises(MiniDriverError):
                self.client.get_object(ObjectRef("object-1", 1), lambda _: None, ReadOptions())
        finally:
            module._checksum = original
