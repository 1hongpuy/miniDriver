#!/usr/bin/env python3
import json
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright


WWW = Path(__file__).resolve().parents[1] / "www-v2"


class StaticHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WWW), **kwargs)

    def log_message(self, _format, *_args):
        pass


def catalog(path):
    return {
        "path": path,
        "breadcrumbs": [{"name": "素材库", "path": "/"}],
        "directories": [],
        "files": [],
    }


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), StaticHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1280, "height": 800})
                page.set_default_timeout(2000)
                page.add_init_script("""
                    class FakeXMLHttpRequest {
                      constructor() { this.upload = {}; this.status = 0; this.responseText = ""; }
                      open() {}
                      setRequestHeader() {}
                      send(body) {
                        const total = body.size;
                        setTimeout(() => this.upload.onprogress({ lengthComputable: true, loaded: total / 2, total }), 40);
                        setTimeout(() => this.upload.onprogress({ lengthComputable: true, loaded: total, total }), 100);
                        setTimeout(() => { this.status = 200; this.responseText = "{}"; this.onload(); }, 130);
                      }
                    }
                    window.XMLHttpRequest = FakeXMLHttpRequest;
                """)

                def api(route):
                    parsed = urlparse(route.request.url)
                    if parsed.path == "/api/v2/catalog":
                        path = parse_qs(parsed.query).get("path", ["/"])[0]
                        route.fulfill(status=200, content_type="application/json", body=json.dumps(catalog(path)))
                    elif parsed.path == "/api/v2/upload/preflight":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({
                            "status": "UPLOAD_REQUIRED", "sessionId": "speed-session",
                            "chunkSize": 1024, "totalChunks": 1, "completed": [],
                        }))
                    elif parsed.path == "/api/v2/upload/sessions/speed-session/routes":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({"routes": [{
                            "chunkHash": "a" * 64, "primaryAddress": "node-test", "primaryPort": 9002,
                            "primaryNodeId": "node-test", "uploadToken": "token", "chain": [],
                        }]}))
                    elif parsed.path == "/api/v2/upload/sessions/speed-session/commit":
                        route.fulfill(status=200, content_type="application/json", body='{"state":"AVAILABLE","fileHash":"abc"}')
                    elif parsed.path == "/api/v2/admin/nodes":
                        route.fulfill(status=200, content_type="application/json", body='{"nodes":[]}')
                    else:
                        route.fulfill(status=404, content_type="application/json", body='{"error":"unexpected request"}')

                page.route("**/api/v2/**", api)
                page.goto(f"http://127.0.0.1:{server.server_port}/", wait_until="networkidle")
                page.set_input_files("#file-input", [{
                    "name": "speed.jpg", "mimeType": "image/jpeg", "buffer": b"x" * 1024,
                }])
                page.locator("#start-upload").click()
                detail = page.locator(".queue-item[data-state='completed'] .queue-item__detail")
                detail.wait_for(state="visible")
                text = detail.inner_text()
                assert "平均" in text and "/s" in text, text
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()

    print("PASS: V2 upload queue shows per-file average throughput")


if __name__ == "__main__":
    main()
