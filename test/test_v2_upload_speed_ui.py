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
                page.set_default_timeout(60000)
                page.add_init_script("""
                    class FakeXMLHttpRequest {
                      constructor() { this.upload = {}; this.status = 0; this.responseText = ""; this.headers = {}; }
                      open() {}
                      setRequestHeader() {}
                      getResponseHeader(name) { return this.headers[name.toLowerCase()] || null; }
                      send(body) {
                        window.__putAttempts = (window.__putAttempts || 0) + 1;
                        window.__activeUploads = (window.__activeUploads || 0) + 1;
                        window.__maxActiveUploads = Math.max(window.__maxActiveUploads || 0, window.__activeUploads);
                        if (window.__activeUploads > 2) {
                          setTimeout(() => {
                            window.__activeUploads -= 1;
                            this.status = 503;
                            this.responseText = '{"error":"DataNode write capacity reached"}';
                            this.headers["retry-after"] = "0";
                            this.onload();
                          }, 20);
                          return;
                        }
                        if (window.__failNextUpload) {
                          window.__failNextUpload = false;
                          setTimeout(() => {
                            window.__activeUploads -= 1;
                            this.status = 400;
                            this.responseText = '{"error":"invalid upload"}';
                            this.onload();
                          }, 20);
                          return;
                        }
                        const total = body.size;
                        setTimeout(() => this.upload.onprogress({ lengthComputable: true, loaded: total / 2, total }), 40);
                        setTimeout(() => this.upload.onprogress({ lengthComputable: true, loaded: total, total }), 220);
                        setTimeout(() => {
                          window.__activeUploads -= 1;
                          this.status = 200; this.responseText = "{}"; this.onload();
                        }, 260);
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
                            "chunkSize": 4 * 1024 * 1024, "totalChunks": 3, "completed": [],
                        }))
                    elif parsed.path == "/api/v2/upload/sessions/speed-session/routes":
                        requested = json.loads(route.request.post_data)
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({"routes": [{
                            "chunkHash": chunk["hash"], "primaryAddress": "node-test", "primaryPort": 9002,
                            "primaryNodeId": "node-test", "uploadToken": "token", "chain": [],
                        } for chunk in requested["chunks"]]}))
                    elif parsed.path == "/api/v2/upload/sessions/speed-session/commit":
                        route.fulfill(status=200, content_type="application/json", body='{"state":"AVAILABLE","fileHash":"abc"}')
                    elif parsed.path == "/api/v2/admin/nodes":
                        route.fulfill(status=200, content_type="application/json", body='{"nodes":[]}')
                    else:
                        route.fulfill(status=404, content_type="application/json", body='{"error":"unexpected request"}')

                page.route("**/api/v2/**", api)
                page.goto(f"http://127.0.0.1:{server.server_port}/", wait_until="networkidle")
                page.set_input_files("#file-input", [
                    {"name": "speed-a.jpg", "mimeType": "image/jpeg", "buffer": b"x" * (3 * 4 * 1024 * 1024)},
                    {"name": "speed-b.jpg", "mimeType": "image/jpeg", "buffer": b"y" * (3 * 4 * 1024 * 1024)},
                ])
                page.locator("#start-upload").click()
                details = page.locator(".queue-item[data-state='completed'] .queue-item__detail")
                details.nth(1).wait_for(state="visible")
                text = details.nth(0).inner_text()
                assert "平均" in text and "/s" in text, text
                assert page.evaluate("window.__maxActiveUploads") <= 2
                assert page.evaluate("window.__putAttempts") == 6

                completed_attempts = page.evaluate("window.__putAttempts")
                page.evaluate("window.__failNextUpload = true")
                page.set_input_files("#file-input", [
                    {"name": "failed-a.jpg", "mimeType": "image/jpeg", "buffer": b"a" * (3 * 4 * 1024 * 1024)},
                    {"name": "next-b.jpg", "mimeType": "image/jpeg", "buffer": b"b" * (3 * 4 * 1024 * 1024)},
                ])
                page.locator("#start-upload").click()
                page.locator(".queue-item[data-state='failed']").wait_for(state="visible")
                page.locator(".queue-item[data-state='completed']").wait_for(state="visible")
                assert page.evaluate("window.__putAttempts") - completed_attempts == 5
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()

    print("PASS: V2 scheduler limits PUTs and isolates failed file workers")


if __name__ == "__main__":
    main()
