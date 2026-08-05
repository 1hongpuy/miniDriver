#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
WWW = ROOT / "www-v2"
THUMBNAIL_BYTES = base64.b64decode(
    "/9j/4AAQSkZJRgABAgAAAQABAAD//gARTGF2YzU4LjEzNC4xMDAA/9sAQwAIBAQEBAQFBQUFBQUGB"
    "gYGBgYGBgYGBgYGBwcHCAgIBwcHBgYHBwgICAgJCQkICAgICQkKCgoMDAsLDg4OEREU/8QATQABAQ"
    "AAAAAAAAAAAAAAAAAAAAYBAQEBAAAAAAAAAAAAAAAAAAAEBhABAAAAAAAAAAAAAAAAAAAAABEBAAA"
    "AAAAAAAAAAAAAAAAAAP/AABEIACAAIAMBIgACEQADEQD/2gAMAwEAAhEDEQA/AKEBh0QAAAAAD//Z"
)
THUMBNAIL_HASH = hashlib.sha256(THUMBNAIL_BYTES).hexdigest()


class StaticHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WWW), **kwargs)

    def log_message(self, _format, *_args):
        pass


def catalog():
    return {
        "path": "/",
        "breadcrumbs": [{"name": "素材库", "path": "/"}],
        "directories": [{"path": "/shoots", "createdAt": 1}],
        "files": [
            {
                "objectId": "source-ready",
                "name": "sunset.jpg",
                "fileHash": "a" * 64,
                "fileSize": 1024,
                "state": "AVAILABLE",
                "thumbnail": {
                    "profile": "thumb-512-jpeg-v1",
                    "state": "READY",
                    "objectId": "thumbnail-ready",
                },
                "preview": {
                    "profile": "preview-2048-jpeg-v1",
                    "state": "READY",
                    "objectId": "preview-ready",
                },
            },
            {
                "objectId": "source-pending",
                "name": "next-frame.jpg",
                "fileHash": "b" * 64,
                "fileSize": 2048,
                "state": "AVAILABLE",
                "thumbnail": {
                    "profile": "thumb-512-jpeg-v1",
                    "state": "PENDING",
                },
                "preview": {
                    "profile": "preview-2048-jpeg-v1",
                    "state": "PENDING",
                },
            },
        ],
    }


def object_metadata(object_id):
    return {
        "objectId": object_id,
        "name": "thumb-512-jpeg-v1.jpg" if object_id == "thumbnail-ready" else
                "preview-2048-jpeg-v1.jpg" if object_id == "preview-ready" else "sunset.jpg",
        "fileSize": len(THUMBNAIL_BYTES),
        "state": "AVAILABLE",
    }


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), StaticHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    requested_paths = []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                viewport = {"width": 1440, "height": 900}
                if os.environ.get("MINIKV_UI_MOBILE"):
                    viewport = {"width": 375, "height": 812}
                page = browser.new_page(viewport=viewport)
                page.set_default_timeout(3000)

                def api(route):
                    parsed = urlparse(route.request.url)
                    requested_paths.append(parsed.path)
                    if parsed.path == "/api/v2/catalog":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps(catalog()))
                    elif parsed.path == "/api/v2/admin/nodes":
                        route.fulfill(status=200, content_type="application/json", body='{"nodes":[]}')
                    elif parsed.path == "/api/v2/objects/thumbnail-ready":
                        route.fulfill(status=200, content_type="application/json",
                                      body=json.dumps(object_metadata("thumbnail-ready")))
                    elif parsed.path == "/api/v2/objects/thumbnail-ready/manifest":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({
                            "fileSize": len(THUMBNAIL_BYTES), "chunks": [{
                                "index": 0, "hash": THUMBNAIL_HASH,
                                "replicas": [{"address": "node-test", "httpPort": 9002}],
                            }],
                        }))
                    elif parsed.path == "/api/v2/objects/preview-ready":
                        route.fulfill(status=200, content_type="application/json",
                                      body=json.dumps(object_metadata("preview-ready")))
                    elif parsed.path == "/api/v2/objects/preview-ready/manifest":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({
                            "fileSize": len(THUMBNAIL_BYTES), "chunks": [{
                                "index": 0, "hash": THUMBNAIL_HASH,
                                "replicas": [{"address": "node-test", "httpPort": 9002}],
                            }],
                        }))
                    else:
                        route.fulfill(status=404, content_type="application/json", body='{"error":"unexpected"}')

                def chunk(route):
                    route.fulfill(status=200, content_type="application/octet-stream", body=THUMBNAIL_BYTES)

                page.route("**/api/v2/**", api)
                page.route("http://node-test:9002/v2/chunks/**", chunk)
                page.goto(f"http://127.0.0.1:{server.server_port}/", wait_until="networkidle")

                ready_card = page.locator(".catalog-card[data-object-id='source-ready']")
                ready_card.locator(".catalog-card__image").wait_for(state="visible")
                assert ready_card.locator(".catalog-card__image").get_attribute("src").startswith("blob:")
                assert ready_card.locator(".catalog-card__image").evaluate(
                    "image => image.complete && image.naturalWidth > 0"
                )

                pending_card = page.locator(".catalog-card[data-object-id='source-pending']")
                assert "生成中" in pending_card.inner_text()
                assert "/api/v2/objects/thumbnail-pending" not in requested_paths

                screenshot = os.environ.get("MINIKV_UI_SCREENSHOT")
                if screenshot:
                    page.screenshot(path=screenshot, full_page=True)

                ready_card.click()
                page.locator("#object-preview").wait_for(state="visible")
                assert "/api/v2/objects/preview-ready" in requested_paths
                assert "/api/v2/objects/preview-ready/manifest" in requested_paths
                assert "/api/v2/objects/source-ready/manifest" not in requested_paths
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()

    print("PASS: V2 catalog renders ready derived thumbnail without fetching pending thumbnail")


if __name__ == "__main__":
    main()
