#!/usr/bin/env python3
import json
import os
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
WWW = ROOT / "www-v2"


class StaticHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WWW), **kwargs)

    def log_message(self, _format, *_args):
        pass


def catalog(path):
    directories = [{"path": "/trip", "createdAt": 1}] if path == "/" else []
    files = [{
        "objectId": "object-photo", "name": "test.jpg", "fileHash": "f" * 64,
        "fileSize": 1024, "state": "AVAILABLE", "createdAt": 1,
    }] if path == "/" else []
    breadcrumbs = ([{"name": "素材库", "path": "/"}] if path == "/" else
                   [{"name": "素材库", "path": "/"}, {"name": "trip", "path": "/trip"}])
    return {"path": path, "breadcrumbs": breadcrumbs, "directories": directories, "files": files}


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), StaticHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    calls = []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1440, "height": 900})
                page.set_default_timeout(1500)
                page.on("dialog", lambda dialog: dialog.accept())

                def api(route):
                    parsed = urlparse(route.request.url)
                    calls.append((route.request.method, parsed.path, parsed.query))
                    if parsed.path == "/api/v2/catalog":
                        path = parse_qs(parsed.query).get("path", ["/"])[0]
                        route.fulfill(status=200, content_type="application/json", body=json.dumps(catalog(path)))
                    elif route.request.method == "DELETE" and parsed.path == "/api/v2/objects/object-photo":
                        route.fulfill(status=200, content_type="application/json", body='{"status":"deleted"}')
                    elif route.request.method == "DELETE" and parsed.path == "/api/v2/directories":
                        route.fulfill(status=200, content_type="application/json", body='{"status":"deleted"}')
                    elif parsed.path == "/api/v2/admin/nodes":
                        route.fulfill(status=200, content_type="application/json", body='{"nodes":[]}')
                    else:
                        route.fulfill(status=404, content_type="application/json", body='{"error":"unexpected request"}')

                page.route("**/api/v2/**", api)
                page.goto(f"http://127.0.0.1:{server.server_port}/", wait_until="networkidle")
                assert page.locator("#delete-directory").is_hidden()
                screenshot = os.environ.get("MINIKV_UI_SCREENSHOT")
                if screenshot:
                    page.screenshot(path=screenshot, full_page=True)

                page.get_by_role("button", name="删除 test.jpg").click()
                page.wait_for_timeout(50)
                assert ("DELETE", "/api/v2/objects/object-photo", "") in calls

                page.locator("#directory-tree").get_by_role("button", name="trip", exact=True).click()
                page.locator("#delete-directory").wait_for(state="visible")
                page.get_by_role("button", name="删除当前目录").click()
                page.wait_for_timeout(50)
                assert ("DELETE", "/api/v2/directories", "path=%2Ftrip") in calls
                assert page.locator("#active-directory-path").inner_text() == "/"
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()

    print("PASS: V2 delete UI calls object and directory delete APIs")


if __name__ == "__main__":
    main()
