#!/usr/bin/env python3
"""Serve conversation_viewer.html with a proxy to the OH agent-server (bypasses CORS)."""
from __future__ import annotations

import http.server
import json
import os
import urllib.request
import urllib.error

OH_BASE = os.getenv("OH_BASE_URL", "http://127.0.0.1:8011")
PORT = int(os.getenv("VIEWER_PORT", "8088"))
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))


def _make_handler(tools_dir: str):
    class ViewerHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=tools_dir, **kw)

        def do_GET(self):
            if self.path.startswith("/api/"):
                return self._proxy("GET")
            super().do_GET()

        def do_POST(self):
            if self.path.startswith("/api/"):
                return self._proxy("POST")
            self.send_error(405)

        def _proxy(self, method: str):
            target = OH_BASE + self.path
            try:
                body = None
                if method == "POST":
                    length = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(length) if length else None
                req = urllib.request.Request(target, data=body, method=method)
                req.add_header("Accept", "application/json")
                if body:
                    req.add_header("Content-Type", "application/json")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = resp.read()
                    self.send_response(resp.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(data)
            except urllib.error.HTTPError as e:
                self.send_response(e.code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            except Exception as e:
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        def log_message(self, fmt, *args):
            pass

    return ViewerHandler

if __name__ == "__main__":
    Handler = _make_handler(TOOLS_DIR)
    with http.server.HTTPServer(("0.0.0.0", PORT), Handler) as srv:
        print(f"Conversation Viewer: http://0.0.0.0:{PORT}/conversation_viewer.html")
        print(f"Proxying API calls to: {OH_BASE}")
        srv.serve_forever()
