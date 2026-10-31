import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .db import snapshot


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/runs':
            try:
                body = json.dumps(snapshot(), default=str).encode()
                self.send_response(200)
            except Exception:
                logging.exception('Dashboard database query failed')
                body = b'{"error":"Database unavailable. Check container logs."}'
                self.send_response(503)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-store')
        elif self.path in ('/', '/index.html'):
            body = Path(__file__).with_name('dashboard.html').read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
        else:
            self.send_error(404)
            return
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0', 8501), Handler).serve_forever()
