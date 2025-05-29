# health.py
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import threading
import contextlib


class _Handler(BaseHTTPRequestHandler):
    ready = False         # flipped to True by mark_ready()

    def do_GET(self):
        if self.path != "/ping":
            self.send_error(404)
            return

        if self.ready:
            self.send_response(200)          # healthy
            self.end_headers()
            self.wfile.write(b"pong")
        else:
            self.send_response(503)          # not-ready (startup)
            self.end_headers()
            self.wfile.write(b"not ready")

    def log_message(self, *_):               # silence default logs
        pass


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def start(addr: str = ":8081"):
    """
    Launch /ping in a background thread.
    Returns a shutdown() function you may call on program exit.
    """
    host, port = addr.split(":") if ":" in addr else ("", addr)
    httpd = ThreadingHTTPServer((host or "", int(port)), _Handler)

    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    def shutdown():
        with contextlib.suppress(Exception):
            httpd.shutdown()

    return shutdown


def mark_ready():
    """Call once all init (model load, RabbitMQ, etc.) succeeded."""
    _Handler.ready = True
