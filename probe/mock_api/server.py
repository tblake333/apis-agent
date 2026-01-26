"""
Mock Cloud API Server for local development and testing.

This simple server receives change events from the probe and logs them.
Use this for local development without needing a real cloud backend.

Usage:
    python server.py

The server will listen on port 8080 and accept POST requests to /api/changes
"""

import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Store received changes in memory (for inspection)
received_changes = []


class MockAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the mock cloud API."""

    def _send_json_response(self, status_code: int, data: dict):
        """Send a JSON response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/health' or self.path == '/api/changes/health':
            self._send_json_response(200, {'status': 'healthy'})
        elif self.path == '/api/changes':
            # Return received changes for inspection
            self._send_json_response(200, {
                'count': len(received_changes),
                'changes': received_changes[-100:]  # Last 100 changes
            })
        else:
            self._send_json_response(404, {'error': 'Not found'})

    def do_POST(self):
        """Handle POST requests."""
        if self.path == '/api/changes':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)

            try:
                change = json.loads(body.decode('utf-8'))

                # Add metadata
                change['received_at'] = datetime.utcnow().isoformat()
                change['_id'] = len(received_changes) + 1

                # Log the change
                logger.info(f"Received {change.get('type', 'UNKNOWN')} on {change.get('table', 'unknown')}")

                # Store the change
                received_changes.append(change)

                self._send_json_response(201, {
                    'status': 'accepted',
                    'id': change['_id']
                })

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
                self._send_json_response(400, {'error': 'Invalid JSON'})
            except Exception as e:
                logger.error(f"Error processing change: {e}")
                self._send_json_response(500, {'error': str(e)})
        else:
            self._send_json_response(404, {'error': 'Not found'})

    def log_message(self, format, *args):
        """Override to use Python logging instead of stderr."""
        logger.debug(f"{self.address_string()} - {format % args}")


def run_server(host: str = '0.0.0.0', port: int = 8080):
    """Run the mock API server."""
    server_address = (host, port)
    httpd = HTTPServer(server_address, MockAPIHandler)
    logger.info(f"Mock Cloud API server running on http://{host}:{port}")
    logger.info("Endpoints:")
    logger.info("  GET  /health        - Health check")
    logger.info("  GET  /api/changes   - List received changes")
    logger.info("  POST /api/changes   - Submit a change event")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        httpd.shutdown()


if __name__ == '__main__':
    run_server()
