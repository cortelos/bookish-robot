import time
import threading
from flask import Flask, request, jsonify
import signal
import sys
import os
from download_transactions import download_xls_from_drive
from export_transactions import export_transactions_to_s3
from common import *

# Initialize Flask app
app = Flask(__name__)

last_request_time = time.time()
IDLE_TIMEOUT = int(os.getenv('IDLE_TIMEOUT', 300))  # Default to 5 minutes (300 seconds)

# Flask route to trigger the process
@app.route('/process_transactions', methods=['GET'])
def process_transactions():
    """Mock processing of transactions."""
    update_last_request_time()

    print(f"Processing request from: {request.remote_addr}")
    # download_xls_from_drive()
    export_transactions_to_s3()
    
    # Return a simple message
    return jsonify({
        "status": "success",
        "message": "Transaction processing triggered successfully."
    })


@app.route('/shutdown', methods=['POST'])
def shutdown():
    """Endpoint to manually shut down the service."""
    update_last_request_time()
    shutdown_server()
    return "Server shutting down..."


def update_last_request_time():
    """Update the timestamp of the last received request."""
    global last_request_time
    last_request_time = time.time()


def shutdown_server():
    """Gracefully shutdown the Flask server."""
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError("Not running with the Werkzeug Server")
    func()


def monitor_idle_time():
    """Monitor idle time and exit if no requests received within timeout."""
    global last_request_time
    while True:
        time.sleep(10)  # Check every 10 seconds
        if time.time() - last_request_time > IDLE_TIMEOUT:
            print(f"Shutting down due to {IDLE_TIMEOUT} seconds of inactivity.")
            os.kill(os.getpid(), signal.SIGINT)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    print("Shutting down gracefully...")
    sys.exit(0)


if __name__ == "__main__":
    # Handle SIGINT and SIGTERM signals for clean shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start idle monitoring in a separate thread
    threading.Thread(target=monitor_idle_time, daemon=True).start()

    # Run Flask server
    app.run(host="0.0.0.0", port=8080)
