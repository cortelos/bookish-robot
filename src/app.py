import boto3
import uuid
from decimal import Decimal
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import json
import hashlib
import time
import threading
from flask import Flask, request, jsonify
import signal
import sys
import os

# Global Constants
REGION_NAME = "us-east-1"
FOLDER_ID = "1drnXtVxi5-yEu117ukZril1AZfodZ1cW"  # Google Drive folder ID
SECRET_NAME = "GoogleDriveCredentials"  # AWS Secrets Manager secret name

# Initialize DynamoDB connection
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table('Transactions')

# Initialize Flask app
app = Flask(__name__)

last_request_time = time.time()
IDLE_TIMEOUT = int(os.getenv('IDLE_TIMEOUT', 300))  # Default to 5 minutes (300 seconds)


# Load Google Drive Credentials from Secrets Manager
def load_google_credentials():
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    secret = client.get_secret_value(SecretId=SECRET_NAME)
    credentials_info = json.loads(secret['SecretString'])

    creds = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return creds


# Download XLS from Google Drive
def download_xls_from_drive():
    creds = load_google_credentials()
    service = build('drive', 'v3', credentials=creds)

    # List files in the folder
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id, name)",
    ).execute()
    items = results.get('files', [])

    if not items:
        print("No XLS files found.")
        return

    for item in items:
        file_id = item['id']
        file_name = item['name']
        print(f"Downloading {file_name}...")

        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        df = pd.read_excel(fh)
        process_transactions_from_df(df)


# Process and Insert Transactions into DynamoDB
def process_transactions_from_df(df):
    required_columns = ['Date', 'Category', 'Sub Category', 'Credit', 'Debit', 'Note']
    
    # Ensure the XLS file contains all required columns
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing one or more required columns: {required_columns}")

    # Iterate over rows and insert into DynamoDB
    for _, row in df.iterrows():
        insert_transaction(
            date=str(row['Date']),
            category=row['Category'],
            sub_category=row['Sub Category'],
            credit=float(row['Credit']) if not pd.isna(row['Credit']) else 0.0,
            debit=float(row['Debit']) if not pd.isna(row['Debit']) else 0.0,
            note=row['Note'] if not pd.isna(row['Note']) else ""
        )


# Generate a hash-based transaction_id
def generate_transaction_id(date, category, sub_category, credit, debit, note):
    # Concatenate all fields into a single string
    transaction_string = f"{date}|{category}|{sub_category}|{credit}|{debit}|{note}"
    # Hash the string using SHA-256
    transaction_hash = hashlib.sha256(transaction_string.encode()).hexdigest()
    return transaction_hash


# Insert transaction in DynamoDB
def insert_transaction(date, category, sub_category, credit, debit, note):
    # Generate transaction_id based on transaction details
    transaction_id = generate_transaction_id(date, category, sub_category, credit, debit, note)
    
    transaction = {
        'transaction_id': transaction_id,
        'date': date,
        'category': category,
        'sub_category': sub_category,
        'credit': Decimal(str(credit)),
        'debit': Decimal(str(debit)),
        'note': note
    }
    
    # Insert or overwrite the item in DynamoDB
    table.put_item(Item=transaction)
    print(f"Inserted or updated transaction: {transaction}")


# Flask route to trigger the process
@app.route('/process_transactions', methods=['GET'])
def process_transactions():
    """Mock processing of transactions."""
    update_last_request_time()

    # Simulate processing
    print(f"Processing request from: {request.remote_addr}")
    download_xls_from_drive()
    
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
