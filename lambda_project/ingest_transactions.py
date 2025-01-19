import boto3
import uuid
from decimal import Decimal
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import json
import os
import hashlib


# Global Constants
REGION_NAME = "us-east-1"
FOLDER_ID = "1drnXtVxi5-yEu117ukZril1AZfodZ1cW"  # Google Drive folder ID
SECRET_NAME = "GoogleDriveCredentials"  # AWS Secrets Manager secret name


# Initialize DynamoDB connection
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table('Transactions')


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


# Insert transaction in dynamo
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



# Lambda Handler
def lambda_handler(event, context):
    download_xls_from_drive()
    return {
        "statusCode": 200,
        "body": "Transactions imported successfully."
    }

if __name__ == "__main__":
    lambda_handler({}, None)