import boto3
import csv
import io
from decimal import Decimal
from datetime import datetime

S3_BUCKET_NAME = 'bookish-robot'


def export_transactions_to_s3():
    # Initialize the clients
    dynamodb = boto3.client('dynamodb')
    s3 = boto3.client('s3')

    # Query DynamoDB table (Replace with your table name and any filtering)
    response = dynamodb.scan(TableName='Transactions')

    # Specify the S3 bucket and file name
    s3_bucket = S3_BUCKET_NAME
    s3_key = 'transactions_to_forecast.csv'

    # Prepare the CSV data to upload to S3
    csv_data = []
    for item in response['Items']:
        # Skip transactions without a category
        if 'category' not in item:
            continue
        
        # Extract relevant fields and convert them
        transaction_id = item['transaction_id']['S']
        transaction_date = item['date']['S']  # Assuming date is stored as a string (YYYY-MM-DD HH:MM:SS)
        
        # Reformat the timestamp to ISO 8601 format (if it's not already in that format)
        try:
            # Assuming transaction_date is in format 'YYYY-MM-DD HH:MM:SS'
            date_obj = datetime.strptime(transaction_date, '%Y-%m-%d %H:%M:%S')
            formatted_date = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')  # Convert to ISO 8601 format
        except ValueError:
            formatted_date = transaction_date  # If the date is already in the correct format
        
        category = item['category']['S']
        sub_category = item.get('sub_category', {}).get('S', 'Unknown')  # Default to 'Unknown' if sub_category is missing
        note = item.get('note', {}).get('S', '')  # Default to empty string if note is missing
        
        # Use both category and sub_category as item_id
        item_id = f"{category}-{sub_category}"
        
        # Credit is positive, Debit is negative
        credit = float(item['credit']['N']) if 'credit' in item else 0.0
        debit = float(item['debit']['N']) if 'debit' in item else 0.0
        # Target value will be credit (positive) - debit (negative)
        target_value = credit - debit
        
        # Add the data to csv
        csv_data.append({
            'transaction_id': transaction_id,
            'timestamp': formatted_date,
            'item_id': item_id,  # Use category and sub_category as item_id
            'category': category,
            'sub_category': sub_category,
            'credit': credit,
            'debit': debit,
            'note': note,
            'target_value': target_value
        })

    # Write to CSV in memory
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=['transaction_id', 'timestamp', 'item_id', 'category', 'sub_category', 'credit', 'debit', 'note', 'target_value'])
    csv_writer.writeheader()
    csv_writer.writerows(csv_data)

    # Upload the CSV to S3
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    print(f"Data uploaded to S3: s3://{s3_bucket}/{s3_key}")