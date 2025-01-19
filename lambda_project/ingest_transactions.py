import boto3
import uuid
from decimal import Decimal

# Initialize DynamoDB connection
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Transactions')  # Make sure this matches your DynamoDB table name


def lambda_handler(event, context):
    # Example data for testing
    insert_transaction("2024-01-17", "Spotify Subscription", -9.99)
    insert_transaction("2024-01-18", "Groceries", -45.30)
    
    return {
        "statusCode": 200,
        "body": "Transactions inserted successfully"
    }


def insert_transaction(date, description, amount):
    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'date': date,
        'description': description,
        'amount': Decimal(str(amount))  # Convert to Decimal for DynamoDB
    }
    table.put_item(Item=transaction)
    print(f"Inserted transaction: {transaction}")


if __name__ == "__main__":
    # Simulated event and context for local testing
    event = {}  # You can mock event data here if needed
    context = None  # Context is usually not needed for basic testing

    # Call the Lambda handler
    lambda_handler(event, context)