#!/bin/bash

# Check if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in the environment
if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]]; then
  echo "Error: AWS credentials are not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
  exit 1
fi

# Set the Docker image name
IMAGE_NAME="bookish-robot"

# Build the Docker image
echo "Building Docker image '$IMAGE_NAME'..."
docker build -t "$IMAGE_NAME" .

# Check if the build was successful
if [[ $? -ne 0 ]]; then
  echo "Error: Docker image build failed."
  exit 1
fi

# Run the Docker container with the necessary AWS credentials
echo "Running Docker container '$IMAGE_NAME' locally..."

docker run -d -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  -e AWS_DEFAULT_REGION="us-east-1" \
  "$IMAGE_NAME"

# Check if the container is running
if [[ $? -eq 0 ]]; then
  echo "Docker container is running on http://localhost:8080"
else
  echo "Error: Failed to run Docker container."
  exit 1
fi
