#!/bin/bash

# Set variables
QUEUE_NAME="data-processing-queue"
REGION="us-east-1"
BUCKET_ARN="XXXX"


# Get queue URL
QUEUE_URL=$(aws sqs get-queue-url --queue-name $QUEUE_NAME --region $REGION --query 'QueueUrl' --output text)

# Delete the queue
aws sqs delete-queue --queue-url $QUEUE_URL --region $REGION

echo "SQS queue '$QUEUE_NAME' has been deleted successfully."

aws s3tables delete-table \
--table-bucket-arn $BUCKET_ARN \
--namespace example_namespace --name orders