#!/bin/bash

# Set variables
QUEUE_NAME="data-processing-queue"
REGION="us-east-1"
BUCKET="XXX"

# Create SQS queue
aws sqs create-queue --queue-name $QUEUE_NAME --region $REGION

# Get queue URL and ARN
QUEUE_URL=$(aws sqs get-queue-url --queue-name $QUEUE_NAME --query 'QueueUrl' --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Set queue attributes
aws sqs set-queue-attributes --queue-url $QUEUE_URL --region $REGION --attributes '{
  "Policy": "{\"Version\":\"2012-10-17\",\"Id\":\"S3SendMessagePolicy\",\"Statement\":[{\"Sid\":\"AllowS3ToSendMessage\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"SQS:SendMessage\",\"Resource\":\"'${QUEUE_ARN}'\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::'${BUCKET}'\"}}}]}"
}'

echo "SQS queue created and configured successfully."
echo "Queue URL: $QUEUE_URL"
echo "Queue ARN: $QUEUE_ARN"
