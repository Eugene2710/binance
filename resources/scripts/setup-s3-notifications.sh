#!/bin/bash
# Bash exits the shell if a command fails
set -e

echo 'Waiting for LocalStack to be ready...'

# Wait for LocalStack health check to pass
MAX_ATTEMPTS=30
ATTEMPT=1

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Checking LocalStack health..."
    
    if curl -s http://localstack:4566/_localstack/health | grep -q '"s3": "available"' && \
       curl -s http://localstack:4566/_localstack/health | grep -q '"sqs": "available"'; then
        echo "LocalStack is ready!"
        break
    fi
    
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "LocalStack failed to become ready after $MAX_ATTEMPTS attempts"
        exit 1
    fi
    
    echo "LocalStack not ready yet, waiting 2 seconds..."
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done

echo 'Creating crypto bucket...'
aws --endpoint-url=http://localstack:4566 s3 mb s3://crypto

echo 'Creating SQS queue...'
aws --endpoint-url=http://localstack:4566 sqs create-queue --queue-name klines-notifications

echo 'Getting queue ARN...'
QUEUE_URL=$(aws --endpoint-url=http://localstack:4566 sqs get-queue-url --queue-name klines-notifications --query 'QueueUrl' --output text)
QUEUE_ARN=$(aws --endpoint-url=http://localstack:4566 sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

echo 'Setting up S3 bucket notification configuration...'

echo 'Updating notification config with actual Queue ARN...'
sed "s/QUEUE_ARN_PLACEHOLDER/$QUEUE_ARN/g" /config/notification-config.json > /tmp/notification-config.json

echo 'Applying notification configuration...'
aws --endpoint-url=http://localstack:4566 s3api put-bucket-notification-configuration --bucket crypto --notification-configuration file:///tmp/notification-config.json

echo 'S3 to SQS notifications setup complete!'
echo 'Queue URL: ' $QUEUE_URL
echo 'Queue ARN: ' $QUEUE_ARN