# AWS SQS Integration for EDGP Rules Engine

This document describes the AWS SQS integration for queue-based validation processing in the EDGP Rules Engine.

## Overview

The SQS integration enables the EDGP Rules Engine to process data validation requests from AWS SQS queues, providing:

- **Scalable Processing**: Multiple concurrent workers for high throughput
- **Load Balancing**: Automatic distribution of messages across workers
- **Fault Tolerance**: Retry logic and Dead Letter Queue (DLQ) support
- **Monitoring**: Health checks and performance statistics
- **Flexible Configuration**: Environment-based configuration

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Input Queue   │───▶│  Workers Pool   │───▶│  Output Queue   │
│                 │    │                 │    │                 │
│ Validation      │    │ • Worker 1      │    │ Validation      │
│ Requests        │    │ • Worker 2      │    │ Results         │
│                 │    │ • Worker 3      │    │                 │
│                 │    │ • Worker 4      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                 │
                                 ▼
                       ┌─────────────────┐
                       │   Dead Letter   │
                       │     Queue       │
                       │                 │
                       │ Failed Messages │
                       └─────────────────┘
```

## Configuration

### Environment Variables

Add these variables to your `.env` file:

```bash
# AWS Credentials
SQS_AWS_ACCESS_KEY_ID=your_access_key_here
SQS_AWS_SECRET_ACCESS_KEY=your_secret_key_here
SQS_AWS_REGION=us-east-1

# Queue URLs
SQS_INPUT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/validation-input-queue
SQS_OUTPUT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/validation-output-queue
SQS_DLQ_URL=https://sqs.us-east-1.amazonaws.com/123456789012/validation-dlq

# Worker Configuration
SQS_WORKER_COUNT=4
SQS_AUTO_START_WORKERS=false
SQS_MAX_RETRIES=3

# Processing Configuration
SQS_MAX_MESSAGES_PER_POLL=10
SQS_VISIBILITY_TIMEOUT=300
SQS_WAIT_TIME_SECONDS=20
```

### Required AWS Permissions

Your AWS credentials need the following SQS permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:SendMessage",
                "sqs:ChangeMessageVisibility",
                "sqs:GetQueueAttributes"
            ],
            "Resource": [
                "arn:aws:sqs:*:*:validation-input-queue",
                "arn:aws:sqs:*:*:validation-output-queue",
                "arn:aws:sqs:*:*:validation-dlq"
            ]
        }
    ]
}
```

## Message Format

### Input Message (Validation Request)

```json
{
    "message_id": "unique-message-id",
    "correlation_id": "optional-correlation-id",
    "timestamp": "2024-01-01T12:00:00Z",
    "source": "source-system",
    "data": [
        {"name": "John", "age": 25, "email": "john@example.com"},
        {"name": "Jane", "age": 30, "email": "jane@example.com"}
    ],
    "rules": [
        {
            "rule_name": "expect_column_to_exist",
            "column_name": "name",
            "value": {}
        },
        {
            "rule_name": "expect_column_values_to_be_between",
            "column_name": "age",
            "value": {"min_value": 18, "max_value": 65}
        }
    ],
    "priority": 5,
    "max_retries": 3,
    "callback_url": "https://example.com/webhook"
}
```

### Output Message (Validation Results)

```json
{
    "message_id": "unique-message-id",
    "correlation_id": "optional-correlation-id",
    "processed_at": "2024-01-01T12:00:05Z",
    "processing_time_ms": 1500,
    "status": "success",
    "worker_id": "worker-abc123",
    "validation_results": [
        {
            "rule": "expect_column_to_exist",
            "column": "name",
            "success": true,
            "message": "Column exists"
        }
    ],
    "summary": {
        "total_rules": 2,
        "passed": 2,
        "failed": 0
    },
    "total_rules": 2,
    "successful_rules": 2,
    "failed_rules": 0
}
```

## API Endpoints

### SQS Management Endpoints

- `GET /sqs/status` - Get SQS manager status and worker statistics
- `GET /sqs/health` - Health check for SQS connection and queues
- `POST /sqs/start` - Start SQS processing workers
- `POST /sqs/stop` - Stop SQS processing workers
- `GET /sqs/queue-stats` - Get queue message counts
- `POST /sqs/send-message` - Send test message to queue
- `GET /sqs/worker-stats` - Get detailed worker performance

### Example Usage

```bash
# Check SQS health
curl http://localhost:8008/sqs/health

# Get queue statistics
curl http://localhost:8008/sqs/queue-stats

# Start SQS workers
curl -X POST http://localhost:8008/sqs/start

# Get worker status
curl http://localhost:8008/sqs/status
```

## CLI Management Tool

Use the included CLI tool for testing and management:

```bash
# Show current configuration
python sqs_cli.py config

# Check SQS health
python sqs_cli.py health

# Get queue statistics  
python sqs_cli.py stats

# Send a test message
python sqs_cli.py send-test
```

## Error Handling

### Retry Logic

- Failed messages are automatically retried up to `max_retries` times
- Retry delay increases exponentially: 30s, 60s, 120s, etc.
- Messages that exceed retry limit are sent to the Dead Letter Queue

### Dead Letter Queue

Messages are sent to DLQ when:
- Maximum retries exceeded
- Permanent validation errors (e.g., invalid data format)
- Processing timeout exceeded

### Monitoring

Monitor the following metrics:
- Queue message counts (visible, in-flight, delayed)
- Worker processing rates and success rates
- Error rates and DLQ message counts
- Processing latency

## Load Balancing

The system automatically distributes work across multiple workers:

- Each worker polls for messages independently
- SQS visibility timeout prevents message duplication
- Workers process messages in parallel
- Failed messages are redistributed to other workers

## Performance Tuning

### Worker Configuration

- **worker_count**: Increase for higher throughput (4-8 recommended)
- **max_messages_per_poll**: Batch size per poll (10 recommended)
- **visibility_timeout**: Time to process message (300s recommended)

### Queue Configuration

- **wait_time_seconds**: Long polling reduces API calls (20s recommended)
- **poll_interval**: Delay when no messages available (5s recommended)

### AWS SQS Limits

- Maximum message size: 256 KB
- Maximum visibility timeout: 12 hours
- Maximum retention period: 14 days

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify AWS credentials and region
   - Check queue URLs are correct
   - Ensure IAM permissions are set

2. **No Messages Processed**
   - Check if workers are running (`/sqs/status`)
   - Verify queue has messages (`/sqs/queue-stats`)
   - Check worker logs for errors

3. **High Error Rates**
   - Review DLQ messages for patterns
   - Check validation rule syntax
   - Monitor processing timeouts

### Logs

Enable debug logging:

```python
import logging
logging.getLogger('app.sqs').setLevel(logging.DEBUG)
```

### Health Checks

Regular health checks verify:
- SQS connection status
- Queue accessibility
- Worker status
- Processing rates

## Security

- Use IAM roles instead of access keys when possible
- Limit SQS permissions to required actions only
- Enable SQS encryption for sensitive data
- Use VPC endpoints for private network access
- Rotate AWS credentials regularly

## Scaling

### Horizontal Scaling

- Deploy multiple application instances
- Each instance runs independent worker pools
- SQS automatically distributes messages

### Vertical Scaling

- Increase worker count per instance
- Optimize processing timeout values
- Use larger EC2 instances for CPU-intensive validation

## Monitoring and Alerting

Set up monitoring for:

- Queue depth (input/output/DLQ)
- Processing latency
- Error rates
- Worker health
- AWS SQS metrics (CloudWatch)

## Integration Examples

### Python Client

```python
import boto3
import json

sqs = boto3.client('sqs', region_name='us-east-1')

message = {
    "message_id": "example-001",
    "data": [{"name": "John", "age": 25}],
    "rules": [{"rule_name": "expect_column_to_exist", "column_name": "name"}]
}

sqs.send_message(
    QueueUrl='your-input-queue-url',
    MessageBody=json.dumps(message)
)
```

### Node.js Client

```javascript
const AWS = require('aws-sdk');
const sqs = new AWS.SQS({region: 'us-east-1'});

const message = {
    message_id: 'example-001',
    data: [{name: 'John', age: 25}],
    rules: [{rule_name: 'expect_column_to_exist', column_name: 'name'}]
};

sqs.sendMessage({
    QueueUrl: 'your-input-queue-url',
    MessageBody: JSON.stringify(message)
}, (err, data) => {
    if (err) console.error(err);
    else console.log('Message sent:', data.MessageId);
});
```
