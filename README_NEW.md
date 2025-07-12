# EDGP Rules Engine

The EDGP Rules Engine is a FastAPI-based microservice that provides data validation capabilities using a rules-based system. It supports real-time validation requests, CORS configuration for web applications, and AWS SQS integration for scalable queue-based processing.

## Features

- **Real-time Validation**: Direct HTTP API for immediate validation results
- **Comprehensive Rule Set**: 20+ built-in validation rules covering various data types and constraints
- **Flexible Data Input**: Support for JSON data arrays with customizable validation rules
- **Detailed Reporting**: Comprehensive validation results with success/failure details
- **CORS Support**: Full cross-origin resource sharing configuration for web applications
- **SQS Integration**: Queue-based processing for scalable validation workflows with automatic load balancing
- **Health Monitoring**: Built-in health checks and status endpoints
- **CLI Tools**: Command-line utilities for management and testing

## Quick Start

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd edgp-rules-engine
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment (optional):
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Run the server:
```bash
python -m app.main
```

The API will be available at `http://localhost:8008`

### Basic Usage

Send a POST request to `/validate` with your data and validation rules:

```json
{
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
  ]
}
```

## API Endpoints

### Core Validation

- `POST /validate` - Validate data with specified rules
- `GET /rules` - Get list of available validation rules
- `GET /health` - Health check endpoint with CORS configuration

### SQS Integration

- `GET /sqs/status` - Get SQS manager status and worker statistics
- `GET /sqs/health` - Health check for SQS connection and queues
- `POST /sqs/start` - Start SQS processing workers
- `POST /sqs/stop` - Stop SQS processing workers
- `GET /sqs/queue-stats` - Get queue message counts
- `POST /sqs/send-message` - Send test message to queue
- `GET /sqs/worker-stats` - Get detailed worker performance

### Documentation

- `GET /docs` - Interactive API documentation (Swagger UI)
- `GET /redoc` - Alternative API documentation

## Available Validation Rules

The engine supports a comprehensive set of validation rules:

### Existence and Structure
- `expect_column_to_exist` - Verify column exists in dataset

### Numeric Validations
- `expect_column_values_to_be_between` - Values within numeric range
- `expect_column_values_to_be_greater_than` - Values above threshold
- `expect_column_values_to_be_less_than` - Values below threshold
- `expect_column_values_to_be_positive` - All values are positive
- `expect_column_min_to_be_between` - Column minimum within range
- `expect_column_max_to_be_between` - Column maximum within range
- `expect_column_mean_to_be_between` - Column average within range
- `expect_column_median_to_be_between` - Column median within range
- `expect_column_sum_to_be_between` - Column sum within range

### Data Type Validations
- `expect_column_values_to_be_of_type` - Values match specific data type
- `expect_column_values_to_be_in_type_list` - Values match one of allowed types
- `expect_column_values_to_be_boolean` - Values are boolean (True/False)

### String and Length Validations
- `expect_column_value_lengths_to_be_between` - String lengths within range
- `expect_column_value_lengths_to_equal` - String lengths equal specific value

### Set and Uniqueness Validations
- `expect_column_values_to_be_in_set` - Values from allowed set
- `expect_column_distinct_values_to_be_in_set` - Unique values from allowed set
- `expect_column_values_to_be_unique` - All values are unique
- `expect_column_proportion_of_unique_values_to_be_between` - Uniqueness ratio within range

### Date and Time Validations
- `expect_column_values_to_be_dateutil_parseable` - Values are valid dates
- `expect_column_values_to_be_between_dates` - Dates within date range
- `expect_column_values_to_be_before` - Dates before specified date
- `expect_column_values_to_be_after` - Dates after specified date

### Null and Missing Value Validations
- `expect_column_values_to_be_none` - Values are null/None

### Sequence Validations
- `expect_column_values_to_be_increasing` - Values in ascending order
- `expect_column_values_to_be_decreasing` - Values in descending order

### Comparative Validations
- `expect_column_pair_values_to_be_equal` - Two columns have equal values
- `expect_column_pair_values_a_to_be_greater_than_b` - Column A > Column B

## Response Format

The API returns detailed validation results:

```json
{
  "total_rules": 2,
  "successful_rules": 2,
  "failed_rules": 0,
  "validation_results": [
    {
      "rule": "expect_column_to_exist",
      "column": "name",
      "success": true,
      "message": "Column exists"
    },
    {
      "rule": "expect_column_values_to_be_between",
      "column": "age",
      "success": true,
      "message": "All values are between 18 and 65"
    }
  ]
}
```

## Configuration

Create a `.env` file for configuration:

```bash
# Server Configuration
PORT=8008
HOST=0.0.0.0
ENVIRONMENT=development

# CORS Configuration
ALLOWED_ORIGINS=["http://localhost:3000","http://127.0.0.1:3000"]

# AWS SQS Configuration (optional)
SQS_AWS_ACCESS_KEY_ID=your_access_key_here
SQS_AWS_SECRET_ACCESS_KEY=your_secret_key_here
SQS_AWS_REGION=us-east-1
SQS_INPUT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/validation-input-queue
SQS_OUTPUT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/validation-output-queue
SQS_DLQ_URL=https://sqs.us-east-1.amazonaws.com/123456789012/validation-dlq
SQS_WORKER_COUNT=4
SQS_AUTO_START_WORKERS=false
SQS_MAX_RETRIES=3
SQS_MAX_MESSAGES_PER_POLL=10
SQS_VISIBILITY_TIMEOUT=300
SQS_WAIT_TIME_SECONDS=20
```

## CORS Configuration

### Development Setup

For local development with frontend applications:

```bash
# .env file
ENVIRONMENT=development
ALLOWED_ORIGINS=["http://localhost:3000","http://127.0.0.1:3000","http://localhost:8080"]
```

### Production Setup

For production deployment:

```bash
# .env.production file
ENVIRONMENT=production
ALLOWED_ORIGINS=["https://yourdomain.com","https://www.yourdomain.com"]
```

### Frontend Integration

**JavaScript/React Example:**
```javascript
// Fetch example with CORS
fetch('http://localhost:8008/validate', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({
        data: [{name: 'John', age: 25}],
        rules: [{rule_name: 'expect_column_to_exist', column_name: 'name'}]
    })
})
.then(response => response.json())
.then(data => console.log(data));
```

**HTML Testing Page:**
```html
<!DOCTYPE html>
<html>
<head><title>EDGP Rules Engine Test</title></head>
<body>
    <button onclick="testValidation()">Test Validation</button>
    <div id="result"></div>
    <script>
        async function testValidation() {
            try {
                const response = await fetch('http://localhost:8008/validate', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        data: [{name: 'John', age: 25}],
                        rules: [{rule_name: 'expect_column_to_exist', column_name: 'name'}]
                    })
                });
                const result = await response.json();
                document.getElementById('result').textContent = JSON.stringify(result, null, 2);
            } catch (error) {
                console.error('CORS Error:', error);
            }
        }
    </script>
</body>
</html>
```

### CORS Testing

Test CORS configuration:

```bash
# Test preflight request
curl -X OPTIONS http://localhost:8008/validate \
  -H "Origin: http://localhost:3000" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: Content-Type"

# Run CORS-specific tests
python tests/test_cors.py
```

### CORS Troubleshooting

**Common CORS Errors:**

1. **"Access to fetch at '...' has been blocked by CORS policy"**
   - Check if your origin is in the `ALLOWED_ORIGINS` list
   - Verify the server is running with CORS enabled

2. **"Preflight request doesn't pass access control check"**
   - Ensure OPTIONS method is allowed
   - Check Access-Control-Request-Headers are permitted

3. **"Credentials flag is 'true', but access control allow origin is '*'"**
   - Remove wildcard from allowed_origins when using credentials
   - Specify exact origins instead

**Security Considerations:**
- Never use wildcard (`*`) in production
- Specify exact domains for allowed origins
- Use HTTPS in production
- Limit allowed methods to only what's needed

## AWS SQS Integration

### Overview

The SQS integration enables scalable queue-based validation processing with:

- **Scalable Processing**: Multiple concurrent workers for high throughput
- **Load Balancing**: Automatic distribution of messages across workers
- **Fault Tolerance**: Retry logic and Dead Letter Queue (DLQ) support
- **Monitoring**: Health checks and performance statistics
- **Automatic Output**: All processed results are sent to output queue

### Architecture

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

### SQS Message Format

**Input Message (Validation Request):**
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

**Output Message (Validation Results):**
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

### SQS Client Integration Examples

**Python Client:**
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

**Node.js Client:**
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

### Performance Tuning

**Worker Configuration:**
- **worker_count**: Increase for higher throughput (4-8 recommended)
- **max_messages_per_poll**: Batch size per poll (10 recommended)
- **visibility_timeout**: Time to process message (300s recommended)

**Queue Configuration:**
- **wait_time_seconds**: Long polling reduces API calls (20s recommended)
- **poll_interval**: Delay when no messages available (5s recommended)

### Error Handling and Monitoring

**Retry Logic:**
- Failed messages are automatically retried up to `max_retries` times
- Retry delay increases exponentially: 30s, 60s, 120s, etc.
- Messages that exceed retry limit are sent to the Dead Letter Queue

**Monitoring Metrics:**
- Queue message counts (visible, in-flight, delayed)
- Worker processing rates and success rates
- Error rates and DLQ message counts
- Processing latency

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run specific test files
python tests/test_validators.py
python tests/test_rules.py
python tests/test_cors.py
python tests/test_sqs.py

# Test with different environments
python tests/test_all_validators.py
```

## CLI Tools and Examples

### CLI Management Tool

Use the included CLI tool for SQS testing and management:

```bash
# Check output queue for results
python scripts/sqs_cli.py check-output

# Show current configuration
python scripts/sqs_cli.py config

# Check SQS health
python scripts/sqs_cli.py health

# Get queue statistics  
python scripts/sqs_cli.py stats

# Send a test message
python scripts/sqs_cli.py send-test
```

### Example Scripts

Explore functionality with provided examples:

```bash
# Complete SQS workflow demonstration
python examples/demo_sqs_workflow.py

# Test output queue functionality
python examples/test_output_queue.py
```

See the `examples/` and `scripts/` directories for detailed usage scenarios and operational tools.

## Project Structure

```
edgp-rules-engine/
├── app/
│   ├── main.py              # FastAPI application entry point
│   ├── api/                 # API route definitions
│   │   ├── routes.py        # Core validation routes
│   │   └── sqs_routes.py    # SQS management routes
│   ├── core/                # Core configuration
│   │   └── config.py        # Environment and CORS config
│   ├── models/              # Pydantic models
│   │   ├── rule.py          # Rule definitions
│   │   ├── sqs_models.py    # SQS message models
│   │   ├── validation_request.py
│   │   └── validation_response.py
│   ├── rules/               # Rule definitions and logic
│   ├── sqs/                 # SQS integration
│   │   ├── client.py        # SQS client wrapper
│   │   ├── config.py        # SQS configuration
│   │   ├── manager.py       # Worker management
│   │   └── processor.py     # Message processing logic
│   └── validators/          # Individual validation implementations
├── examples/                # Example scripts and demos
│   ├── demo_sqs_workflow.py # Complete SQS workflow demo
│   └── test_output_queue.py # Output queue testing
├── scripts/                 # Operational and management scripts
│   └── sqs_cli.py          # SQS command-line interface
├── tests/                   # Test suite
│   ├── test_validators.py   # Validator tests
│   ├── test_cors.py        # CORS functionality tests
│   ├── test_sqs.py         # SQS integration tests
│   └── cors_test.html      # Browser CORS testing
└── requirements.txt         # Python dependencies
```

## Security Considerations

### CORS Security
- Never use wildcard (`*`) in production
- Specify exact domains for allowed origins
- Use HTTPS in production
- Review headers that are exposed/allowed

### SQS Security
- Use IAM roles instead of access keys when possible
- Limit SQS permissions to required actions only
- Enable SQS encryption for sensitive data
- Use VPC endpoints for private network access
- Rotate AWS credentials regularly

## Scaling and Performance

### Horizontal Scaling
- Deploy multiple application instances
- Each instance runs independent worker pools
- SQS automatically distributes messages

### Vertical Scaling
- Increase worker count per instance
- Optimize processing timeout values
- Use larger EC2 instances for CPU-intensive validation

### Monitoring and Alerting
Set up monitoring for:
- Queue depth (input/output/DLQ)
- Processing latency
- Error rates
- Worker health
- AWS SQS metrics (CloudWatch)

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
