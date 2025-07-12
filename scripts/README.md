# Scripts Directory

This directory contains utility scripts for managing and operating the EDGP Rules Engine.

## Available Scripts

### `sqs_cli.py`
**SQS Management CLI Tool**

A command-line interface for managing SQS operations, testing, and monitoring.

**Usage:**
```bash
# Show configuration
python scripts/sqs_cli.py config

# Check SQS health
python scripts/sqs_cli.py health

# Get queue statistics
python scripts/sqs_cli.py stats

# Send test message
python scripts/sqs_cli.py send-test

# Check output queue for results
python scripts/sqs_cli.py check-output
```

**Purpose:**
- Production operations and monitoring
- Troubleshooting SQS connectivity
- Testing message flow
- Queue management

**When to use:**
- Setting up SQS integration
- Debugging queue issues
- Monitoring system health
- Testing before deployment

## Installation

No additional installation required. Scripts use the same dependencies as the main application.

## Security Note

These scripts use the same AWS credentials and queue URLs as configured in your `.env` file. Ensure proper access controls are in place.
