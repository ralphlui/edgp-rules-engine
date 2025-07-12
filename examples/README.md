# Examples Directory

This directory contains example scripts and demonstrations of the EDGP Rules Engine functionality.

## Available Examples

### `demo_sqs_workflow.py`
**Complete SQS Workflow Demonstration**

Shows the end-to-end SQS message processing flow from input queue to output queue.

**Usage:**
```bash
python examples/demo_sqs_workflow.py
```

**What it demonstrates:**
- Creating validation requests
- Sending messages to input queue
- Waiting for processing
- Retrieving results from output queue
- Complete message lifecycle

### `test_output_queue.py`
**Output Queue Functionality Test**

Tests the output queue functionality by processing a validation message and verifying results.

**Usage:**
```bash
python examples/test_output_queue.py
```

**What it tests:**
- Message processing pipeline
- Output queue message creation
- Result data structure
- Error handling

## Prerequisites

Before running examples:

1. **Configure AWS credentials** in `.env`:
   ```bash
   SQS_AWS_ACCESS_KEY_ID=your_key
   SQS_AWS_SECRET_ACCESS_KEY=your_secret
   SQS_INPUT_QUEUE_URL=your_input_queue_url
   SQS_OUTPUT_QUEUE_URL=your_output_queue_url
   ```

2. **Start SQS workers** (for workflow demo):
   ```bash
   # Option 1: Via API
   curl -X POST http://localhost:8008/sqs/start
   
   # Option 2: Set auto-start in .env
   SQS_AUTO_START_WORKERS=true
   ```

## Learning Path

Recommended order for exploring SQS functionality:

1. **Start with CLI**: `python scripts/sqs_cli.py config`
2. **Test connectivity**: `python scripts/sqs_cli.py health` 
3. **Run output test**: `python examples/test_output_queue.py`
4. **Complete workflow**: `python examples/demo_sqs_workflow.py`

## Troubleshooting

If examples fail:
- Check AWS credentials and permissions
- Verify queue URLs are correct
- Ensure SQS workers are running
- Review application logs for errors

## Customization

These examples can be modified to:
- Test different validation rules
- Use custom data sets
- Integrate with your specific workflows
- Add custom monitoring and alerting
