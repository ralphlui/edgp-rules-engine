"""
Test script to verify SQS output queue functionality.
"""
import asyncio
import json
import sys
from datetime import datetime

sys.path.insert(0, '/Users/jjfwang/Documents/02-NUS/01-Capstone/edgp-rules-engine')

async def test_output_queue():
    """Test that messages are properly sent to output queue after processing"""
    try:
        from app.sqs.config import SQSSettings
        from app.sqs.client import SQSClient
        from app.sqs.processor import MessageProcessor
        from app.models.sqs_models import SQSValidationRequest, ValidationRule, SQSMessageWrapper
        
        print("🔧 Testing SQS Output Queue Functionality")
        print("=" * 60)
        
        # Initialize settings and client
        settings = SQSSettings()
        print(f"Input Queue: {settings.input_queue_url}")
        print(f"Output Queue: {settings.output_queue_url}")
        print(f"Has Output Queue: {settings.has_output_queue}")
        
        if not settings.has_output_queue:
            print("❌ No output queue configured!")
            return
        
        client = SQSClient(settings)
        processor = MessageProcessor(settings, client)
        
        # Create test validation request
        test_request = SQSValidationRequest(
            message_id=f"test-output-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            correlation_id="test-correlation-123",
            data=[
                {"name": "John Doe", "age": 25, "email": "john@example.com"},
                {"name": "Jane Smith", "age": 30, "email": "jane@example.com"}
            ],
            rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name"
                ),
                ValidationRule(
                    rule_name="expect_column_values_to_be_between",
                    column_name="age",
                    value={"min_value": 18, "max_value": 65}
                )
            ]
        )
        
        # Create message wrapper (simulate SQS message)
        message_wrapper = SQSMessageWrapper(
            receipt_handle="test-receipt-handle",
            message_id="test-sqs-message-id",
            body=test_request
        )
        
        print(f"\n📨 Processing test message: {test_request.message_id}")
        
        # Process the message
        result = await processor.process_message(message_wrapper)
        
        print(f"✅ Processing Result:")
        print(f"  - Success: {result.success}")
        print(f"  - Message ID: {result.message_id}")
        print(f"  - Processing Time: {result.processing_time_ms}ms")
        
        if result.response:
            print(f"  - Status: {result.response.status}")
            print(f"  - Worker ID: {result.response.worker_id}")
            print(f"  - Total Rules: {result.response.total_rules}")
            print(f"  - Successful Rules: {result.response.successful_rules}")
            print(f"  - Failed Rules: {result.response.failed_rules}")
        
        print(f"\n📊 Check your output queue for the processed results!")
        print(f"Queue URL: {settings.output_queue_url}")
        
        # Try to check output queue
        print(f"\n🔍 Checking output queue for messages...")
        output_messages = client.receive_messages(settings.output_queue_url)
        
        if output_messages:
            print(f"✅ Found {len(output_messages)} message(s) in output queue:")
            for msg in output_messages:
                print(f"  - Message ID: {msg.body.message_id}")
                print(f"  - Status: {msg.body.status if hasattr(msg.body, 'status') else 'N/A'}")
        else:
            print("📭 No messages found in output queue (may take a moment to appear)")
        
    except Exception as e:
        print(f"❌ Error testing output queue: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_output_queue())
