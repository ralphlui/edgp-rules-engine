"""
Complete SQS workflow demonstration.
This script shows the full message processing flow from input to output queue.
"""
import asyncio
import json
import time
import sys
from datetime import datetime

sys.path.append('/Users/jjfwang/Documents/02-NUS/01-Capstone/edgp-rules-engine')

async def demo_complete_workflow():
    """Demonstrate the complete SQS workflow"""
    try:
        from app.sqs.config import SQSSettings
        from app.sqs.client import SQSClient
        from app.models.sqs_models import SQSValidationRequest, ValidationRule
        
        print("🚀 SQS Complete Workflow Demo")
        print("=" * 60)
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        print(f"📍 Configuration:")
        print(f"  Input Queue: {settings.input_queue_url}")
        print(f"  Output Queue: {settings.output_queue_url}")
        print(f"  Has Output Queue: {settings.has_output_queue}")
        
        if not settings.has_output_queue:
            print("❌ Output queue not configured! Please set SQS_OUTPUT_QUEUE_URL in .env")
            return
        
        # Step 1: Create validation request
        print(f"\n📝 Step 1: Creating validation request...")
        
        test_request = SQSValidationRequest(
            message_id=f"demo-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            correlation_id=f"demo-corr-{int(time.time())}",
            source="demo_script",
            data=[
                {"name": "Alice Johnson", "age": 28, "email": "alice@example.com", "salary": 55000},
                {"name": "Bob Wilson", "age": 35, "email": "bob@example.com", "salary": 70000},
                {"name": "Carol Brown", "age": 42, "email": "carol@example.com", "salary": 85000}
            ],
            rules=[
                ValidationRule(rule_name="expect_column_to_exist", column_name="name"),
                ValidationRule(rule_name="expect_column_to_exist", column_name="age"),
                ValidationRule(
                    rule_name="expect_column_values_to_be_between",
                    column_name="age",
                    value={"min_value": 18, "max_value": 65}
                ),
                ValidationRule(rule_name="expect_column_values_to_be_valid_email", column_name="email"),
                ValidationRule(
                    rule_name="expect_column_values_to_be_between",
                    column_name="salary",
                    value={"min_value": 30000, "max_value": 100000}
                )
            ],
            priority=5,
            callback_url=None  # Optional webhook URL
        )
        
        print(f"  ✅ Created request with {len(test_request.rules)} validation rules")
        print(f"  ✅ Dataset contains {len(test_request.data)} records")
        print(f"  ✅ Message ID: {test_request.message_id}")
        
        # Step 2: Send to input queue
        print(f"\n📤 Step 2: Sending message to input queue...")
        
        message_id = client.send_message(
            test_request.dict(),
            queue_url=settings.input_queue_url
        )
        
        if message_id:
            print(f"  ✅ Message sent successfully!")
            print(f"  ✅ SQS Message ID: {message_id}")
        else:
            print(f"  ❌ Failed to send message")
            return
        
        # Step 3: Wait for processing (workers should pick it up)
        print(f"\n⏳ Step 3: Waiting for message processing...")
        print(f"  📝 Make sure SQS workers are running:")
        print(f"     curl -X POST http://localhost:8008/sqs/start")
        print(f"  📝 Or start the FastAPI app with auto_start_workers=true")
        
        # Check for results in output queue
        print(f"\n🔍 Step 4: Checking output queue for results...")
        
        max_wait_time = 60  # Wait up to 60 seconds
        wait_interval = 5   # Check every 5 seconds
        waited_time = 0
        result_found = False
        
        while waited_time < max_wait_time and not result_found:
            print(f"  🔄 Checking... ({waited_time}s elapsed)")
            
            output_messages = client.receive_messages(settings.output_queue_url)
            
            for msg in output_messages:
                try:
                    # Check if this is our result
                    result_data = msg.body
                    if hasattr(result_data, 'correlation_id') and result_data.correlation_id == test_request.correlation_id:
                        print(f"\n🎉 Found our result!")
                        print(f"  📊 Result Details:")
                        print(f"     Message ID: {result_data.message_id}")
                        print(f"     Correlation ID: {result_data.correlation_id}")
                        print(f"     Status: {getattr(result_data, 'status', 'N/A')}")
                        print(f"     Worker ID: {getattr(result_data, 'worker_id', 'N/A')}")
                        print(f"     Processing Time: {getattr(result_data, 'processing_time_ms', 'N/A')}ms")
                        print(f"     Total Rules: {getattr(result_data, 'total_rules', 'N/A')}")
                        print(f"     Successful Rules: {getattr(result_data, 'successful_rules', 'N/A')}")
                        print(f"     Failed Rules: {getattr(result_data, 'failed_rules', 'N/A')}")
                        
                        # Show validation results
                        if hasattr(result_data, 'validation_results') and result_data.validation_results:
                            print(f"\n  📋 Validation Results:")
                            for i, rule_result in enumerate(result_data.validation_results[:3], 1):
                                print(f"     {i}. Rule: {rule_result.get('rule', 'N/A')}")
                                print(f"        Success: {rule_result.get('success', False)}")
                                print(f"        Message: {rule_result.get('message', 'N/A')}")
                        
                        result_found = True
                        
                        # Delete the message from output queue (optional)
                        client.delete_message(msg.receipt_handle, settings.output_queue_url)
                        print(f"  🗑️ Result message deleted from output queue")
                        break
                        
                except Exception as e:
                    print(f"  ⚠️ Error parsing message: {e}")
            
            if not result_found:
                await asyncio.sleep(wait_interval)
                waited_time += wait_interval
        
        if not result_found:
            print(f"\n⏰ No results found after {max_wait_time}s")
            print(f"  📝 Check if SQS workers are running")
            print(f"  📝 Check worker logs for processing status")
            print(f"  📝 Manual check: python sqs_cli.py check-output")
        
        print(f"\n✅ Demo complete!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(demo_complete_workflow())
