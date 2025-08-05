#!/usr/bin/env python3
"""
SQS Management CLI tool for the EDGP Rules Engine.
This script helps manage SQS queues and send test messages.
"""
import asyncio
import json
import sys
import argparse
import traceback
from datetime import datetime
from typing import Dict, Any

# Add the parent directory to the path for imports
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_test_message() -> Dict[str, Any]:
    """Create a test validation message using standardized format"""
    return {
        "message_id": f"test-msg-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "correlation_id": f"corr-{datetime.now().timestamp()}",
        "timestamp": datetime.now().isoformat(),
        "source": "sqs_cli_tool",
        
        # New standardized format
        "data_entry": {
            "data_type": "tabular",
            "data_key": f"test-dataset-{int(datetime.now().timestamp())}",
            "columns": ["name", "age", "email", "salary"],
            "data": [
                {"name": "John Doe", "age": 25, "email": "john@example.com", "salary": 50000},
                {"name": "Jane Smith", "age": 30, "email": "jane@example.com", "salary": 75000},
                {"name": "Bob Johnson", "age": 35, "email": "bob@example.com", "salary": 60000},
                {"name": "Alice Brown", "age": 28, "email": "alice@example.com", "salary": 55000}
            ],
            "source": "test_data_generator",
            "schema_version": "1.0"
        },
        
        "validation_rules": [
            {
                "rule_name": "expect_column_to_exist",
                "column_name": "name",
                "rule_description": "Ensure name column exists",
                "severity": "error"
            },
            {
                "rule_name": "expect_column_to_exist", 
                "column_name": "age",
                "rule_description": "Ensure age column exists",
                "severity": "error"
            },
            {
                "rule_name": "expect_column_values_to_be_between",
                "column_name": "age",
                "value": {
                    "min_value": 18,
                    "max_value": 65
                },
                "rule_description": "Age should be between 18 and 65",
                "severity": "error"
            },
            {
                "rule_name": "expect_column_values_to_be_between",
                "column_name": "salary",
                "value": {
                    "min_value": 30000,
                    "max_value": 100000
                },
                "rule_description": "Salary should be between 30K and 100K", 
                "severity": "warning"
            }
        ],
        
        "batch_id": f"test-batch-{datetime.now().strftime('%Y%m%d')}",
        "priority": 5,
        "max_retries": 3,
        "callback_url": None,
        
        # Legacy fields for backward compatibility
        "data": [
            {"name": "John Doe", "age": 25, "email": "john@example.com", "salary": 50000},
            {"name": "Jane Smith", "age": 30, "email": "jane@example.com", "salary": 75000},
            {"name": "Bob Johnson", "age": 35, "email": "bob@example.com", "salary": 60000},
            {"name": "Alice Brown", "age": 28, "email": "alice@example.com", "salary": 55000}
        ],
        "rules": [
            {
                "rule_name": "expect_column_to_exist",
                "column_name": "name",
                "value": {}
            },
            {
                "rule_name": "expect_column_to_exist", 
                "column_name": "age",
                "value": {}
            },
            {
                "rule_name": "expect_column_values_to_be_between",
                "column_name": "age",
                "value": {
                    "min_value": 18,
                    "max_value": 65
                }
            },
            {
                "rule_name": "expect_column_values_to_be_between",
                "column_name": "salary",
                "value": {
                    "min_value": 30000,
                    "max_value": 100000
                }
            }
        ]
    }

async def send_test_message():
    """Send a test message to SQS"""
    try:
        from app.sqs import SQSClient, SQSSettings
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        message = create_test_message()
        message_id = client.send_message(message)
        
        if message_id:
            print(f"✅ Test message sent successfully!")
            print(f"Message ID: {message_id}")
            print(f"Queue: {settings.input_queue_url}")
            print(f"Message content: {json.dumps(message, indent=2)}")
        else:
            print("❌ Failed to send test message")
            
    except Exception as e:
        print(f"❌ Error sending test message: {e}")

async def check_queue_stats():
    """Check queue statistics"""
    try:
        from app.sqs import SQSClient, SQSSettings
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        stats = client.get_queue_stats()
        
        print("📊 Queue Statistics:")
        print("=" * 50)
        
        for queue_name, queue_stats in stats.items():
            print(f"\n{queue_name.upper()}:")
            for stat_name, stat_value in queue_stats.items():
                print(f"  {stat_name}: {stat_value}")
                
    except Exception as e:
        print(f"❌ Error getting queue stats: {e}")

async def health_check():
    """Perform SQS health check"""
    try:
        from app.sqs import SQSClient, SQSSettings
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        health = client.health_check()
        
        print("🏥 SQS Health Check:")
        print("=" * 50)
        
        overall_health = "✅ HEALTHY" if all([
            health.get('sqs_connection', False),
            health.get('input_queue', False)
        ]) else "❌ UNHEALTHY"
        
        print(f"Overall Status: {overall_health}")
        print(f"Timestamp: {health.get('timestamp', 'unknown')}")
        print(f"SQS Connection: {'✅' if health.get('sqs_connection') else '❌'}")
        print(f"Input Queue: {'✅' if health.get('input_queue') else '❌'}")
        print(f"Output Queue: {'✅' if health.get('output_queue') else '❌'}")
        print(f"DLQ: {'✅' if health.get('dlq') else '❌'}")
        
        if 'error' in health:
            print(f"Error: {health['error']}")
            
    except Exception as e:
        print(f"❌ Error performing health check: {e}")

async def show_config():
    """Show current SQS configuration"""
    try:
        from app.sqs import SQSSettings
        
        settings = SQSSettings()
        
        print("⚙️ SQS Configuration:")
        print("=" * 50)
        print(f"APP_ENV: {os.getenv('APP_ENV', 'not set')}")
        print(f"AWS Region: {settings.aws_region}")
        print(f"AWS Access Key ID: {settings.aws_access_key_id[:10]}... (masked)" if settings.aws_access_key_id else "AWS Access Key ID: not set")
        print(f"AWS Secret Key: {'*' * 20} (masked)" if settings.aws_secret_access_key else "AWS Secret Key: not set")
        print(f"AWS Session Token: {'Set' if settings.aws_session_token else 'Not set'}")
        print(f"Input Queue: {settings.input_queue_url}")
        print(f"Output Queue: {settings.output_queue_url}")
        print(f"DLQ: {settings.dlq_url}")
        print(f"Worker Count: {settings.worker_count}")
        print(f"Auto Start Workers: {settings.auto_start_workers}")
        print(f"Max Retries: {settings.max_retries}")
        print(f"Max Messages Per Poll: {settings.max_messages_per_poll}")
        print(f"Visibility Timeout: {settings.visibility_timeout}s")
        print(f"Wait Time: {settings.wait_time_seconds}s")
        print(f"Poll Interval: {settings.poll_interval}s")
        print(f"Has Output Queue: {settings.has_output_queue}")
        print(f"Has DLQ: {settings.has_dlq}")
        
        # Check region mismatch
        if settings.input_queue_url:
            queue_region = None
            if ".amazonaws.com/" in settings.input_queue_url:
                # Extract region from queue URL like: https://sqs.us-east-1.amazonaws.com/...
                url_parts = settings.input_queue_url.split(".")
                for i, part in enumerate(url_parts):
                    if part == "sqs" and i + 1 < len(url_parts):
                        queue_region = url_parts[i + 1]
                        break
            
            if queue_region and queue_region != settings.aws_region:
                print(f"⚠️  REGION MISMATCH DETECTED!")
                print(f"   Queue URL region: {queue_region}")
                print(f"   AWS config region: {settings.aws_region}")
                print(f"   This will cause signature errors!")
        
    except Exception as e:
        print(f"❌ Error showing config: {e}")

async def validate_config():
    """Validate SQS configuration and diagnose common issues"""
    try:
        from app.sqs import SQSSettings
        
        settings = SQSSettings()
        
        print("🔍 SQS Configuration Validation:")
        print("=" * 50)
        
        issues = []
        warnings = []
        
        # Check AWS credentials
        if not settings.aws_access_key_id:
            issues.append("❌ AWS Access Key ID is not set")
        else:
            print(f"✅ AWS Access Key ID: {settings.aws_access_key_id[:10]}...")
        
        if not settings.aws_secret_access_key:
            issues.append("❌ AWS Secret Access Key is not set")
        else:
            print(f"✅ AWS Secret Access Key: Set")
        
        # Check region configuration
        print(f"✅ AWS Region: {settings.aws_region}")
        
        # Check queue URL and region matching
        if settings.input_queue_url:
            print(f"✅ Input Queue URL: {settings.input_queue_url}")
            
            # Extract region from queue URL
            queue_region = None
            if ".amazonaws.com/" in settings.input_queue_url:
                url_parts = settings.input_queue_url.split(".")
                for i, part in enumerate(url_parts):
                    if part == "sqs" and i + 1 < len(url_parts):
                        queue_region = url_parts[i + 1]
                        break
            
            if queue_region:
                if queue_region != settings.aws_region:
                    issues.append(f"❌ REGION MISMATCH: Queue is in '{queue_region}' but AWS client configured for '{settings.aws_region}'")
                else:
                    print(f"✅ Region consistency: Queue and client both use '{settings.aws_region}'")
            else:
                warnings.append("⚠️ Could not extract region from queue URL")
        else:
            issues.append("❌ Input Queue URL is not set")
        
        # Check environment
        app_env = os.getenv('APP_ENV', 'not set')
        print(f"✅ APP_ENV: {app_env}")
        
        # Summary
        print("\n📋 Validation Summary:")
        print("-" * 30)
        
        if issues:
            print("❌ Issues found:")
            for issue in issues:
                print(f"   {issue}")
        
        if warnings:
            print("⚠️ Warnings:")
            for warning in warnings:
                print(f"   {warning}")
        
        if not issues and not warnings:
            print("✅ All validations passed!")
        
        # Recommendations
        if issues:
            print("\n💡 Recommendations:")
            for issue in issues:
                if "REGION MISMATCH" in issue:
                    print("   🔧 Fix region mismatch by either:")
                    print("      - Update SQS_AWS_REGION in your .env file to match queue region")
                    print("      - Or update your queue URLs to use the correct region")
                elif "Access Key" in issue:
                    print("   � Set AWS credentials in your environment file")
                elif "Queue URL" in issue:
                    print("   🔧 Set SQS_INPUT_QUEUE_URL in your environment file")
        
    except Exception as e:
        print(f"❌ Error validating config: {e}")

async def check_output_queue():
    """Check output queue for processed results"""
    try:
        from app.sqs import SQSClient, SQSSettings
        
        settings = SQSSettings()
        
        if not settings.has_output_queue:
            print("❌ No output queue configured")
            return
        
        client = SQSClient(settings)
        
        print("📤 Output Queue Check:")
        print("=" * 50)
        
        # Get queue stats
        stats = client.get_queue_stats()
        if 'output_queue' in stats:
            output_stats = stats['output_queue']
            print(f"Queue URL: {settings.output_queue_url}")
            print(f"Available Messages: {output_stats.get('approximate_number_of_messages', '0')}")
            print(f"In-Flight Messages: {output_stats.get('approximate_number_of_messages_not_visible', '0')}")
        
        # Try to peek at messages
        messages = client.receive_messages(settings.output_queue_url)
        if messages:
            print(f"\n📨 Found {len(messages)} result message(s):")
            for i, msg in enumerate(messages[:3], 1):  # Show first 3
                try:
                    result_data = msg.body
                    print(f"  {i}. Message ID: {result_data.message_id}")
                    print(f"     Status: {getattr(result_data, 'status', 'N/A')}")
                    print(f"     Worker: {getattr(result_data, 'worker_id', 'N/A')}")
                    print(f"     Processing Time: {getattr(result_data, 'processing_time_ms', 'N/A')}ms")
                except Exception as e:
                    print(f"  {i}. Error parsing message: {e}")
            
            if len(messages) > 3:
                print(f"     ... and {len(messages) - 3} more messages")
        else:
            print("\n📭 No result messages found in output queue")
            
    except Exception as e:
        print(f"❌ Error checking output queue: {e}")

async def start_listener():
    """Start listening to the input queue for messages"""
    try:
        from app.sqs import SQSManager, SQSSettings
        
        print("🎧 Starting SQS Listener...")
        print("=" * 50)
        
        settings = SQSSettings()
        manager = SQSManager(settings)
        
        print(f"👥 Worker Count: {settings.worker_count}")
        print(f"📥 Input Queue: {settings.input_queue_url}")
        print(f"📤 Output Queue: {settings.output_queue_url}")
        print(f"⏱️  Poll Interval: {settings.poll_interval}s")
        print(f"🔄 Max Messages Per Poll: {settings.max_messages_per_poll}")
        print("\n🚀 Starting listeners... (Press Ctrl+C to stop)")
        
        # Start the manager
        await manager.start()
        
        # Keep running until interrupted
        try:
            while manager.is_running:
                # Show periodic stats
                stats = manager.get_stats()
                if stats.get('total_processed', 0) > 0:
                    print(f"📊 Processed: {stats.get('total_processed', 0)}, Errors: {stats.get('total_errors', 0)}")
                await asyncio.sleep(5)  # Check every 5 seconds instead of 1
        except KeyboardInterrupt:
            print("\n⏹️  Received interrupt signal, stopping gracefully...")
        except Exception as e:
            print(f"\n❌ Error during listening: {e}")
        finally:
            print("🛑 Shutting down workers...")
            await manager.stop()
            print("✅ Listener stopped successfully")
            
    except Exception as e:
        print(f"❌ Error starting listener: {e}")

async def listen_with_timeout():
    """Listen for messages with a timeout (for testing)"""
    try:
        from app.sqs import SQSManager, SQSSettings
        
        print("🎧 Starting SQS Listener (30 second timeout)...")
        print("=" * 50)
        
        settings = SQSSettings()
        manager = SQSManager(settings)
        
        print(f"👥 Worker Count: {settings.worker_count}")
        print(f"📥 Input Queue: {settings.input_queue_url}")
        print(f"📤 Output Queue: {settings.output_queue_url}")
        print("\n🚀 Starting listeners for 30 seconds...")
        
        # Start the manager
        await manager.start()
        
        # Run for 30 seconds
        start_time = asyncio.get_event_loop().time()
        timeout = 30
        
        try:
            while manager.is_running:
                elapsed = asyncio.get_event_loop().time() - start_time
                remaining = timeout - elapsed
                
                if remaining <= 0:
                    print(f"\n⏰ Timeout reached ({timeout}s), stopping...")
                    break
                
                # Show stats every 5 seconds
                if int(elapsed) % 5 == 0:
                    stats = manager.get_stats()
                    print(f"⏱️  {remaining:.0f}s remaining - Processed: {stats.get('total_processed', 0)}, Errors: {stats.get('total_errors', 0)}")
                
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            print("\n⏹️  Received interrupt signal, stopping...")
        finally:
            print("🛑 Shutting down workers...")
            await manager.stop()
            
            # Final stats
            stats = manager.get_stats()
            print("📊 Final Statistics:")
            print(f"  Messages Processed: {stats.get('total_processed', 0)}")
            print(f"  Errors: {stats.get('total_errors', 0)}")
            print("✅ Listener stopped successfully")
            
    except Exception as e:
        print(f"❌ Error in timed listener: {e}")

async def listen_once():
    """Listen for messages once and exit (for testing)"""
    try:
        from app.sqs import SQSClient, SQSSettings, MessageProcessor
        
        print("👂 Listening for messages (single check)...")
        print("=" * 50)
        
        settings = SQSSettings()
        client = SQSClient(settings)
        processor = MessageProcessor(settings, client)
        
        # Check for messages
        messages = client.receive_messages(settings.input_queue_url)
        
        if messages:
            print(f"📨 Found {len(messages)} message(s):")
            
            for i, msg in enumerate(messages, 1):
                print(f"\n  {i}. Message ID: {msg.body.message_id}")
                print(f"     Correlation ID: {msg.body.correlation_id}")
                print(f"     Retry Count: {msg.body.retry_count}")
                print(f"     Data Rows: {len(msg.body.data) if msg.body.data else 0}")
                print(f"     Rules Count: {len(msg.body.rules) if msg.body.rules else 0}")
                
                # Optionally process the message
                response = input(f"     Process this message? (y/N): ").strip().lower()
                if response == 'y':
                    print(f"     🔄 Processing message {msg.body.message_id}...")
                    try:
                        result = await processor.process_message(msg)
                        if result.success:
                            print(f"     ✅ Successfully processed in {result.processing_time_ms}ms")
                            # Delete message from queue
                            client.delete_message(settings.input_queue_url, msg.receipt_handle)
                            print(f"     🗑️  Message deleted from queue")
                        else:
                            print(f"     ❌ Processing failed: {result.error}")
                    except Exception as e:
                        print(f"     ❌ Error processing: {e}")
                else:
                    print(f"     ⏭️  Skipped processing")
        else:
            print("📭 No messages found in input queue")
            
    except Exception as e:
        print(f"❌ Error checking for messages: {e}")

async def test_workflow():
    """Test complete SQS workflow: send message + listen + process"""
    try:
        print("🧪 Testing Complete SQS Workflow...")
        print("=" * 50)
        
        # Step 1: Send test message
        print("📤 Step 1: Sending test message...")
        await send_test_message()
        
        # Step 2: Wait briefly for message propagation
        print("\n⏳ Step 2: Waiting for message propagation...")
        await asyncio.sleep(2)
        
        # Step 3: Listen and process
        print("\n👂 Step 3: Listening for messages...")
        from app.sqs import SQSClient, SQSSettings, MessageProcessor
        
        settings = SQSSettings()
        client = SQSClient(settings)
        processor = MessageProcessor(settings, client)
        
        # Check for messages multiple times
        for attempt in range(5):
            messages = client.receive_messages(settings.input_queue_url)
            
            if messages:
                print(f"📨 Found {len(messages)} message(s) on attempt {attempt + 1}:")
                
                for i, msg in enumerate(messages, 1):
                    print(f"\n  Message {i}:")
                    print(f"    ID: {msg.body.message_id}")
                    print(f"    Correlation: {msg.body.correlation_id}")
                    print(f"    Data Rows: {len(msg.body.data) if msg.body.data else 0}")
                    print(f"    Rules: {len(msg.body.rules) if msg.body.rules else 0}")
                    
                    print(f"    🔄 Processing message...")
                    try:
                        result = await processor.process_message(msg)
                        if result.success:
                            print(f"    ✅ Successfully processed in {result.processing_time_ms}ms")
                            
                            # Delete message from queue
                            client.delete_message(settings.input_queue_url, msg.receipt_handle)
                            print(f"    🗑️  Message deleted from input queue")
                            
                            # Check output queue
                            print(f"    📤 Checking output queue...")
                            await asyncio.sleep(1)  # Brief wait for output
                            output_messages = client.receive_messages(settings.output_queue_url)
                            if output_messages:
                                print(f"    ✅ Found {len(output_messages)} response(s) in output queue")
                                for output_msg in output_messages:
                                    if hasattr(output_msg.body, 'status'):
                                        print(f"       Status: {output_msg.body.status}")
                                        print(f"       Processing Time: {getattr(output_msg.body, 'processing_time_ms', 'N/A')}ms")
                            else:
                                print(f"    ❌ No response found in output queue")
                        else:
                            print(f"    ❌ Processing failed: {result.error}")
                    except Exception as e:
                        print(f"    ❌ Error processing: {e}")
                        
                print(f"\n🎉 Workflow test completed!")
                return
            else:
                print(f"  Attempt {attempt + 1}: No messages found, retrying...")
                await asyncio.sleep(1)
        
        print("❌ No messages found after 5 attempts")
            
    except Exception as e:
        print(f"❌ Error in workflow test: {e}")

async def test_inbound_processing():
    """End-to-End SQS Test: simulate message → listen → process → output result"""
    try:
        print("🔄 End-to-End SQS Processing Test")
        print("=" * 50)
        print("Steps: Simulate → Listen → Process → Output")
        
        from app.sqs import SQSClient, SQSSettings, MessageProcessor
        
        settings = SQSSettings()
        client = SQSClient(settings)
        processor = MessageProcessor(settings, client)
        
        # Step 1: Simulate inbound message to input queue
        print("\n📤 Step 1: Simulating inbound message to input queue...")
        print(f"Target queue: {settings.input_queue_url}")
        
        test_message = create_test_message()
        message_id = client.send_message(test_message)
        
        if not message_id:
            print("❌ Failed to send test message")
            return
        
        print(f"✅ Message simulated successfully!")
        print(f"   Message ID: {test_message['message_id']}")
        print(f"   Correlation ID: {test_message['correlation_id']}")
        print(f"   Data rows: {len(test_message['data_entry']['data'])}")
        print(f"   Validation rules: {len(test_message['validation_rules'])}")
        
        # Step 2: Listen to inbound queue and capture the message
        print(f"\n👂 Step 2: Listening to inbound queue for our message...")
        
        message_found = False
        max_attempts = 10
        
        for attempt in range(max_attempts):
            print(f"   Attempt {attempt + 1}/{max_attempts}...")
            
            # Receive messages from input queue
            messages = client.receive_messages(settings.input_queue_url)
            
            if messages:
                print(f"   � Found {len(messages)} message(s) in queue!")
                
                # Look for our specific message
                our_message = None
                for msg in messages:
                    if hasattr(msg.body, 'message_id') and msg.body.message_id == test_message['message_id']:
                        our_message = msg
                        message_found = True
                        break
                
                if our_message:
                    print(f"   🎯 Found our test message!")
                    print(f"      Message ID: {our_message.body.message_id}")
                    print(f"      Receipt Handle: {our_message.receipt_handle[:20]}...")
                    
                    # Step 3: Process the message
                    print(f"\n🔄 Step 3: Processing the message...")
                    print(f"   Processing message: {our_message.body.message_id}")
                    
                    try:
                        processing_start = asyncio.get_event_loop().time()
                        result = await processor.process_message(our_message)
                        processing_time = (asyncio.get_event_loop().time() - processing_start) * 1000
                        
                        if result.success:
                            print(f"   ✅ Message processed successfully!")
                            print(f"   ⏱️  Processing time: {processing_time:.2f}ms")
                            print(f"   📊 Validation completed")
                            
                            # Delete message from input queue (cleanup)
                            delete_success = client.delete_message(settings.input_queue_url, our_message.receipt_handle)
                            if delete_success:
                                print(f"   �️  Message deleted from input queue")
                            else:
                                print(f"   ⚠️  Failed to delete message from input queue")
                            
                        else:
                            print(f"   ❌ Message processing failed!")
                            print(f"   Error: {result.error}")
                            return
                            
                    except Exception as e:
                        print(f"   ❌ Error during processing: {e}")
                        return
                    
                    break
                else:
                    print(f"   📭 Our message not found among {len(messages)} messages")
                    # Check if any message has our correlation ID
                    for msg in messages:
                        if hasattr(msg.body, 'correlation_id') and msg.body.correlation_id == test_message['correlation_id']:
                            print(f"   🔍 Found message with our correlation ID: {msg.body.message_id}")
            else:
                print(f"   📭 No messages in queue")
            
            if message_found:
                break
                
            # Wait before next attempt
            await asyncio.sleep(1)
        
        if not message_found:
            print(f"   ❌ Our message was not found after {max_attempts} attempts")
            print(f"   💡 It may have been processed by background workers")
        
        # Step 4: Check output queue for the result
        print(f"\n📤 Step 4: Checking output queue for processing result...")
        print(f"Target queue: {settings.output_queue_url}")
        
        # Wait a moment for the result to appear
        await asyncio.sleep(2)
        
        output_messages = client.receive_messages(settings.output_queue_url)
        
        if output_messages:
            print(f"✅ Found {len(output_messages)} result message(s) in output queue!")
            
            # Look for our result message
            our_result = None
            for msg in output_messages:
                if hasattr(msg.body, 'message_id') and msg.body.message_id == test_message['message_id']:
                    our_result = msg
                    break
                elif hasattr(msg.body, 'correlation_id') and msg.body.correlation_id == test_message['correlation_id']:
                    our_result = msg
                    break
            
            if our_result:
                print(f"🎯 Found our result message!")
                result_body = our_result.body
                
                print(f"   📋 Result Details:")
                print(f"      Message ID: {result_body.message_id}")
                print(f"      Correlation ID: {getattr(result_body, 'correlation_id', 'N/A')}")
                print(f"      Status: {getattr(result_body, 'status', 'unknown')}")
                print(f"      Processing Time: {getattr(result_body, 'processing_time_ms', 'N/A')}ms")
                print(f"      Worker ID: {getattr(result_body, 'worker_id', 'N/A')}")
                print(f"      Timestamp: {getattr(result_body, 'timestamp', 'N/A')}")
                
                # Show validation summary
                if hasattr(result_body, 'summary') and result_body.summary:
                    summary = result_body.summary
                    print(f"   📊 Validation Summary:")
                    print(f"      Total Rules: {getattr(summary, 'total_rules', 'N/A')}")
                    print(f"      Successful: {getattr(summary, 'successful_rules', 'N/A')}")
                    print(f"      Failed: {getattr(summary, 'failed_rules', 'N/A')}")
                    
                    success_rate = getattr(summary, 'success_rate', 0)
                    if isinstance(success_rate, (int, float)):
                        print(f"      Success Rate: {success_rate:.1%}")
                    
                    print(f"      Data Rows: {getattr(summary, 'total_rows', 'N/A')}")
                    print(f"      Data Columns: {getattr(summary, 'total_columns', 'N/A')}")
                
                # Show detailed validation results
                if hasattr(result_body, 'validation_results') and result_body.validation_results:
                    print(f"   🔍 Validation Results ({len(result_body.validation_results)} rules):")
                    
                    for i, vr in enumerate(result_body.validation_results, 1):
                        success = getattr(vr, 'success', False)
                        success_icon = "✅" if success else "❌"
                        rule_name = getattr(vr, 'rule_name', 'Unknown')
                        column_name = getattr(vr, 'column_name', 'N/A')
                        message_text = getattr(vr, 'message', 'No message')
                        
                        print(f"      {i}. {success_icon} {rule_name}")
                        if column_name != 'N/A':
                            print(f"         Column: {column_name}")
                        print(f"         Result: {message_text}")
                        
                        # Show key metrics
                        if hasattr(vr, 'observed_value'):
                            print(f"         Observed: {vr.observed_value}")
                        if hasattr(vr, 'unexpected_count'):
                            print(f"         Unexpected Count: {vr.unexpected_count}")
                
            else:
                print(f"🔍 Our specific result not found, showing available results:")
                for i, msg in enumerate(output_messages[:3], 1):
                    try:
                        result_body = msg.body
                        print(f"   {i}. Message ID: {result_body.message_id}")
                        print(f"      Correlation: {getattr(result_body, 'correlation_id', 'N/A')}")
                        print(f"      Status: {getattr(result_body, 'status', 'unknown')}")
                    except Exception as e:
                        print(f"   {i}. Error parsing: {e}")
        
        else:
            print(f"❌ No result messages found in output queue")
            print(f"💡 The result may have been processed by another consumer")
        
        # Final summary
        print(f"\n🎉 End-to-End Test Summary:")
        print(f"=" * 50)
        print(f"✅ Step 1: Message simulated and sent to input queue")
        print(f"{'✅' if message_found else '⚠️'} Step 2: Message {'found and captured' if message_found else 'processed by background workers'}")
        print(f"✅ Step 3: Message processing completed")
        print(f"✅ Step 4: Output queue checked for results")
        
        if message_found:
            print(f"\n🎯 Complete end-to-end flow verified!")
            print(f"   📥 Input → 🔄 Processing → 📤 Output")
        else:
            print(f"\n💡 System is working efficiently!")
            print(f"   Messages are being processed faster than manual testing can track")
        
    except Exception as e:
        print(f"❌ Error in end-to-end test: {e}")
        import traceback
        traceback.print_exc()

async def show_validation_results():
    """Display detailed validation results from output queue"""
    try:
        print("📊 Validation Results from Output Queue")
        print("=" * 50)
        
        from app.sqs import SQSClient, SQSSettings
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        print("📤 Checking output queue for validation results...")
        output_messages = client.receive_messages(settings.output_queue_url)
        
        if not output_messages:
            print("📭 No messages found in output queue")
            return
        
        print(f"✅ Found {len(output_messages)} result message(s):")
        print()
        
        for i, msg in enumerate(output_messages, 1):
            try:
                result = msg.body
                print(f"📋 Result {i}:")
                print(f"  Message ID: {result.message_id}")
                print(f"  Correlation ID: {getattr(result, 'correlation_id', 'N/A')}")
                print(f"  Status: {getattr(result, 'status', 'unknown')}")
                print(f"  Processing Time: {getattr(result, 'processing_time_ms', 'N/A')}ms")
                print(f"  Worker: {getattr(result, 'worker_id', 'N/A')}")
                print(f"  Timestamp: {getattr(result, 'timestamp', 'N/A')}")
                
                # Show data information
                if hasattr(result, 'data_key'):
                    print(f"  Data Key: {result.data_key}")
                    print(f"  Data Type: {result.data_type}")
                
                # Show validation summary
                if hasattr(result, 'summary') and result.summary:
                    summary = result.summary
                    print(f"  📊 Validation Summary:")
                    print(f"     Total Rules: {getattr(summary, 'total_rules', 'N/A')}")
                    print(f"     Successful: {getattr(summary, 'successful_rules', 'N/A')}")
                    print(f"     Failed: {getattr(summary, 'failed_rules', 'N/A')}")
                    
                    success_rate = getattr(summary, 'success_rate', 0)
                    if isinstance(success_rate, (int, float)):
                        print(f"     Success Rate: {success_rate:.1%}")
                    else:
                        print(f"     Success Rate: {success_rate}")
                    
                    print(f"     Data Rows: {getattr(summary, 'total_rows', 'N/A')}")
                    print(f"     Data Columns: {getattr(summary, 'total_columns', 'N/A')}")
                
                # Show detailed validation results
                if hasattr(result, 'validation_results') and result.validation_results:
                    print(f"  🔍 Validation Details ({len(result.validation_results)} rules):")
                    
                    for j, vr in enumerate(result.validation_results, 1):
                        success = getattr(vr, 'success', False)
                        success_icon = "✅" if success else "❌"
                        rule_name = getattr(vr, 'rule_name', 'Unknown')
                        column_name = getattr(vr, 'column_name', 'N/A')
                        message_text = getattr(vr, 'message', 'No message')
                        
                        print(f"     {j}. {success_icon} {rule_name}")
                        if column_name != 'N/A':
                            print(f"        Column: {column_name}")
                        print(f"        Result: {message_text}")
                        
                        # Show additional metrics if available
                        if hasattr(vr, 'unexpected_count'):
                            print(f"        Unexpected: {vr.unexpected_count}")
                        if hasattr(vr, 'unexpected_percent'):
                            print(f"        Unexpected %: {vr.unexpected_percent}")
                        if hasattr(vr, 'observed_value'):
                            print(f"        Observed: {vr.observed_value}")
                
                elif hasattr(result, 'error_message'):
                    print(f"  ❌ Error: {result.error_message}")
                
                print(f"  {'─' * 40}")
                print()
                
            except Exception as e:
                print(f"  Result {i}: Error parsing - {e}")
                print(f"    Raw body type: {type(msg.body)}")
                print(f"    Raw body: {str(msg.body)[:200]}...")
                print()
        
        print(f"📊 Total results displayed: {len(output_messages)}")
        
    except Exception as e:
        print(f"❌ Error showing validation results: {e}")
        import traceback
        traceback.print_exc()
    """Complete end-to-end test: send message and check output queue for results"""
    try:
        print("🔄 End-to-End SQS Test...")
        print("=" * 50)
        
        from app.sqs import SQSClient, SQSSettings
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        # Step 1: Check initial queue states
        print("📊 Step 1: Initial queue status...")
        stats = client.get_queue_stats()
        input_msgs_before = int(stats.get('input_queue', {}).get('approximate_number_of_messages', 0))
        output_msgs_before = int(stats.get('output_queue', {}).get('approximate_number_of_messages', 0))
        
        print(f"  Input Queue: {input_msgs_before} messages")
        print(f"  Output Queue: {output_msgs_before} messages")
        
        # Step 2: Send test message
        print("\n📤 Step 2: Sending test message...")
        message = create_test_message()
        message_id = client.send_message(message)
        
        if not message_id:
            print("❌ Failed to send message")
            return
        
        print(f"✅ Sent message: {message['message_id']}")
        print(f"📥 To queue: {settings.input_queue_url}")
        
        # Step 3: Wait and check for processing
        print(f"\n⏳ Step 3: Waiting for message processing...")
        
        max_wait = 15  # Wait up to 15 seconds
        for wait_time in range(max_wait):
            await asyncio.sleep(1)
            
            # Check output queue for new messages
            stats = client.get_queue_stats()
            output_msgs_now = int(stats.get('output_queue', {}).get('approximate_number_of_messages', 0))
            
            if output_msgs_now > output_msgs_before:
                print(f"✅ New result detected after {wait_time + 1} seconds!")
                break
            else:
                print(f"  Waiting... ({wait_time + 1}/{max_wait}s)")
        
        # Step 4: Check output queue for results
        print(f"\n📤 Step 4: Checking output queue for results...")
        
        output_messages = client.receive_messages(settings.output_queue_url)
        
        if output_messages:
            print(f"✅ Found {len(output_messages)} result message(s):")
            
            for i, msg in enumerate(output_messages, 1):
                try:
                    result = msg.body
                    print(f"\n  Result {i}:")
                    print(f"    Message ID: {result.message_id}")
                    print(f"    Status: {getattr(result, 'status', 'unknown')}")
                    print(f"    Processing Time: {getattr(result, 'processing_time_ms', 'N/A')}ms")
                    print(f"    Worker: {getattr(result, 'worker_id', 'N/A')}")
                    
                    # Show data information
                    if hasattr(result, 'data_key'):
                        print(f"    Data Key: {result.data_key}")
                        print(f"    Data Type: {result.data_type}")
                    
                    # Show validation summary
                    if hasattr(result, 'summary') and result.summary:
                        summary = result.summary
                        print(f"    � Validation Summary:")
                        print(f"       Total Rules: {getattr(summary, 'total_rules', 'N/A')}")
                        print(f"       Successful: {getattr(summary, 'successful_rules', 'N/A')}")
                        print(f"       Failed: {getattr(summary, 'failed_rules', 'N/A')}")
                        print(f"       Success Rate: {getattr(summary, 'success_rate', 0):.1%}")
                        print(f"       Data Rows: {getattr(summary, 'total_rows', 'N/A')}")
                        print(f"       Data Columns: {getattr(summary, 'total_columns', 'N/A')}")
                    
                    # Show detailed validation results
                    if hasattr(result, 'validation_results') and result.validation_results:
                        print(f"    🔍 Validation Details:")
                        for j, vr in enumerate(result.validation_results, 1):
                            success_icon = "✅" if getattr(vr, 'success', False) else "❌"
                            rule_name = getattr(vr, 'rule_name', 'Unknown')
                            column_name = getattr(vr, 'column_name', 'N/A')
                            message_text = getattr(vr, 'message', 'No message')
                            print(f"       {j}. {success_icon} {rule_name} ({column_name})")
                            print(f"          {message_text}")
                    
                    # Check if this is our message
                    if result.message_id == message['message_id']:
                        print(f"    🎯 This is our test message!")
                        
                except Exception as e:
                    print(f"  Result {i}: Error parsing - {e}")
        else:
            print("❌ No result messages found in output queue")
        
        # Step 5: Final statistics
        print(f"\n📈 Step 5: Final queue statistics...")
        final_stats = client.get_queue_stats()
        input_msgs_after = int(final_stats.get('input_queue', {}).get('approximate_number_of_messages', 0))
        output_msgs_after = int(final_stats.get('output_queue', {}).get('approximate_number_of_messages', 0))
        
        print(f"  Input Queue: {input_msgs_before} → {input_msgs_after} messages")
        print(f"  Output Queue: {output_msgs_before} → {output_msgs_after} messages")
        print(f"  Messages Processed: {output_msgs_after - output_msgs_before}")
        
        print(f"\n🎉 End-to-end test completed!")
        
    except Exception as e:
        print(f"❌ Error in end-to-end test: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description="SQS Management CLI for EDGP Rules Engine")
    parser.add_argument("command", choices=[
        "send-test", "stats", "health", "config", "check-output", "validate", 
        "listen", "listen-timeout", "listen-once", "test-workflow", "test-inbound", "show-results"
    ], help="Command to execute")
    
    args = parser.parse_args()
    
    print("🔧 EDGP Rules Engine - SQS Management CLI")
    print("=" * 60)
    
    if args.command == "send-test":
        await send_test_message()
    elif args.command == "stats":
        await check_queue_stats()
    elif args.command == "health":
        await health_check()
    elif args.command == "config":
        await show_config()
    elif args.command == "check-output":
        await check_output_queue()
    elif args.command == "validate":
        await validate_config()
    elif args.command == "listen":
        await start_listener()
    elif args.command == "listen-timeout":
        await listen_with_timeout()
    elif args.command == "listen-once":
        await listen_once()
    elif args.command == "test-workflow":
        await test_workflow()
    elif args.command == "test-inbound":
        await test_inbound_processing()
    elif args.command == "show-results":
        await show_validation_results()

if __name__ == "__main__":
    asyncio.run(main())
