#!/usr/bin/env python3
"""
SQS Management CLI tool for the EDGP Rules Engine.
This script helps manage SQS queues and send test messages.
"""
import asyncio
import json
import sys
import argparse
from datetime import datetime
from typing import Dict, Any

# Add the parent directory to the path for imports
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_test_message() -> Dict[str, Any]:
    """Create a test validation message"""
    return {
        "message_id": f"test-msg-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "correlation_id": f"corr-{datetime.now().timestamp()}",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "sqs_cli_tool",
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
                "value": {"min_value": 18, "max_value": 65}
            },
            {
                "rule_name": "expect_column_values_to_be_valid_email",
                "column_name": "email",
                "value": {}
            },
            {
                "rule_name": "expect_column_values_to_be_between",
                "column_name": "salary", 
                "value": {"min_value": 30000, "max_value": 100000}
            }
        ],
        "priority": 5,
        "max_retries": 3,
        "callback_url": None
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
        print(f"AWS Region: {settings.aws_region}")
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
        
    except Exception as e:
        print(f"❌ Error showing config: {e}")

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

async def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description="SQS Management CLI for EDGP Rules Engine")
    parser.add_argument("command", choices=[
        "send-test", "stats", "health", "config", "check-output"
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
    elif args.command == "check-output":
        await check_output_queue()

if __name__ == "__main__":
    asyncio.run(main())
