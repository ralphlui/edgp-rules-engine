#!/usr/bin/env python3
"""
Application launcher with configurable SQS queue URLs.

Usage:
    python launch_with_queues.py --account-id 123456789012 --region ap-southeast-1 --env dev
    python launch_with_queues.py --account-id 999999999999 --region us-east-1 --env prod
"""
import argparse
import os
import sys
from pathlib import Path

def setup_environment(account_id: str, region: str, environment: str):
    """
    Set up environment variables for SQS configuration.
    
    Args:
        account_id: AWS account ID
        region: AWS region
        environment: Environment name (dev, prod, test)
    """
    
    # Set AWS configuration
    os.environ['AWS_ACCOUNT_ID'] = account_id
    os.environ['SQS_AWS_REGION'] = region
    
    # Set queue templates based on environment
    queue_configs = {
        'dev': {
            'SQS_INPUT_QUEUE_URL': 'INPUT_QUEUE/dev_validation_input',
            'SQS_OUTPUT_QUEUE_URL': 'OUTPUT_QUEUE/dev_validation_output',
            'SQS_DLQ_URL': 'DLQ_QUEUE/dev_validation_dlq'
        },
        'prod': {
            'SQS_INPUT_QUEUE_URL': 'INPUT_QUEUE/prod_validation_input',
            'SQS_OUTPUT_QUEUE_URL': 'OUTPUT_QUEUE/prod_validation_output',
            'SQS_DLQ_URL': 'DLQ_QUEUE/prod_validation_dlq'
        },
        'test': {
            'SQS_INPUT_QUEUE_URL': 'INPUT_QUEUE/test_validation_input',
            'SQS_OUTPUT_QUEUE_URL': 'OUTPUT_QUEUE/test_validation_output',
            'SQS_DLQ_URL': 'DLQ_QUEUE/test_validation_dlq'
        }
    }
    
    if environment in queue_configs:
        for key, value in queue_configs[environment].items():
            os.environ[key] = value
        
        print(f"🔧 Configured {environment} environment:")
        print(f"   AWS Account ID: {account_id}")
        print(f"   AWS Region: {region}")
        
        # Add parent directory to path for imports
        project_root = Path(__file__).parent.parent
        sys.path.insert(0, str(project_root))
        
        # Show resolved URLs
        from app.sqs.config import SQSSettings
        sqs_settings = SQSSettings()
        
        print(f"   Input Queue:  {sqs_settings.get_input_queue_url()}")
        print(f"   Output Queue: {sqs_settings.get_output_queue_url()}")
        print(f"   DLQ:          {sqs_settings.get_dlq_url()}")
        
    else:
        print(f"❌ Unknown environment: {environment}")
        print("   Available environments: dev, prod, test")
        sys.exit(1)

def main():
    """
    Main launcher function.
    """
    parser = argparse.ArgumentParser(description="Launch EDGP Rules Engine with configurable SQS queues")
    
    parser.add_argument(
        '--account-id', 
        required=True,
        help='AWS Account ID (e.g., 123456789012)'
    )
    
    parser.add_argument(
        '--region',
        default='ap-southeast-1',
        help='AWS Region (default: ap-southeast-1)'
    )
    
    parser.add_argument(
        '--env', '--environment',
        choices=['dev', 'prod', 'test'],
        default='dev',
        help='Environment configuration (default: dev)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=8008,
        help='Application port (default: 8008)'
    )
    
    parser.add_argument(
        '--host',
        default='localhost',
        help='Application host (default: localhost)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of SQS workers (default: 4)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show configuration without starting the application'
    )
    
    args = parser.parse_args()
    
    print("🚀 EDGP Rules Engine Launcher")
    print("=" * 40)
    
    # Set up environment
    setup_environment(args.account_id, args.region, args.env)
    
    # Set additional configuration
    os.environ['PORT'] = str(args.port)
    os.environ['HOST'] = args.host
    os.environ['SQS_WORKER_COUNT'] = str(args.workers)
    
    if args.dry_run:
        print("\n✅ Dry run completed - configuration ready!")
        print("\nTo start the application:")
        print(f"   python -m app.main")
        return
    
    print(f"\n🌟 Starting application on {args.host}:{args.port}")
    print("   Press Ctrl+C to stop")
    
    try:
        # Add parent directory to path for imports  
        project_root = Path(__file__).parent.parent
        sys.path.insert(0, str(project_root))
        
        # Import and run the application
        import uvicorn
        from app.main import app
        
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            reload=False,
            log_level="info"
        )
        
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except ImportError:
        print("\n📦 Installing missing dependencies...")
        os.system("pip install uvicorn")
        print("Please run the command again")
    except Exception as e:
        print(f"\n❌ Error starting application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
