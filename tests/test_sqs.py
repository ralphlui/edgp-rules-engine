"""
Tests for SQS functionality.
"""
import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

from app.models.sqs_models import (
    SQSValidationRequest, 
    SQSValidationResponse, 
    ValidationRule,
    MessageStatus,
    SQSMessageWrapper
)
from app.sqs.config import SQSSettings

def test_validation_rule_model():
    """Test ValidationRule model"""
    rule = ValidationRule(
        rule_name="expect_column_values_to_be_between",
        column_name="age",
        value={"min_value": 18, "max_value": 65}
    )
    
    assert rule.rule_name == "expect_column_values_to_be_between"
    assert rule.column_name == "age"
    assert rule.value["min_value"] == 18
    assert rule.value["max_value"] == 65

def test_sqs_validation_request_model():
    """Test SQSValidationRequest model"""
    rules = [
        ValidationRule(
            rule_name="expect_column_to_exist",
            column_name="name"
        ),
        ValidationRule(
            rule_name="expect_column_values_to_be_between",
            column_name="age",
            value={"min_value": 0, "max_value": 120}
        )
    ]
    
    data = [
        {"name": "John", "age": 25},
        {"name": "Jane", "age": 30}
    ]
    
    request = SQSValidationRequest(
        message_id="test-msg-123",
        data=data,
        rules=rules,
        correlation_id="corr-123",
        source="test-system"
    )
    
    assert request.message_id == "test-msg-123"
    assert len(request.data) == 2
    assert len(request.rules) == 2
    assert request.correlation_id == "corr-123"
    assert request.source == "test-system"
    assert request.priority == 5  # default
    assert request.retry_count == 0  # default

def test_sqs_validation_request_validation():
    """Test SQSValidationRequest validation"""
    
    # Test empty data validation
    with pytest.raises(ValueError, match="Data cannot be empty"):
        SQSValidationRequest(
            message_id="test",
            data=[],
            rules=[ValidationRule(rule_name="test", column_name="test")]
        )
    
    # Test empty rules validation
    with pytest.raises(ValueError, match="Rules cannot be empty"):
        SQSValidationRequest(
            message_id="test",
            data=[{"test": "data"}],
            rules=[]
        )

def test_sqs_validation_response_model():
    """Test SQSValidationResponse model"""
    response = SQSValidationResponse(
        message_id="test-msg-123",
        processing_time_ms=1500,
        status=MessageStatus.SUCCESS,
        worker_id="worker-1",
        validation_results=[
            {"rule": "test_rule", "success": True, "message": "Passed"}
        ],
        summary={"total": 1, "passed": 1, "failed": 0},
        total_rules=1,
        successful_rules=1,
        failed_rules=0
    )
    
    assert response.message_id == "test-msg-123"
    assert response.processing_time_ms == 1500
    assert response.status == MessageStatus.SUCCESS
    assert response.worker_id == "worker-1"
    assert len(response.validation_results) == 1
    assert response.total_rules == 1

@patch('boto3.Session')
def test_sqs_client_init(mock_session):
    """Test SQS client initialization"""
    from app.sqs.client import SQSClient
    
    mock_client = Mock()
    mock_session.return_value.client.return_value = mock_client
    
    settings = SQSSettings(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        aws_region="us-east-1"
    )
    
    client = SQSClient(settings)
    
    mock_session.assert_called_once_with(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        region_name="us-east-1"
    )
    assert client.sqs == mock_client

@patch('boto3.Session')
def test_sqs_client_receive_messages(mock_session):
    """Test SQS client message receiving"""
    from app.sqs.client import SQSClient
    
    # Mock SQS response
    mock_sqs = Mock()
    mock_session.return_value.client.return_value = mock_sqs
    
    mock_sqs.receive_message.return_value = {
        'Messages': [
            {
                'MessageId': 'msg-123',
                'ReceiptHandle': 'receipt-123',
                'Body': json.dumps({
                    'message_id': 'test-msg-123',
                    'data': [{'name': 'John', 'age': 25}],
                    'rules': [{'rule_name': 'expect_column_to_exist', 'column_name': 'name'}]
                }),
                'Attributes': {'SentTimestamp': '1234567890'}
            }
        ]
    }
    
    settings = SQSSettings(input_queue_url="test-queue-url")
    client = SQSClient(settings)
    
    messages = client.receive_messages()
    
    assert len(messages) == 1
    assert messages[0].message_id == 'msg-123'
    assert messages[0].body.message_id == 'test-msg-123'
    assert len(messages[0].body.data) == 1
    assert len(messages[0].body.rules) == 1

@patch('app.sqs.client.SQSClient')
@pytest.mark.asyncio
async def test_message_processor(mock_sqs_client):
    """Test message processor"""
    from app.sqs.processor import MessageProcessor
    from app.sqs.config import SQSSettings
    
    settings = SQSSettings()
    mock_client = Mock()
    processor = MessageProcessor(settings, mock_client)
    
    # Create test message
    request = SQSValidationRequest(
        message_id="test-msg-123",
        data=[{"name": "John", "age": 25}],
        rules=[ValidationRule(rule_name="expect_column_to_exist", column_name="name")]
    )
    
    message = SQSMessageWrapper(
        receipt_handle="receipt-123",
        message_id="sqs-msg-123",
        body=request
    )
    
    # Mock the validation function
    with patch('app.sqs.processor.api_validate_data') as mock_validate:
        mock_validate.return_value = {
            "results": [{"success": True, "message": "Column exists"}],
            "summary": {"total": 1, "passed": 1, "failed": 0}
        }
        
        result = await processor.process_message(message)
        
        assert result.success is True
        assert result.message_id == "test-msg-123"
        assert result.should_delete is True
        assert result.response.status == MessageStatus.SUCCESS

def test_sqs_settings():
    """Test SQS settings with environment variables"""
    settings = SQSSettings(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        aws_region="us-west-2",
        worker_count=2,
        max_retries=5
    )
    
    assert settings.aws_access_key_id == "test_key"
    assert settings.aws_secret_access_key == "test_secret"
    assert settings.aws_region == "us-west-2"
    assert settings.worker_count == 2
    assert settings.max_retries == 5
    assert settings.max_messages_per_poll == 10  # default
    assert settings.auto_start_workers is False  # default

@patch('app.sqs.manager.SQSClient')
@patch('app.sqs.manager.MessageProcessor')
@pytest.mark.asyncio
async def test_sqs_manager(mock_processor_class, mock_client_class):
    """Test SQS manager"""
    from app.sqs.manager import SQSManager
    
    settings = SQSSettings(worker_count=2)
    manager = SQSManager(settings)
    
    # Mock workers
    mock_worker1 = Mock()
    mock_worker1.run_worker_loop = AsyncMock()
    mock_worker1.get_stats.return_value = {"worker_id": "worker-1", "processed_count": 10}
    
    mock_worker2 = Mock()
    mock_worker2.run_worker_loop = AsyncMock()
    mock_worker2.get_stats.return_value = {"worker_id": "worker-2", "processed_count": 15}
    
    mock_processor_class.side_effect = [mock_worker1, mock_worker2]
    
    # Test starting manager
    start_task = asyncio.create_task(manager.start())
    
    # Allow some processing time
    await asyncio.sleep(0.1)
    
    # Check workers were created
    assert len(manager.workers) == 2
    assert manager.is_running is True
    
    # Test status
    status = manager.get_status()
    assert status["worker_count"] == 2
    assert status["is_running"] is True
    
    # Stop manager
    await manager.stop()
    start_task.cancel()
    
    assert manager.is_running is False

def test_message_status_enum():
    """Test MessageStatus enum"""
    assert MessageStatus.PENDING == "pending"
    assert MessageStatus.PROCESSING == "processing"
    assert MessageStatus.SUCCESS == "success"
    assert MessageStatus.FAILED == "failed"
    assert MessageStatus.RETRY == "retry"
    assert MessageStatus.DLQ == "dlq"

if __name__ == "__main__":
    pytest.main([__file__])
