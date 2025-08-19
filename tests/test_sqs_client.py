"""
Comprehensive unit tests for app.sqs.client module
Tests SQS client functionality for AWS SQS operations
"""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError

from app.sqs.client import SQSClient
from app.sqs.config import SQSSettings
from app.models.sqs_models import (
    SQSMessageWrapper,
    SQSValidationRequest,
    SQSValidationResponse,
    DataEntry,
    ValidationRule
)


class TestSQSClientInitialization:
    """Test SQS client initialization"""

    @patch('app.sqs.client.boto3.Session')
    def test_sqs_client_init_success(self, mock_session):
        """Test successful SQS client initialization"""
        mock_sqs_client = MagicMock()
        mock_session.return_value.client.return_value = mock_sqs_client
        
        settings = SQSSettings()
        client = SQSClient(settings)
        
        assert client.settings == settings
        assert client.sqs == mock_sqs_client
        mock_session.assert_called_once_with(
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )

    @patch('app.sqs.client.boto3.Session')
    def test_sqs_client_init_failure(self, mock_session):
        """Test SQS client initialization failure"""
        mock_session.side_effect = Exception("Connection failed")
        
        settings = SQSSettings()
        
        with pytest.raises(Exception, match="Connection failed"):
            SQSClient(settings)

    @patch('app.sqs.client.boto3.Session')
    @patch('app.sqs.client.logger')
    def test_sqs_client_init_logging(self, mock_logger, mock_session):
        """Test SQS client initialization with logging"""
        mock_sqs_client = MagicMock()
        mock_session.return_value.client.return_value = mock_sqs_client
        
        settings = SQSSettings()
        settings.aws_region = "us-west-2"
        
        SQSClient(settings)
        
        mock_logger.info.assert_called_with("SQS client connected to region: us-west-2")

    @patch('app.sqs.client.boto3.Session')
    @patch('app.sqs.client.logger')
    def test_sqs_client_init_error_logging(self, mock_logger, mock_session):
        """Test SQS client initialization error logging"""
        error_msg = "AWS credentials error"
        mock_session.side_effect = Exception(error_msg)
        
        settings = SQSSettings()
        
        with pytest.raises(Exception):
            SQSClient(settings)
        
        mock_logger.error.assert_called_with(f"Failed to connect to SQS: {error_msg}")


class TestSQSClientReceiveMessages:
    """Test SQS message receiving functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_receive_messages_success(self):
        """Test successful message receiving"""
        # Mock SQS response with proper DataEntry structure
        mock_message_body = {
            "data_entry": {
                "data_type": "tabular",
                "domain_name": "test-domain",
                "file_id": "file-123",
                "policy_id": "policy-456",
                "data": {"records": [{"col1": "value1"}]},
                "validation_rules": [
                    {
                        "rule_name": "expect_column_to_exist",
                        "column_name": "col1",
                        "rule_description": "Test rule",
                        "severity": "error"
                    }
                ]
            }
        }
        
        self.client.sqs.receive_message.return_value = {
            'Messages': [
                {
                    'MessageId': 'msg-123',
                    'ReceiptHandle': 'receipt-123',
                    'Body': json.dumps(mock_message_body),
                    'Attributes': {'ApproximateReceiveCount': '1'}
                }
            ]
        }
        
        messages = self.client.receive_messages()
        
        assert len(messages) == 1
        assert messages[0].message_id == 'msg-123'
        assert messages[0].receipt_handle == 'receipt-123'
        assert messages[0].body.data_entry.file_id == "file-123"

    def test_receive_messages_no_messages(self):
        """Test receiving when no messages available"""
        self.client.sqs.receive_message.return_value = {}
        
        messages = self.client.receive_messages()
        
        assert len(messages) == 0

    def test_receive_messages_with_custom_queue(self):
        """Test receiving messages from custom queue URL"""
        custom_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/custom-queue"
        self.client.sqs.receive_message.return_value = {'Messages': []}
        
        self.client.receive_messages(queue_url=custom_queue_url)
        
        self.client.sqs.receive_message.assert_called_once()
        call_args = self.client.sqs.receive_message.call_args[1]
        assert call_args['QueueUrl'] == custom_queue_url

    def test_receive_messages_client_error(self):
        """Test receiving messages with ClientError"""
        self.client.sqs.receive_message.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            'ReceiveMessage'
        )
        
        messages = self.client.receive_messages()
        
        assert messages == []

    def test_receive_messages_parse_error(self):
        """Test receiving messages with JSON parse error"""
        self.client.sqs.receive_message.return_value = {
            'Messages': [
                {
                    'MessageId': 'msg-123',
                    'ReceiptHandle': 'receipt-123',
                    'Body': '{"invalid": json}',  # Invalid JSON
                    'Attributes': {}
                }
            ]
        }
        
        with patch('app.sqs.client.logger') as mock_logger:
            messages = self.client.receive_messages()
            
            assert len(messages) == 0
            mock_logger.error.assert_called()

    def test_receive_messages_validation_request_error(self):
        """Test receiving messages with SQSValidationRequest validation error"""
        self.client.sqs.receive_message.return_value = {
            'Messages': [
                {
                    'MessageId': 'msg-123',
                    'ReceiptHandle': 'receipt-123',
                    'Body': json.dumps({"incomplete": "data"}),  # Missing required fields
                    'Attributes': {}
                }
            ]
        }
        
        with patch('app.sqs.client.logger') as mock_logger:
            messages = self.client.receive_messages()
            
            assert len(messages) == 0
            mock_logger.error.assert_called()


class TestSQSClientDeleteMessage:
    """Test SQS message deletion functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_delete_message_success(self):
        """Test successful message deletion"""
        receipt_handle = "receipt-123"
        
        result = self.client.delete_message(receipt_handle)
        
        assert result == True
        self.client.sqs.delete_message.assert_called_once_with(
            QueueUrl=self.settings.get_input_queue_url(),
            ReceiptHandle=receipt_handle
        )

    def test_delete_message_with_custom_queue(self):
        """Test message deletion with custom queue URL"""
        receipt_handle = "receipt-123"
        custom_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/custom-queue"
        
        result = self.client.delete_message(receipt_handle, queue_url=custom_queue_url)
        
        assert result == True
        self.client.sqs.delete_message.assert_called_once_with(
            QueueUrl=custom_queue_url,
            ReceiptHandle=receipt_handle
        )

    def test_delete_message_client_error(self):
        """Test message deletion with ClientError"""
        self.client.sqs.delete_message.side_effect = ClientError(
            {'Error': {'Code': 'ReceiptHandleIsInvalid', 'Message': 'Invalid receipt handle'}},
            'DeleteMessage'
        )
        
        result = self.client.delete_message("invalid-receipt")
        
        assert result == False


class TestSQSClientSendMessage:
    """Test SQS message sending functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_send_message_success(self):
        """Test successful message sending"""
        self.client.sqs.send_message.return_value = {'MessageId': 'msg-123'}
        message_body = {"test": "data"}
        
        result = self.client.send_message(message_body)
        
        assert result == 'msg-123'
        self.client.sqs.send_message.assert_called_once_with(
            QueueUrl=self.settings.get_output_queue_url(),
            MessageBody=json.dumps(message_body, default=str)
        )

    def test_send_message_with_delay(self):
        """Test sending message with delay"""
        self.client.sqs.send_message.return_value = {'MessageId': 'msg-123'}
        message_body = {"test": "data"}
        delay_seconds = 30
        
        result = self.client.send_message(message_body, delay_seconds=delay_seconds)
        
        assert result == 'msg-123'
        call_args = self.client.sqs.send_message.call_args[1]
        assert call_args['DelaySeconds'] == delay_seconds

    def test_send_message_with_attributes(self):
        """Test sending message with attributes"""
        self.client.sqs.send_message.return_value = {'MessageId': 'msg-123'}
        message_body = {"test": "data"}
        message_attributes = {"source": {"StringValue": "test", "DataType": "String"}}
        
        result = self.client.send_message(message_body, message_attributes=message_attributes)
        
        assert result == 'msg-123'
        call_args = self.client.sqs.send_message.call_args[1]
        assert call_args['MessageAttributes'] == message_attributes

    def test_send_message_client_error(self):
        """Test sending message with ClientError"""
        self.client.sqs.send_message.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            'SendMessage'
        )
        
        result = self.client.send_message({"test": "data"})
        
        assert result == None


class TestSQSClientDLQ:
    """Test SQS Dead Letter Queue functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.settings.dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789012/dlq"
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_send_to_dlq_success(self):
        """Test successful DLQ sending"""
        self.client.sqs.send_message.return_value = {'MessageId': 'dlq-msg-123'}
        
        # Create mock message with proper DataEntry structure
        mock_data_entry = DataEntry(
            data_type="tabular",
            domain_name="test-domain",
            file_id="file-123",
            policy_id="policy-456",
            data={"records": [{"col1": "value1"}]},
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="col1",
                    rule_description="Test rule",
                    severity="error"
                )
            ]
        )
        
        mock_request = SQSValidationRequest(
            data_entry=mock_data_entry
        )
        
        mock_message = SQSMessageWrapper(
            receipt_handle="receipt-123",
            message_id="msg-123",
            body=mock_request,
            attributes={},
            attempts=3
        )
        
        result = self.client.send_to_dlq(mock_message, "Processing failed")
        
        assert result == True
        self.client.sqs.send_message.assert_called_once()

    def test_send_to_dlq_no_dlq_configured(self):
        """Test DLQ sending when DLQ URL not configured"""
        self.settings.dlq_url = None
        
        mock_message = MagicMock()
        mock_message.message_id = "test-123"
        
        with patch('app.sqs.client.logger') as mock_logger:
            result = self.client.send_to_dlq(mock_message, "Error")
            
            assert result == False
            mock_logger.warning.assert_called_with(
                "DLQ URL not configured, cannot send message to DLQ"
            )

    def test_send_to_dlq_send_failure(self):
        """Test DLQ sending when send_message fails"""
        self.client.send_message = Mock(return_value=None)
        
        mock_message = MagicMock()
        result = self.client.send_to_dlq(mock_message, "Error")
        
        assert result == False


class TestSQSClientMessageVisibility:
    """Test SQS message visibility functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_change_message_visibility_success(self):
        """Test successful message visibility change"""
        receipt_handle = "receipt-123"
        visibility_timeout = 60
        
        result = self.client.change_message_visibility(receipt_handle, visibility_timeout)
        
        assert result == True
        self.client.sqs.change_message_visibility.assert_called_once_with(
            QueueUrl=self.settings.get_input_queue_url(),
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout
        )

    def test_change_message_visibility_client_error(self):
        """Test message visibility change with ClientError"""
        self.client.sqs.change_message_visibility.side_effect = ClientError(
            {'Error': {'Code': 'ReceiptHandleIsInvalid', 'Message': 'Invalid receipt handle'}},
            'ChangeMessageVisibility'
        )
        
        result = self.client.change_message_visibility("invalid-receipt", 60)
        
        assert result == False


class TestSQSClientQueueAttributes:
    """Test SQS queue attributes functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_get_queue_attributes_success(self):
        """Test successful queue attributes retrieval"""
        mock_attributes = {
            'ApproximateNumberOfMessages': '5',
            'ApproximateNumberOfMessagesNotVisible': '2'
        }
        self.client.sqs.get_queue_attributes.return_value = {
            'Attributes': mock_attributes
        }
        
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        result = self.client.get_queue_attributes(queue_url)
        
        assert result == mock_attributes
        self.client.sqs.get_queue_attributes.assert_called_once_with(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )

    def test_get_queue_attributes_client_error(self):
        """Test queue attributes retrieval with ClientError"""
        self.client.sqs.get_queue_attributes.side_effect = ClientError(
            {'Error': {'Code': 'AWS.SimpleQueueService.NonExistentQueue', 'Message': 'Queue does not exist'}},
            'GetQueueAttributes'
        )
        
        result = self.client.get_queue_attributes("invalid-queue-url")
        
        assert result == None


class TestSQSClientQueueStats:
    """Test SQS queue statistics functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.settings.input_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/input"
        self.settings.output_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/output"
        self.settings.dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789012/dlq"
        
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_get_queue_stats_all_queues(self):
        """Test getting statistics for all queues"""
        self.client.get_queue_attributes = Mock(side_effect=[
            {'ApproximateNumberOfMessages': '5', 'ApproximateNumberOfMessagesNotVisible': '2', 'ApproximateNumberOfMessagesDelayed': '1'},
            {'ApproximateNumberOfMessages': '3', 'ApproximateNumberOfMessagesNotVisible': '1'},
            {'ApproximateNumberOfMessages': '0'}
        ])
        
        stats = self.client.get_queue_stats()
        
        assert 'input_queue' in stats
        assert 'output_queue' in stats
        assert 'dlq' in stats
        
        assert stats['input_queue']['approximate_number_of_messages'] == '5'
        assert stats['output_queue']['approximate_number_of_messages'] == '3'
        assert stats['dlq']['approximate_number_of_messages'] == '0'

    def test_get_queue_stats_missing_queues(self):
        """Test getting statistics when some queues are not configured"""
        self.settings.dlq_url = None
        self.client.get_queue_attributes = Mock(side_effect=[
            {'ApproximateNumberOfMessages': '5'},
            {'ApproximateNumberOfMessages': '3'}
        ])
        
        stats = self.client.get_queue_stats()
        
        assert 'input_queue' in stats
        assert 'output_queue' in stats
        assert 'dlq' not in stats

    def test_get_queue_stats_attribute_failure(self):
        """Test getting statistics when attribute retrieval fails"""
        self.client.get_queue_attributes = Mock(return_value=None)
        
        stats = self.client.get_queue_stats()
        
        assert stats == {}


class TestSQSClientHealthCheck:
    """Test SQS health check functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.settings.input_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/input"
        self.settings.output_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/output"
        self.settings.dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789012/dlq"
        
        with patch('app.sqs.client.boto3.Session'):
            self.client = SQSClient(self.settings)
        self.client.sqs = MagicMock()

    def test_health_check_all_healthy(self):
        """Test health check when all components are healthy"""
        self.client.sqs.list_queues.return_value = {}
        self.client.get_queue_attributes = Mock(return_value={'ApproximateNumberOfMessages': '0'})
        
        health = self.client.health_check()
        
        assert health['sqs_connection'] == True
        assert health['input_queue'] == True
        assert health['output_queue'] == True
        assert health['dlq'] == True
        assert 'timestamp' in health

    def test_health_check_connection_failure(self):
        """Test health check when SQS connection fails"""
        self.client.sqs.list_queues.side_effect = Exception("Connection failed")
        
        health = self.client.health_check()
        
        assert health['sqs_connection'] == False
        assert 'error' in health

    def test_health_check_queue_failures(self):
        """Test health check when some queues are unhealthy"""
        self.client.sqs.list_queues.return_value = {}
        self.client.get_queue_attributes = Mock(side_effect=[
            {'ApproximateNumberOfMessages': '0'},  # input queue healthy
            None,  # output queue unhealthy
            {'ApproximateNumberOfMessages': '0'}   # dlq healthy
        ])
        
        health = self.client.health_check()
        
        assert health['sqs_connection'] == True
        assert health['input_queue'] == True
        assert health['output_queue'] == False
        assert health['dlq'] == True

    def test_health_check_no_dlq_configured(self):
        """Test health check when DLQ is not configured"""
        self.settings.dlq_url = None
        self.client.sqs.list_queues.return_value = {}
        self.client.get_queue_attributes = Mock(return_value={'ApproximateNumberOfMessages': '0'})
        
        health = self.client.health_check()
        
        assert health['sqs_connection'] == True
        assert health['input_queue'] == True
        assert health['output_queue'] == True
        assert health['dlq'] == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
