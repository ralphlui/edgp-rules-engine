"""
Comprehensive unit tests for app.sqs.processor module
Tests SQS message processing functionality
"""
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime

from app.sqs.processor import MessageProcessor
from app.sqs.config import SQSSettings
from app.sqs.client import SQSClient
from app.models.sqs_models import (
    SQSMessageWrapper,
    SQSValidationRequest,
    SQSValidationResponse,
    DataEntry,
    ValidationRule,
    ProcessingResult,
    MessageStatus
)
from app.models.validation import ValidationResponse, ValidationResultDetail, ValidationSummary


class TestMessageProcessorInitialization:
    """Test MessageProcessor initialization"""

    def test_processor_init(self):
        """Test MessageProcessor initialization"""
        settings = SQSSettings()
        sqs_client = Mock(spec=SQSClient)
        
        processor = MessageProcessor(settings, sqs_client)
        
        assert processor.settings == settings
        assert processor.sqs_client == sqs_client
        assert processor.worker_id.startswith("worker-")
        assert processor.is_running == False
        assert processor.processed_count == 0
        assert processor.error_count == 0


class TestMessageProcessorProcessMessage:
    """Test MessageProcessor.process_message functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.sqs_client = Mock(spec=SQSClient)
        self.processor = MessageProcessor(self.settings, self.sqs_client)

    def create_mock_message(self, message_id="test-123", attempts=1):
        """Create a mock SQS message for testing"""
        data_entry = DataEntry(
            data_type="tabular",
            domain_name="test-domain",
            file_id="file-123",
            policy_id="policy-456",
            data={
                "columns": ["name", "age"],
                "records": [
                    {"name": "John", "age": 25},
                    {"name": "Jane", "age": 30}
                ]
            },
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    rule_description="Name column should exist",
                    severity="error"
                )
            ]
        )
        
        validation_request = SQSValidationRequest(
            message_id=message_id,
            correlation_id="corr-123",
            timestamp=datetime.now().isoformat(),
            source="test",
            data_entry=data_entry,
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    rule_description="Name column should exist",
                    severity="error"
                )
            ],
            batch_id="batch-123",
            priority=5,
            max_retries=3
        )
        
        return SQSMessageWrapper(
            receipt_handle="receipt-123",
            message_id=f"msg-{message_id}",
            body=validation_request,
            attributes={},
            attempts=attempts
        )

    @pytest.mark.asyncio
    async def test_process_message_success(self):
        """Test successful message processing"""
        mock_message = self.create_mock_message()
        
        # Mock API validation response
        mock_validation_response = ValidationResponse(
            results=[
                ValidationResultDetail(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    success=True,
                    result={"element_count": 2, "unexpected_count": 0}
                )
            ],
            summary=ValidationSummary(
                total_rules=1,
                successful_rules=1,
                failed_rules=0,
                success_rate=100.0,
                total_rows=2,
                total_columns=2,
                execution_time_ms=50,
                validation_engine="great_expectations"
            )
        )
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            mock_validate.return_value = mock_validation_response
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                result = await self.processor.process_message(mock_message)
                
                assert result.success == True
                assert result.message_id == "msg-test-123"
                assert result.should_delete == True
                assert result.response.status == MessageStatus.SUCCESS
                assert self.processor.processed_count == 1

    @pytest.mark.asyncio
    async def test_process_message_api_failure(self):
        """Test message processing when API validation fails"""
        mock_message = self.create_mock_message()
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            mock_validate.side_effect = ValueError("Validation API failed")
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                result = await self.processor.process_message(mock_message)
                
                assert result.success == False
                assert result.should_delete == True  # Permanent error
                assert result.should_retry == False
                assert result.send_to_dlq == True  # ValidationError should send to DLQ
                assert self.processor.error_count == 1

    @pytest.mark.asyncio
    async def test_process_message_retry_logic(self):
        """Test message processing retry logic for non-permanent errors"""
        mock_message = self.create_mock_message(attempts=2)  # Below max retries
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            mock_validate.side_effect = RuntimeError("Temporary network error")
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                result = await self.processor.process_message(mock_message)
                
                assert result.success == False
                assert result.should_delete == False  # Should retry
                assert result.should_retry == True
                assert result.send_to_dlq == False

    @pytest.mark.asyncio
    async def test_process_message_max_retries_exceeded(self):
        """Test message processing when max retries exceeded"""
        mock_message = self.create_mock_message(attempts=4)  # Exceeds max retries (3)
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            mock_validate.side_effect = RuntimeError("Persistent error")
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                result = await self.processor.process_message(mock_message)
                
                assert result.success == False
                assert result.should_delete == True
                assert result.should_retry == False
                assert result.send_to_dlq == True

    @pytest.mark.asyncio
    async def test_process_message_permanent_error(self):
        """Test message processing with permanent error"""
        mock_message = self.create_mock_message(attempts=1)
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            mock_validate.side_effect = KeyError("Missing required field")
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                result = await self.processor.process_message(mock_message)
                
                assert result.success == False
                assert result.should_delete == True  # Permanent error, don't retry
                assert result.should_retry == False
                assert result.send_to_dlq == True  # KeyError is permanent and should send to DLQ

    @pytest.mark.asyncio
    async def test_process_message_with_complex_data(self):
        """Test message processing with complex validation rules"""
        # Create message with multiple validation rules
        data_entry = DataEntry(
            data_type="tabular",
            domain_name="test-domain",
            file_id="complex-file",
            policy_id="complex-policy",
            data={
                "columns": ["name", "age", "salary", "email"],
                "records": [
                    {"name": "John", "age": 25, "salary": 50000, "email": "john@example.com"},
                    {"name": "Jane", "age": 30, "salary": 75000, "email": "jane@example.com"},
                    {"name": "Bob", "age": 35, "salary": 60000, "email": "bob@example.com"}
                ]
            },
            validation_rules=[
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
        
        validation_request = SQSValidationRequest(
            message_id="complex-123",
            correlation_id="complex-corr",
            timestamp=datetime.now().isoformat(),
            source="test",
            data_entry=data_entry,
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    rule_description="Name column should exist",
                    severity="error"
                ),
                ValidationRule(
                    rule_name="expect_column_values_to_be_between",
                    column_name="age",
                    value={"min_value": 18, "max_value": 65},
                    rule_description="Age should be between 18 and 65",
                    severity="error"
                ),
                ValidationRule(
                    rule_name="expect_column_values_to_be_between",
                    column_name="salary",
                    value={"min_value": 30000, "max_value": 100000},
                    rule_description="Salary should be reasonable",
                    severity="warning"
                )
            ],
            batch_id="complex-batch",
            priority=3,
            max_retries=3
        )
        
        mock_message = SQSMessageWrapper(
            receipt_handle="complex-receipt",
            message_id="complex-msg",
            body=validation_request,
            attributes={}
        )
        
        # Mock comprehensive validation response
        mock_validation_response = ValidationResponse(
            results=[
                ValidationResultDetail(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    success=True,
                    result={"element_count": 3, "unexpected_count": 0}
                ),
                ValidationResultDetail(
                    rule_name="expect_column_values_to_be_between",
                    column_name="age",
                    success=True,
                    result={"element_count": 3, "unexpected_count": 0}
                ),
                ValidationResultDetail(
                    rule_name="expect_column_values_to_be_between",
                    column_name="salary",
                    success=True,
                    result={"element_count": 3, "unexpected_count": 0}
                )
            ],
            summary=ValidationSummary(
                total_rules=3,
                successful_rules=3,
                failed_rules=0,
                success_rate=100.0,
                total_rows=3,
                total_columns=4,
                execution_time_ms=150,
                validation_engine="great_expectations"
            )
        )
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            mock_validate.return_value = mock_validation_response
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                result = await self.processor.process_message(mock_message)
                
                assert result.success == True
                assert result.response.status == MessageStatus.SUCCESS
                # Check that we have a response (validation_results is not a field in SQSValidationResponse)
                assert result.response is not None


class TestMessageProcessorErrorClassification:
    """Test MessageProcessor._is_permanent_error functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.sqs_client = Mock(spec=SQSClient)
        self.processor = MessageProcessor(self.settings, self.sqs_client)

    def test_is_permanent_error_validation_error(self):
        """Test permanent error classification for ValidationError"""
        error = ValueError("Invalid value")
        
        result = self.processor._is_permanent_error(error)
        
        assert result == True

    def test_is_permanent_error_key_error(self):
        """Test permanent error classification for KeyError"""
        error = KeyError("Missing key")
        
        result = self.processor._is_permanent_error(error)
        
        assert result == True

    def test_is_permanent_error_type_error(self):
        """Test permanent error classification for TypeError"""
        error = TypeError("Wrong type")
        
        result = self.processor._is_permanent_error(error)
        
        assert result == True

    def test_is_permanent_error_temporary_error(self):
        """Test permanent error classification for temporary errors"""
        error = ConnectionError("Network timeout")
        
        result = self.processor._is_permanent_error(error)
        
        assert result == False

    def test_is_permanent_error_runtime_error(self):
        """Test permanent error classification for RuntimeError"""
        error = RuntimeError("Runtime issue")
        
        result = self.processor._is_permanent_error(error)
        
        assert result == False


class TestMessageProcessorSendCallback:
    """Test MessageProcessor._send_callback functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.sqs_client = Mock(spec=SQSClient)
        self.processor = MessageProcessor(self.settings, self.sqs_client)

    @pytest.mark.asyncio
    async def test_send_callback_success(self):
        """Test successful callback sending"""
        callback_url = "https://webhook.example.com/callback"
        
        mock_response = SQSValidationResponse(
            file_id="file-123",
            policy_id="policy-456",
            data_type="tabular",
            status="success",
            domain_name="test-domain",
            data={"id": "test-123"}
        )
        
        # Create a proper async context manager mock
        mock_session = AsyncMock()
        mock_response_obj = AsyncMock()
        mock_response_obj.status = 200
        
        # Mock the async context manager protocol for ClientSession
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        
        # Mock the post method and its async context manager
        mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response_obj)
        mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            await self.processor._send_callback(callback_url, mock_response)
            
            mock_session.post.assert_called_once()
            call_args = mock_session.post.call_args
            assert call_args[0][0] == callback_url

    @pytest.mark.asyncio
    async def test_send_callback_failure(self):
        """Test callback sending failure handling"""
        callback_url = "https://invalid-webhook.example.com/callback"
        
        mock_response = SQSValidationResponse(
            file_id="file-123",
            policy_id="policy-456",
            data_type="tabular",
            status="success",
            domain_name="test-domain",
            data={"id": "test-123"}
        )
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_session.return_value.post.side_effect = Exception("Network error")
            
            with patch('app.sqs.processor.logger') as mock_logger:
                await self.processor._send_callback(callback_url, mock_response)
                
                mock_logger.error.assert_called()


class TestMessageProcessorSendToOutputQueue:
    """Test MessageProcessor._send_to_output_queue functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.sqs_client = Mock(spec=SQSClient)
        self.processor = MessageProcessor(self.settings, self.sqs_client)

    @pytest.mark.asyncio
    async def test_send_to_output_queue_success(self):
        """Test successful output queue sending"""
        mock_response = SQSValidationResponse(
            file_id="file-123",
            policy_id="policy-456",
            data_type="tabular",
            status="success",
            domain_name="test-domain",
            data={"id": "test-123"}
        )
        
        self.sqs_client.send_message.return_value = "output-msg-123"
        
        result = await self.processor._send_to_output_queue(mock_response, "original-msg-123")
        
        assert result == True
        self.sqs_client.send_message.assert_called_once()
        call_args = self.sqs_client.send_message.call_args[0]
        assert isinstance(call_args[0], dict)  # Message body is dict

    @pytest.mark.asyncio
    async def test_send_to_output_queue_failure(self):
        """Test output queue sending failure"""
        mock_response = SQSValidationResponse(
            file_id="file-123",
            policy_id="policy-456",
            data_type="tabular",
            status="success",
            domain_name="test-domain",
            data={"id": "test-123"}
        )
        
        self.sqs_client.send_message.return_value = None  # Send failure
        
        with patch('app.sqs.processor.logger') as mock_logger:
            result = await self.processor._send_to_output_queue(mock_response, "original-msg-123")
            
            assert result == False
            mock_logger.error.assert_called()


class TestMessageProcessorDataConversion:
    """Test MessageProcessor data conversion functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        self.settings = SQSSettings()
        self.sqs_client = Mock(spec=SQSClient)
        self.processor = MessageProcessor(self.settings, self.sqs_client)

    @pytest.mark.asyncio
    async def test_process_message_data_conversion(self):
        """Test data conversion from SQS format to API format"""
        mock_message = self.create_mock_message()
        
        with patch('app.sqs.processor.api_validate_data') as mock_validate:
            # Capture the converted data
            captured_request = None
            def capture_request(request):
                nonlocal captured_request
                captured_request = request
                return ValidationResponse(
                    results=[],
                    summary=ValidationSummary(
                        total_rules=0, successful_rules=0, failed_rules=0,
                        success_rate=0.0, total_rows=0, total_columns=0,
                        execution_time_ms=50, validation_engine="great_expectations"
                    )
                )
            
            mock_validate.side_effect = capture_request
            
            with patch.object(self.processor, '_send_to_output_queue', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = True
                
                await self.processor.process_message(mock_message)
                
                # Verify data conversion
                assert captured_request is not None
                assert hasattr(captured_request, 'dataset')
                assert hasattr(captured_request, 'rules')
                
                # Verify converted rules format
                assert isinstance(captured_request.rules, list)
                if captured_request.rules:
                    rule = captured_request.rules[0]
                    assert hasattr(rule, 'rule_name')
                    assert hasattr(rule, 'column_name')
                    assert hasattr(rule, 'value')

    def create_mock_message(self, message_id="test-123", attempts=1):
        """Create a mock SQS message for testing"""
        data_entry = DataEntry(
            data_type="tabular",
            domain_name="test-domain",
            file_id="file-123",
            policy_id="policy-456",
            data={
                "columns": ["name", "age"],
                "records": [
                    {"name": "John", "age": 25},
                    {"name": "Jane", "age": 30}
                ]
            },
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    rule_description="Name column should exist",
                    severity="error"
                )
            ]
        )
        
        validation_request = SQSValidationRequest(
            message_id=message_id,
            correlation_id="corr-123",
            timestamp=datetime.now().isoformat(),
            source="test",
            data_entry=data_entry,
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name",
                    value={"exists": True},
                    rule_description="Name column should exist",
                    severity="error"
                )
            ],
            batch_id="batch-123",
            priority=5,
            max_retries=3
        )
        
        return SQSMessageWrapper(
            receipt_handle="receipt-123",
            message_id=f"msg-{message_id}",
            body=validation_request,
            attributes={},
            attempts=attempts
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
