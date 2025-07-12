"""
SQS module for AWS SQS integration with queue-based validation processing.

This module provides:
- SQS Client for queue operations
- Message models for validation requests/responses
- Message processor for handling validation logic
- SQS Manager for coordinating multiple workers
- Load balancing and retry logic
"""

from .config import SQSSettings
from ..models.sqs_models import (
    SQSValidationRequest, 
    SQSValidationResponse, 
    SQSMessageWrapper,
    ValidationRule,
    MessageStatus,
    ProcessingResult
)
from .client import SQSClient
from .processor import MessageProcessor
from .manager import SQSManager, get_sqs_manager, start_sqs_processing, stop_sqs_processing

__all__ = [
    'SQSSettings',
    'SQSValidationRequest',
    'SQSValidationResponse', 
    'SQSMessageWrapper',
    'ValidationRule',
    'MessageStatus',
    'ProcessingResult',
    'SQSClient',
    'MessageProcessor',
    'SQSManager',
    'get_sqs_manager',
    'start_sqs_processing',
    'stop_sqs_processing'
]
