"""
AWS SQS Client for managing queue operations.
"""
import boto3
import json
import logging
from typing import List, Optional, Dict, Any
from botocore.exceptions import ClientError, BotoCoreError
from datetime import datetime

from .config import SQSSettings
from ..models.sqs_models import SQSMessageWrapper, SQSValidationRequest

logger = logging.getLogger(__name__)

class SQSClient:
    """AWS SQS Client for queue operations"""
    
    def __init__(self, settings: SQSSettings):
        self.settings = settings
        self.sqs = None
        self._connect()
    
    def _connect(self):
        """Initialize SQS client connection"""
        try:
            session = boto3.Session(
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.aws_region
            )
            
            self.sqs = session.client('sqs')
            logger.info(f"SQS client connected to region: {self.settings.aws_region}")
            
        except Exception as e:
            logger.error(f"Failed to connect to SQS: {e}")
            raise

    def receive_messages(self, queue_url: Optional[str] = None) -> List[SQSMessageWrapper]:
        """
        Receive messages from SQS queue
        
        Args:
            queue_url: Queue URL (defaults to input queue)
            
        Returns:
            List of wrapped SQS messages
        """
        if not queue_url:
            queue_url = self.settings.input_queue_url
            
        try:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=self.settings.max_messages_per_poll,
                WaitTimeSeconds=self.settings.wait_time_seconds,
                VisibilityTimeout=self.settings.visibility_timeout,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            wrapped_messages = []
            
            for message in messages:
                try:
                    # Parse message body
                    body_data = json.loads(message['Body'])
                    validation_request = SQSValidationRequest(**body_data)
                    
                    # Create wrapped message
                    wrapped_message = SQSMessageWrapper(
                        receipt_handle=message['ReceiptHandle'],
                        message_id=message['MessageId'],
                        body=validation_request,
                        attributes=message.get('Attributes', {})
                    )
                    
                    wrapped_messages.append(wrapped_message)
                    
                except Exception as e:
                    logger.error(f"Failed to parse message {message.get('MessageId', 'unknown')}: {e}")
                    # Optionally send to DLQ
                    continue
            
            if wrapped_messages:
                logger.info(f"Received {len(wrapped_messages)} messages from queue")
            else:
                logger.debug(f"Received 0 messages from queue")
            return wrapped_messages
            
        except ClientError as e:
            logger.error(f"SQS receive_message error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error receiving messages: {e}")
            return []

    def delete_message(self, receipt_handle: str, queue_url: Optional[str] = None) -> bool:
        """
        Delete message from queue
        
        Args:
            receipt_handle: Message receipt handle
            queue_url: Queue URL (defaults to input queue)
            
        Returns:
            True if successful, False otherwise
        """
        if not queue_url:
            queue_url = self.settings.input_queue_url
            
        try:
            self.sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.debug(f"Message deleted successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete message: {e}")
            return False

    def send_message(self, message_body: Dict[str, Any], queue_url: Optional[str] = None, 
                    delay_seconds: int = 0, message_attributes: Optional[Dict] = None) -> Optional[str]:
        """
        Send message to queue
        
        Args:
            message_body: Message content
            queue_url: Queue URL (defaults to output queue)
            delay_seconds: Message delay
            message_attributes: Message attributes
            
        Returns:
            Message ID if successful, None otherwise
        """
        if not queue_url:
            queue_url = self.settings.output_queue_url
            
        try:
            params = {
                'QueueUrl': queue_url,
                'MessageBody': json.dumps(message_body, default=str)
            }
            
            if delay_seconds > 0:
                params['DelaySeconds'] = delay_seconds
                
            if message_attributes:
                params['MessageAttributes'] = message_attributes
            
            response = self.sqs.send_message(**params)
            message_id = response['MessageId']
            
            logger.debug(f"Message sent successfully: {message_id}")
            return message_id
            
        except ClientError as e:
            logger.error(f"Failed to send message: {e}")
            return None

    def send_to_dlq(self, message: SQSMessageWrapper, error_reason: str) -> bool:
        """
        Send message to Dead Letter Queue
        
        Args:
            message: Original message
            error_reason: Reason for DLQ
            
        Returns:
            True if successful, False otherwise
        """
        if not self.settings.dlq_url:
            logger.warning("DLQ URL not configured, cannot send message to DLQ")
            return False
        
        dlq_message = {
            "original_message_id": message.message_id,
            "original_body": message.body.dict(),
            "error_reason": error_reason,
            "failed_at": datetime.utcnow().isoformat(),
            "attempts": message.attempts
        }
        
        message_id = self.send_message(dlq_message, self.settings.dlq_url)
        return message_id is not None

    def change_message_visibility(self, receipt_handle: str, visibility_timeout: int, 
                                queue_url: Optional[str] = None) -> bool:
        """
        Change message visibility timeout
        
        Args:
            receipt_handle: Message receipt handle
            visibility_timeout: New visibility timeout in seconds
            queue_url: Queue URL (defaults to input queue)
            
        Returns:
            True if successful, False otherwise
        """
        if not queue_url:
            queue_url = self.settings.input_queue_url
            
        try:
            self.sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout
            )
            logger.debug(f"Message visibility changed to {visibility_timeout} seconds")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to change message visibility: {e}")
            return False

    def get_queue_attributes(self, queue_url: str) -> Optional[Dict[str, Any]]:
        """
        Get queue attributes
        
        Args:
            queue_url: Queue URL
            
        Returns:
            Queue attributes or None if failed
        """
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )
            return response.get('Attributes', {})
            
        except ClientError as e:
            logger.error(f"Failed to get queue attributes: {e}")
            return None

    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics for all configured queues
        
        Returns:
            Dictionary with queue statistics
        """
        stats = {}
        
        # Input queue stats
        if self.settings.input_queue_url:
            input_attrs = self.get_queue_attributes(self.settings.input_queue_url)
            if input_attrs:
                stats['input_queue'] = {
                    'approximate_number_of_messages': input_attrs.get('ApproximateNumberOfMessages', '0'),
                    'approximate_number_of_messages_not_visible': input_attrs.get('ApproximateNumberOfMessagesNotVisible', '0'),
                    'approximate_number_of_messages_delayed': input_attrs.get('ApproximateNumberOfMessagesDelayed', '0')
                }
        
        # Output queue stats
        if self.settings.output_queue_url:
            output_attrs = self.get_queue_attributes(self.settings.output_queue_url)
            if output_attrs:
                stats['output_queue'] = {
                    'approximate_number_of_messages': output_attrs.get('ApproximateNumberOfMessages', '0'),
                    'approximate_number_of_messages_not_visible': output_attrs.get('ApproximateNumberOfMessagesNotVisible', '0')
                }
        
        # DLQ stats
        if self.settings.dlq_url:
            dlq_attrs = self.get_queue_attributes(self.settings.dlq_url)
            if dlq_attrs:
                stats['dlq'] = {
                    'approximate_number_of_messages': dlq_attrs.get('ApproximateNumberOfMessages', '0')
                }
        
        return stats

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on SQS connection and queues
        
        Returns:
            Health check results
        """
        health = {
            'sqs_connection': False,
            'input_queue': False,
            'output_queue': False,
            'dlq': False,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        try:
            # Test SQS connection
            self.sqs.list_queues(MaxResults=1)
            health['sqs_connection'] = True
            
            # Test input queue
            if self.settings.input_queue_url:
                attrs = self.get_queue_attributes(self.settings.input_queue_url)
                health['input_queue'] = attrs is not None
            
            # Test output queue
            if self.settings.output_queue_url:
                attrs = self.get_queue_attributes(self.settings.output_queue_url)
                health['output_queue'] = attrs is not None
            
            # Test DLQ
            if self.settings.dlq_url:
                attrs = self.get_queue_attributes(self.settings.dlq_url)
                health['dlq'] = attrs is not None
                
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            health['error'] = str(e)
        
        return health
