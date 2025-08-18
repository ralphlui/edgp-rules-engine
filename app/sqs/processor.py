"""
Message processor for handling SQS validation requests.
"""
import asyncio
import logging
import time
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..api.routes import validate_data as api_validate_data
from ..models.validation import ValidationRequest
from ..models.sqs_models import (
    SQSValidationRequest,
    SQSValidationResponse,
    SQSMessageWrapper,
    ProcessingResult,
    MessageStatus,
    ValidationResultDetail,
    ValidationSummary,
    DataType,
    get_dataset_from_request,
    get_validation_rules_from_request,
    create_response_from_request_and_results
)
from .config import SQSSettings
from .client import SQSClient

logger = logging.getLogger(__name__)

class MessageProcessor:
    """Processes SQS validation messages"""
    
    def __init__(self, settings: SQSSettings, sqs_client: SQSClient):
        self.settings = settings
        self.sqs_client = sqs_client
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.is_running = False
        self.processed_count = 0
        self.error_count = 0
        
    async def process_message(self, message: SQSMessageWrapper) -> ProcessingResult:
        """
        Process a single validation message
        
        Args:
            message: SQS message wrapper
            
        Returns:
            Processing result
        """
        start_time = time.time()
        
        try:
            logger.info(f"Processing message {message.message_id} (worker: {self.worker_id})")
            
            # Extract data and rules using helper functions
            dataset = get_dataset_from_request(message.body)
            validation_rules = get_validation_rules_from_request(message.body)
            
            # Get file and policy IDs from data_entry
            file_id = message.body.data_entry.file_id
            policy_id = message.body.data_entry.policy_id
            data_type = message.body.data_entry.data_type
            
            # Convert SQS ValidationRule objects to the format expected by the API
            api_rules = []
            for rule in validation_rules:
                api_rule = {
                    "rule_name": rule.rule_name,
                    "column_name": rule.column_name,
                    "value": rule.value
                }
                api_rules.append(api_rule)
            
            # Create proper ValidationRequest object using unified model
            validation_request = ValidationRequest(
                dataset=dataset,
                rules=api_rules
            )
            
            # Call validation API (reuse existing logic)
            validation_response = api_validate_data(validation_request)
            
            # Calculate processing time
            processing_time = int((time.time() - start_time) * 1000)
            
            # The API now returns ValidationResponse with unified models
            # Extract results and summary
            detailed_results = validation_response.results
            enhanced_summary = validation_response.summary
            
            # Update summary with processing time
            enhanced_summary.execution_time_ms = processing_time
            
            # Create response using helper function
            sqs_response = create_response_from_request_and_results(
                request=message.body,
                validation_results=detailed_results,
                summary=enhanced_summary,
                processing_time_ms=processing_time,
                status=MessageStatus.SUCCESS
            )
            
            # Always send response to output queue (required for SQS workflow)
            output_sent = await self._send_to_output_queue(sqs_response, message.message_id)
            
            self.processed_count += 1
            
            return ProcessingResult(
                success=True,
                message_id=message.message_id,
                processing_time_ms=processing_time,
                response=sqs_response,
                should_delete=True
            )
            
        except Exception as e:
            self.error_count += 1
            processing_time = int((time.time() - start_time) * 1000)
            
            logger.error(f"Failed to process message {message.message_id}: {e}")
            
            # Determine retry logic
            should_retry = (
                message.attempts < self.settings.max_retries and
                not self._is_permanent_error(e)
            )
            
            # Get data information for error summary
            data_entry = message.body.data_entry
            error_data_rows = len(data_entry.data) if isinstance(data_entry.data, list) else 1
            error_data_cols = len(data_entry.data.keys()) if isinstance(data_entry.data, dict) else 0
            
            # Create error summary
            error_summary = ValidationSummary(
                total_rules=len(data_entry.validation_rules),
                successful_rules=0,
                failed_rules=len(data_entry.validation_rules),
                success_rate=0.0,
                total_rows=error_data_rows,
                total_columns=error_data_cols,
                execution_time_ms=processing_time,
                validation_engine="great_expectations"
            )
            
            # Create error response using helper function with empty results
            error_response = create_response_from_request_and_results(
                request=message.body,
                validation_results=[],
                summary=error_summary,
                processing_time_ms=processing_time,
                status=MessageStatus.FAILED
            )
            
            # Send error response to output queue for tracking
            await self._send_to_output_queue(error_response, message.message_id)
            
            return ProcessingResult(
                success=False,
                message_id=message.message_id,
                processing_time_ms=processing_time,
                response=error_response,
                error=str(e),
                should_delete=not should_retry,
                should_retry=should_retry,
                send_to_dlq=not should_retry
            )

    def _is_permanent_error(self, error: Exception) -> bool:
        """
        Determine if error is permanent (shouldn't retry)
        
        Args:
            error: Exception that occurred
            
        Returns:
            True if error is permanent
        """
        permanent_errors = [
            "ValidationError",
            "ValueError", 
            "KeyError",
            "TypeError"
        ]
        
        return type(error).__name__ in permanent_errors

    async def _send_callback(self, callback_url: str, response: SQSValidationResponse):
        """
        Send callback notification
        
        Args:
            callback_url: Webhook URL
            response: Validation response
        """
        try:
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    callback_url,
                    json=response.model_dump(),
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        logger.info(f"Callback sent successfully to {callback_url}")
                    else:
                        logger.warning(f"Callback failed with status {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to send callback to {callback_url}: {e}")

    async def _send_to_output_queue(self, response: SQSValidationResponse, original_message_id: str) -> bool:
        """
        Send validation response to output SQS queue
        
        Args:
            response: Validation response to send
            original_message_id: ID of the original message being processed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Always try to send to output queue if configured
            if self.settings.output_queue_url:
                message_id = self.sqs_client.send_message(
                    response.model_dump(),
                    queue_url=self.settings.get_output_queue_url()
                )
                
                if message_id:
                    logger.info(f"Response sent to output queue: {message_id} for message {original_message_id}")
                    return True
                else:
                    logger.error(f"Failed to send response to output queue for message {original_message_id}")
                    return False
            else:
                logger.warning("Output queue URL not configured, cannot send response")
                return False
                
        except Exception as e:
            logger.error(f"Error sending response to output queue for message {original_message_id}: {e}")
            return False

    def process_batch(self, messages: List[SQSMessageWrapper]) -> List[ProcessingResult]:
        """
        Process a batch of messages with concurrent workers
        
        Args:
            messages: List of messages to process
            
        Returns:
            List of processing results
        """
        if not messages:
            return []
        
        logger.info(f"Processing batch of {len(messages)} messages")
        
        results = []
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=self.settings.worker_count) as executor:
            # Submit all messages for processing
            future_to_message = {
                executor.submit(asyncio.run, self.process_message(msg)): msg 
                for msg in messages
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_message):
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Handle message based on result
                    message = future_to_message[future]
                    
                    if result.should_delete:
                        success = self.sqs_client.delete_message(message.receipt_handle)
                        if not success:
                            logger.error(f"Failed to delete message {result.message_id}")
                    
                    elif result.should_retry:
                        # Increase retry count and change visibility for retry
                        message.body.retry_count += 1
                        visibility_timeout = min(
                            self.settings.visibility_timeout * (2 ** message.body.retry_count),
                            self.settings.max_retry_delay
                        )
                        self.sqs_client.change_message_visibility(
                            message.receipt_handle, 
                            visibility_timeout
                        )
                    
                    elif result.send_to_dlq:
                        # Send to DLQ and delete from main queue
                        self.sqs_client.send_to_dlq(message, result.error or "Processing failed")
                        self.sqs_client.delete_message(message.receipt_handle)
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing message: {e}")
                    # Try to delete message to prevent infinite loops
                    message = future_to_message[future]
                    self.sqs_client.delete_message(message.receipt_handle)
        
        return results

    async def run_worker_loop(self):
        """
        Main worker loop for processing messages
        """
        logger.info(f"Starting worker {self.worker_id}")
        self.is_running = True
        
        consecutive_empty_polls = 0
        max_empty_polls = 5
        
        try:
            while self.is_running:
                try:
                    # Check if we should stop before receiving messages
                    if not self.is_running:
                        break
                    
                    # Receive messages from queue (run in executor to allow cancellation)
                    loop = asyncio.get_event_loop()
                    messages = await loop.run_in_executor(
                        None, self.sqs_client.receive_messages
                    )
                    
                    # Check again after receiving messages
                    if not self.is_running:
                        break
                    
                    if messages:
                        consecutive_empty_polls = 0
                        
                        # Process messages in batch
                        results = self.process_batch(messages)
                        
                        # Log batch results
                        successful = sum(1 for r in results if r.success)
                        failed = len(results) - successful
                        
                        logger.info(
                            f"Batch processed: {successful} successful, {failed} failed "
                            f"(Total processed: {self.processed_count}, errors: {self.error_count})"
                        )
                        
                    else:
                        consecutive_empty_polls += 1
                        
                        # If no messages for a while, sleep to reduce polling
                        if consecutive_empty_polls >= max_empty_polls:
                            logger.debug("No messages received, sleeping...")
                            await asyncio.sleep(self.settings.poll_interval)
                            consecutive_empty_polls = 0
                    
                    # Short sleep to allow cancellation to be checked
                    await asyncio.sleep(0.1)
                
                except Exception as e:
                    if self.is_running:  # Only log errors if still running
                        logger.error(f"Error in worker loop: {e}")
                        await asyncio.sleep(5)  # Brief pause before retrying
                    
        except asyncio.CancelledError:
            logger.info(f"Worker {self.worker_id} cancelled")
            raise  # Re-raise to ensure proper cancellation
        finally:
            self.is_running = False
            logger.info(f"Worker {self.worker_id} stopped")

    def stop(self):
        """Stop the worker"""
        logger.info(f"Stopping worker {self.worker_id}")
        self.is_running = False

    def get_stats(self) -> Dict[str, Any]:
        """
        Get worker statistics
        
        Returns:
            Worker statistics
        """
        return {
            "worker_id": self.worker_id,
            "is_running": self.is_running,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "success_rate": (
                self.processed_count / (self.processed_count + self.error_count) 
                if (self.processed_count + self.error_count) > 0 else 0
            )
        }
