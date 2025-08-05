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
from ..models.validation import (
    ValidationRequest,
    SQSValidationRequest,
    SQSValidationResponse,
    SQSMessageWrapper,
    ProcessingResult,
    MessageStatus,
    ValidationResultDetail,
    ValidationSummary,
    DataType,
    convert_legacy_validation_request
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
            logger.info(f"Processing message {message.body.message_id} (worker: {self.worker_id})")
            
            # Use unified message methods to get data
            dataset = message.body.get_dataset()
            data_key = message.body.get_data_key()
            data_type = message.body.get_data_type()
            
            # Create proper ValidationRequest object using unified model
            validation_request = ValidationRequest(
                dataset=dataset,
                rules=message.body.validation_rules,
                data_key=data_key,
                data_type=data_type
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
            
            # Create response
            sqs_response = SQSValidationResponse(
                message_id=message.body.message_id,
                correlation_id=message.body.correlation_id,
                processing_time_ms=processing_time,
                status=MessageStatus.SUCCESS,
                worker_id=self.worker_id,
                data_key=data_key,
                data_type=data_type,
                validation_results=detailed_results,
                summary=enhanced_summary,
                batch_id=getattr(message.body, 'batch_id', None),
                source=getattr(message.body, 'source', None),
                
                # Legacy compatibility fields
                total_rules=enhanced_summary.total_rules,
                successful_rules=enhanced_summary.successful_rules,
                failed_rules=enhanced_summary.failed_rules
            )
            
            # Always send response to output queue (required for SQS workflow)
            output_sent = await self._send_to_output_queue(sqs_response)
            
            # Send callback if configured
            if message.body.callback_url:
                await self._send_callback(message.body.callback_url, sqs_response)
            
            self.processed_count += 1
            
            return ProcessingResult(
                success=True,
                message_id=message.body.message_id,
                processing_time_ms=processing_time,
                response=sqs_response,
                should_delete=True
            )
            
        except Exception as e:
            self.error_count += 1
            processing_time = int((time.time() - start_time) * 1000)
            
            logger.error(f"Failed to process message {message.body.message_id}: {e}")
            
            # Determine retry logic
            should_retry = (
                message.body.retry_count < (message.body.max_retries or self.settings.max_retries) and
                not self._is_permanent_error(e)
            )
            
            # Get data information for error response
            if hasattr(message.body, 'data_entry') and message.body.data_entry:
                error_data_key = message.body.data_entry.data_key
                error_data_type = message.body.data_entry.data_type
                error_data_rows = len(message.body.data_entry.data) if message.body.data_entry.data else 0
                error_data_cols = len(message.body.data_entry.columns) if message.body.data_entry.columns else 0
            else:
                error_data_key = f"legacy-{message.body.message_id}"
                error_data_type = DataType.TABULAR
                error_data_rows = len(message.body.data) if message.body.data else 0
                error_data_cols = len(message.body.data[0].keys()) if message.body.data and len(message.body.data) > 0 else 0
            
            # Create error summary
            error_summary = ValidationSummary(
                total_rules=len(getattr(message.body, 'validation_rules', []) or getattr(message.body, 'rules', [])),
                successful_rules=0,
                failed_rules=0,
                success_rate=0.0,
                total_rows=error_data_rows,
                total_columns=error_data_cols,
                execution_time_ms=processing_time,
                validation_engine="great_expectations"
            )
            
            # Create error response
            error_response = SQSValidationResponse(
                message_id=message.body.message_id,
                correlation_id=message.body.correlation_id,
                processing_time_ms=processing_time,
                status=MessageStatus.RETRY if should_retry else MessageStatus.FAILED,
                worker_id=self.worker_id,
                data_key=error_data_key,
                data_type=error_data_type,
                summary=error_summary,
                error_message=str(e),
                error_code=type(e).__name__,
                batch_id=getattr(message.body, 'batch_id', None),
                source=getattr(message.body, 'source', None),
                
                # Legacy compatibility fields
                total_rules=error_summary.total_rules,
                successful_rules=error_summary.successful_rules,
                failed_rules=error_summary.failed_rules
            )
            
            # Send error response to output queue for tracking
            await self._send_to_output_queue(error_response)
            
            return ProcessingResult(
                success=False,
                message_id=message.body.message_id,
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
                    json=response.dict(),
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        logger.info(f"Callback sent successfully to {callback_url}")
                    else:
                        logger.warning(f"Callback failed with status {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to send callback to {callback_url}: {e}")

    async def _send_to_output_queue(self, response: SQSValidationResponse) -> bool:
        """
        Send validation response to output SQS queue
        
        Args:
            response: Validation response to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Always try to send to output queue if configured
            if self.settings.output_queue_url:
                message_id = self.sqs_client.send_message(
                    response.dict(),
                    queue_url=self.settings.output_queue_url
                )
                
                if message_id:
                    logger.info(f"Response sent to output queue: {message_id} for message {response.message_id}")
                    return True
                else:
                    logger.error(f"Failed to send response to output queue for message {response.message_id}")
                    return False
            else:
                logger.warning("Output queue URL not configured, cannot send response")
                return False
                
        except Exception as e:
            logger.error(f"Error sending response to output queue for message {response.message_id}: {e}")
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
                    # Receive messages from queue
                    messages = self.sqs_client.receive_messages()
                    
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
                
                except Exception as e:
                    logger.error(f"Error in worker loop: {e}")
                    await asyncio.sleep(5)  # Brief pause before retrying
                    
        except asyncio.CancelledError:
            logger.info(f"Worker {self.worker_id} cancelled")
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
