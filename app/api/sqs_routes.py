"""
SQS API routes for managing SQS operations and monitoring.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, Any, Optional
import logging

from ..sqs import get_sqs_manager, start_sqs_processing, stop_sqs_processing
from ..models.sqs_models import SQSValidationRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sqs", tags=["SQS"])

@router.get("/status")
async def get_sqs_status() -> Dict[str, Any]:
    """
    Get SQS manager status and statistics
    
    Returns:
        SQS manager status including worker stats and queue information
    """
    try:
        manager = get_sqs_manager()
        return manager.get_status()
    except Exception as e:
        logger.error(f"Failed to get SQS status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get SQS status")

@router.get("/health")
async def get_sqs_health() -> Dict[str, Any]:
    """
    Get SQS health check information
    
    Returns:
        Health status of SQS connection, queues, and workers
    """
    try:
        manager = get_sqs_manager()
        return manager.get_health()
    except Exception as e:
        logger.error(f"Failed to get SQS health: {e}")
        raise HTTPException(status_code=500, detail="Failed to get SQS health")

@router.post("/start")
async def start_sqs(background_tasks: BackgroundTasks):
    """
    Start SQS processing workers
    
    Returns:
        Success message
    """
    try:
        manager = get_sqs_manager()
        if manager.is_running:
            raise HTTPException(status_code=400, detail="SQS processing is already running")
        
        # Start SQS processing in background
        background_tasks.add_task(start_sqs_processing)
        
        return {"message": "SQS processing started successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start SQS processing: {e}")
        raise HTTPException(status_code=500, detail="Failed to start SQS processing")

@router.post("/stop")
async def stop_sqs():
    """
    Stop SQS processing workers
    
    Returns:
        Success message
    """
    try:
        manager = get_sqs_manager()
        if not manager.is_running:
            raise HTTPException(status_code=400, detail="SQS processing is not running")
        
        await stop_sqs_processing()
        
        return {"message": "SQS processing stopped successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to stop SQS processing: {e}")
        raise HTTPException(status_code=500, detail="Failed to stop SQS processing")

@router.get("/queue-stats")
async def get_queue_stats() -> Dict[str, Any]:
    """
    Get queue statistics for all configured queues
    
    Returns:
        Queue statistics including message counts
    """
    try:
        manager = get_sqs_manager()
        return manager.sqs_client.get_queue_stats()
    except Exception as e:
        logger.error(f"Failed to get queue stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get queue statistics")

@router.post("/send-message")
async def send_test_message(
    validation_request: SQSValidationRequest,
    queue_url: Optional[str] = None
):
    """
    Send a test validation message to SQS queue
    
    Args:
        validation_request: Validation request to send
        queue_url: Optional queue URL (defaults to input queue)
        
    Returns:
        Message ID if successful
    """
    try:
        manager = get_sqs_manager()
        
        message_id = manager.sqs_client.send_message(
            validation_request.dict(),
            queue_url=queue_url
        )
        
        if not message_id:
            raise HTTPException(status_code=500, detail="Failed to send message")
        
        return {
            "message": "Message sent successfully",
            "message_id": message_id,
            "queue": queue_url or "input_queue"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")

@router.get("/worker-stats")
async def get_worker_stats() -> Dict[str, Any]:
    """
    Get detailed worker statistics
    
    Returns:
        Individual worker performance statistics
    """
    try:
        manager = get_sqs_manager()
        status = manager.get_status()
        
        return {
            "total_workers": status.get("worker_count", 0),
            "running_workers": sum(1 for w in status.get("workers", []) if w.get("is_running", False)),
            "total_processed": status.get("total_processed", 0),
            "total_errors": status.get("total_errors", 0),
            "success_rate": status.get("success_rate", 0),
            "workers": status.get("workers", [])
        }
        
    except Exception as e:
        logger.error(f"Failed to get worker stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get worker statistics")
