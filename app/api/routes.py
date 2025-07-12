from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List, Dict, Any, Optional
import logging
from app.models.rule import Rule
from app.rules.expectation_rules import get_all_expectation_rules
from app.validators.validator import data_validator
from app.models.validation_request import ValidationRequest
from app.models.validation_response import ValidationResponse, ValidationResult, ValidationSummary

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/api/rules", response_model=list[Rule])
def read_all_rules():
    return get_all_expectation_rules()

@router.post("/api/validate", response_model=ValidationResponse)
def validate_data(request: ValidationRequest) -> ValidationResponse:
    """
    Validate data using specified rules
    
    Args:
        request: ValidationRequest containing dataset and rules
        
    Returns:
        ValidationResponse with validation results
    """
    try:
        data = request.dataset
        rules = request.rules
        
        if not data:
            raise HTTPException(status_code=400, detail="Dataset is required")
        if not rules:
            raise HTTPException(status_code=400, detail="Rules are required")
        
        # Process validation using existing validator
        results = []
        summary = {"total_rules": len(rules), "passed": 0, "failed": 0}
        
        for rule in rules:
            try:
                rule_name = rule.rule_name
                column_name = rule.column_name
                value = rule.value or {}
                
                # Import the appropriate validator based on rule name
                validator_module = __import__(f"app.validators.{rule_name}", fromlist=[rule_name])
                validator_class = getattr(validator_module, rule_name.replace("_", " ").title().replace(" ", ""))
                
                # Create validator instance and validate
                validator_instance = validator_class()
                result = validator_instance.validate(data, column_name, **value)
                
                results.append(ValidationResult(
                    rule=rule_name,
                    column=column_name,
                    success=result.get("success", False),
                    message=result.get("message", ""),
                    details=result.get("details", {})
                ))
                
                if result.get("success", False):
                    summary["passed"] += 1
                else:
                    summary["failed"] += 1
                    
            except Exception as e:
                logger.error(f"Error validating rule {rule.rule_name}: {e}")
                results.append(ValidationResult(
                    rule=rule.rule_name,
                    column=rule.column_name or "",
                    success=False,
                    message=f"Validation error: {str(e)}",
                    details={}
                ))
                summary["failed"] += 1
        
        return ValidationResponse(
            results=results,
            summary=ValidationSummary(**summary)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validation request failed: {e}")
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

# SQS Routes - conditionally imported
try:
    from ..sqs import get_sqs_manager, start_sqs_processing, stop_sqs_processing
    from ..models.sqs_models import SQSValidationRequest
    SQS_AVAILABLE = True
except ImportError:
    SQS_AVAILABLE = False
    logger.warning("SQS functionality not available - SQS routes will not be registered")

# SQS Management Routes
if SQS_AVAILABLE:
    @router.get("/sqs/status")
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

    @router.get("/sqs/health")
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

    @router.post("/sqs/start")
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

    @router.post("/sqs/stop")
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

    @router.get("/sqs/queue-stats")
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

    @router.post("/sqs/send-message")
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

    @router.get("/sqs/worker-stats")
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

