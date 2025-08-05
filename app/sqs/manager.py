"""
SQS Manager for coordinating multiple workers and managing the SQS processing.
"""
import asyncio
import logging
import signal
from typing import List, Dict, Any, Optional
from datetime import datetime

from .config import SQSSettings
from .client import SQSClient
from .processor import MessageProcessor

logger = logging.getLogger(__name__)

class SQSManager:
    """Manages SQS processing with multiple workers"""
    
    def __init__(self, settings: Optional[SQSSettings] = None):
        self.settings = settings or SQSSettings()
        self.sqs_client = SQSClient(self.settings)
        self.workers: List[MessageProcessor] = []
        self.is_running = False
        self.start_time: Optional[datetime] = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(self.stop())
    
    async def start(self):
        """Start the SQS manager with workers"""
        if self.is_running:
            logger.warning("SQS Manager is already running")
            return
        
        logger.info(f"Starting SQS Manager with {self.settings.worker_count} workers")
        self.start_time = datetime.now()
        self.is_running = True
        
        # Create and start workers
        tasks = []
        for i in range(self.settings.worker_count):
            worker = MessageProcessor(self.settings, self.sqs_client)
            self.workers.append(worker)
            
            # Start worker as async task
            task = asyncio.create_task(worker.run_worker_loop())
            tasks.append(task)
        
        logger.info(f"All {len(self.workers)} workers started successfully")
        
        # Wait for all workers to complete
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("All workers cancelled")
        except Exception as e:
            logger.error(f"Error in worker tasks: {e}")
        finally:
            self.is_running = False
    
    async def stop(self):
        """Stop the SQS manager and all workers"""
        if not self.is_running:
            logger.warning("SQS Manager is not running")
            return
        
        logger.info("Stopping SQS Manager...")
        self.is_running = False
        
        # Stop all workers
        for worker in self.workers:
            worker.stop()
        
        # Give workers time to finish current messages
        logger.info("Waiting for workers to finish current messages...")
        await asyncio.sleep(2)
        
        logger.info("SQS Manager stopped successfully")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of the SQS manager
        
        Returns:
            Status information
        """
        worker_stats = [worker.get_stats() for worker in self.workers]
        
        total_processed = sum(w['processed_count'] for w in worker_stats)
        total_errors = sum(w['error_count'] for w in worker_stats)
        
        uptime = None
        if self.start_time:
            uptime = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "is_running": self.is_running,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime_seconds": uptime,
            "worker_count": len(self.workers),
            "total_processed": total_processed,
            "total_errors": total_errors,
            "success_rate": (
                total_processed / (total_processed + total_errors) 
                if (total_processed + total_errors) > 0 else 0
            ),
            "workers": worker_stats,
            "queue_stats": self.sqs_client.get_queue_stats(),
            "settings": {
                "worker_count": self.settings.worker_count,
                "max_messages_per_poll": self.settings.max_messages_per_poll,
                "visibility_timeout": self.settings.visibility_timeout,
                "max_retries": self.settings.max_retries
            }
        }
    
    def get_health(self) -> Dict[str, Any]:
        """
        Get health status
        
        Returns:
            Health information
        """
        sqs_health = self.sqs_client.health_check()
        
        worker_health = {
            "running_workers": sum(1 for w in self.workers if w.is_running),
            "total_workers": len(self.workers),
            "all_workers_healthy": all(w.is_running for w in self.workers) if self.workers else False
        }
        
        overall_healthy = (
            sqs_health.get('sqs_connection', False) and
            sqs_health.get('input_queue', False) and
            worker_health['all_workers_healthy'] and
            self.is_running
        )
        
        return {
            "healthy": overall_healthy,
            "timestamp": datetime.now().isoformat(),
            "sqs": sqs_health,
            "workers": worker_health,
            "manager_running": self.is_running
        }

# Global SQS manager instance
_sqs_manager: Optional[SQSManager] = None

def get_sqs_manager() -> SQSManager:
    """Get the global SQS manager instance"""
    global _sqs_manager
    if _sqs_manager is None:
        _sqs_manager = SQSManager()
    return _sqs_manager

async def start_sqs_processing():
    """Start SQS processing (for use in FastAPI startup)"""
    manager = get_sqs_manager()
    await manager.start()

async def stop_sqs_processing():
    """Stop SQS processing (for use in FastAPI shutdown)"""
    global _sqs_manager
    if _sqs_manager:
        await _sqs_manager.stop()
