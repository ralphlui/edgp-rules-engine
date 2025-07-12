from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
import time
import logging
from app.api.routes import router
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Conditional import for SQS functionality
try:
    from app.api.sqs_routes import router as sqs_router
    from app.sqs import start_sqs_processing, stop_sqs_processing, get_sqs_manager
    SQS_AVAILABLE = True
    logger.info("✅ SQS functionality available")
except ImportError as e:
    logger.warning(f"⚠️ SQS functionality not available: {e}")
    SQS_AVAILABLE = False
    sqs_router = None

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description
)

# Add CORS middleware - this must be added before other middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Log the request
    logger.info(f"🔍 {request.method} {request.url}")
    if origin := request.headers.get("origin"):
        logger.info(f"🌐 Origin: {origin}")
    
    response = await call_next(request)
    
    # Log the response
    process_time = time.time() - start_time
    logger.info(f"✅ {request.method} {request.url} - {response.status_code} - {process_time:.2f}s")
    
    return response

# Add health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint to verify the service is running"""
    return {
        "status": "healthy",
        "service": settings.api_title,
        "version": settings.api_version,
        "environment": settings.environment,
        "cors_enabled": True,
        "allowed_origins": settings.allowed_origins
    }

app.include_router(router)

# Include SQS router if available
if SQS_AVAILABLE and sqs_router:
    app.include_router(sqs_router)
    logger.info("✅ SQS routes included")

# Add startup and shutdown events for SQS
@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    logger.info("🚀 Application starting up...")
    
    if SQS_AVAILABLE:
        try:
            # Check if SQS is configured and user wants auto-start
            from app.sqs.config import SQSSettings
            sqs_settings = SQSSettings()
            
            if sqs_settings.auto_start_workers:
                logger.info("🔄 Auto-starting SQS workers...")
                # Start SQS processing in background
                import asyncio
                asyncio.create_task(start_sqs_processing())
            else:
                logger.info("⏸️ SQS workers not auto-started (auto_start_workers=False)")
                
        except Exception as e:
            logger.error(f"❌ Failed to start SQS processing: {e}")
    
    logger.info("✅ Application startup complete")

@app.on_event("shutdown") 
async def shutdown_event():
    """Application shutdown event"""
    logger.info("🛑 Application shutting down...")
    
    if SQS_AVAILABLE:
        try:
            await stop_sqs_processing()
            logger.info("✅ SQS processing stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping SQS processing: {e}")
    
    logger.info("✅ Application shutdown complete")

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print(f"🚀 Starting {settings.api_title}")
    print("=" * 60)
    print(f"Version: {settings.api_version}")
    print(f"Environment: {settings.environment}")
    print(f"Host: {settings.host}")
    print(f"Port: {settings.port}")
    print(f"URL: http://{settings.host}:{settings.port}")
    print(f"Docs: http://{settings.host}:{settings.port}/docs")
    print(f"Environment file: .env")
    print(f"CORS: ✅ Enabled ({len(settings.allowed_origins)} origins)")
    print("=" * 60)
    
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
        log_level="info"
    )