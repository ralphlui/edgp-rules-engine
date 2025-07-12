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
    print("=" * 60)
    
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
        log_level="info"
    )