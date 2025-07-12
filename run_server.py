#!/usr/bin/env python3
"""
Startup script for the EDGP Rules Engine API
Reads configuration from .env file
"""
import uvicorn
from app.core.config import settings

if __name__ == "__main__":
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
