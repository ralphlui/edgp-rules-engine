"""
Test CORS configuration for the FastAPI application.
"""
import requests
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

def test_cors_headers():
    """Test that CORS headers are properly set"""
    try:
        # Test with a simple GET request to the health endpoint
        response = requests.get(
            "http://localhost:8008/health",
            headers={"Origin": "http://localhost:3000"}
        )
        
        print("=== CORS Header Test Results ===")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        # Check for CORS headers
        cors_headers = {
            'Access-Control-Allow-Origin': response.headers.get('Access-Control-Allow-Origin'),
            'Access-Control-Allow-Methods': response.headers.get('Access-Control-Allow-Methods'),
            'Access-Control-Allow-Headers': response.headers.get('Access-Control-Allow-Headers'),
            'Access-Control-Allow-Credentials': response.headers.get('Access-Control-Allow-Credentials')
        }
        
        print("\n=== CORS Headers ===")
        for header, value in cors_headers.items():
            if value:
                print(f"✅ {header}: {value}")
            else:
                print(f"❌ {header}: Not present")
        
        # Test preflight request (OPTIONS)
        print("\n=== Testing Preflight Request ===")
        options_response = requests.options(
            "http://localhost:8008/validate",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "Content-Type"
            }
        )
        
        print(f"OPTIONS Status Code: {options_response.status_code}")
        print("OPTIONS CORS Headers:")
        for header, value in options_response.headers.items():
            if header.lower().startswith('access-control'):
                print(f"  {header}: {value}")
                
    except requests.exceptions.ConnectionError:
        print("❌ Server is not running. Please start the server first with:")
        print("   python run_server.py")
        print("   or")
        print("   python app/main.py")
    except Exception as e:
        print(f"❌ Error testing CORS: {e}")

if __name__ == "__main__":
    test_cors_headers()
