# CORS Configuration Documentation

## Overview
This microservice is configured with Cross-Origin Resource Sharing (CORS) support to prevent CORS errors when accessing the API from web browsers or frontend applications.

## Configuration Files

### Development Configuration (`.env`)
- Allows common development server origins (React, Vue, Angular, etc.)
- Includes wildcard (`*`) for development flexibility
- Allows all HTTP methods and headers

### Production Configuration (`.env.production`)
- More restrictive origin list
- Should be customized with your actual domain(s)
- Better security posture for production environments

## CORS Settings

### Allowed Origins
The following origins are allowed by default in development:
- `http://localhost:3000` - React default
- `http://localhost:3001` - React alternative port
- `http://localhost:8080` - Vue.js default
- `http://localhost:4200` - Angular default
- `http://localhost:5173` - Vite default
- `http://127.0.0.1:*` - localhost alternatives
- `*` - Wildcard (development only)

### Allowed Methods
- GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH

### Allowed Headers
- All headers (`*`) - configurable per environment

### Credentials
- Enabled (`allow_credentials: true`) for cookie/auth support

## Testing CORS

### Health Check Endpoint
```bash
curl -X GET "http://localhost:8008/health" \
  -H "Origin: http://localhost:3000" \
  -H "Accept: application/json"
```

### Preflight Request Test
```bash
curl -X OPTIONS "http://localhost:8008/validate" \
  -H "Origin: http://localhost:3000" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: Content-Type"
```

### Automated Test
```bash
python tests/test_cors.py
```

## Frontend Integration

### JavaScript/Fetch
```javascript
fetch('http://localhost:8008/validate', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  credentials: 'include', // Include cookies if needed
  body: JSON.stringify({
    data: [...],
    rules: [...]
  })
})
.then(response => response.json())
.then(data => console.log(data));
```

### Axios
```javascript
import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:8008',
  withCredentials: true, // Include cookies if needed
});

api.post('/validate', {
  data: [...],
  rules: [...]
});
```

## Production Deployment

### Environment Variables
For production, set these environment variables:

```bash
ENVIRONMENT=production
ALLOWED_ORIGINS=["https://yourdomain.com","https://www.yourdomain.com"]
```

### Docker
If using Docker, expose the port and set environment:

```dockerfile
EXPOSE 8008
ENV ENVIRONMENT=production
ENV ALLOWED_ORIGINS=["https://yourdomain.com"]
```

### Security Considerations

1. **Never use wildcard (`*`) in production**
2. **Specify exact domains** for allowed origins
3. **Use HTTPS** in production
4. **Limit allowed methods** to only what's needed
5. **Review headers** that are exposed/allowed

## Troubleshooting

### Common CORS Errors

1. **"Access to fetch at '...' has been blocked by CORS policy"**
   - Check if your origin is in the allowed_origins list
   - Verify the server is running with CORS enabled

2. **"Preflight request doesn't pass access control check"**
   - Ensure OPTIONS method is allowed
   - Check Access-Control-Request-Headers are permitted

3. **"Credentials flag is 'true', but access control allow origin is '*'"**
   - Remove wildcard from allowed_origins when using credentials
   - Specify exact origins instead

### Debug Mode
The server logs all requests and origins when running in development mode. Check the console output for CORS-related information.

## API Endpoints

### Health Check
- **GET** `/health` - Returns service status and CORS configuration
- Useful for verifying CORS headers are set correctly

### Validation
- **POST** `/validate` - Main validation endpoint
- **GET** `/rules` - Get available rules
- **GET** `/docs` - OpenAPI documentation (Swagger UI)

## Updates

To modify CORS settings:

1. Edit `.env` file for development
2. Edit `.env.production` for production
3. Restart the server for changes to take effect
4. Test with `python tests/test_cors.py`
