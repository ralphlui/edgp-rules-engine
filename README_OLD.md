# edgp-rules-engine

## Overview
The `edgp-rules-engine` is a FastAPI microservice designed for data quality validation using Great Expectations rules. It provides a RESTful API to validate datasets against predefined rules and returns detailed validation results.

## Features
- 🎯 **16+ Validation Rules**: Comprehensive set of data quality validators based on Great Expectations
- 📊 **Detailed Reporting**: Element counts, error percentages, and sample data in results
- ⚙️ **Environment Configuration**: Server settings via `.env` file
- 🧪 **Comprehensive Testing**: Full test coverage with pytest
- 📚 **Auto-generated Documentation**: Interactive API docs with FastAPI

## Project Structure
```
edgp-rules-engine/
├── app/
│   ├── main.py                    # FastAPI application and server startup
│   ├── api/
│   │   └── routes.py             # API endpoints
│   ├── core/
│   │   └── config.py             # Configuration management
│   ├── models/                   # Pydantic data models
│   │   ├── rule.py
│   │   ├── validation_request.py
│   │   └── validation_response.py
│   ├── rules/
│   │   └── expectation_rules.py  # Sample rules data
│   └── validators/               # Individual validator functions
│       ├── validator.py          # Main validator logic
│       ├── validator_registry.py # Validator registry
│       └── expect_*.py          # Individual validation functions
├── tests/                        # Test files
├── .env                         # Environment configuration
├── requirements.txt
└── README.md
```

## Installation
1. Clone the repository:
   ```
   git clone <repository-url>
   cd edgp-rules-engine
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration
The application reads configuration from a `.env` file in the root directory:

```env
# Server Configuration
HOST=localhost
PORT=8008

# Environment
ENVIRONMENT=development

# API Configuration
API_TITLE=EDGP Rules Engine API
API_VERSION=1.0.0
API_DESCRIPTION=Data Quality Validation API using Great Expectations rules
```

## Running the Application

### Option 1: Using main.py (Recommended)
```bash
python app/main.py
```

### Option 2: Using uvicorn directly
```bash
uvicorn app.main:app --host localhost --port 8008 --reload
```

**Note:** You can use environment variables or specific values:
- With specific values: `uvicorn app.main:app --host localhost --port 8008 --reload`
- With environment variables: `uvicorn app.main:app --host $HOST --port $PORT --reload` (Unix/Mac)

The server will start and display:
- 🌐 **API**: http://localhost:8008
- 📚 **Documentation**: http://localhost:8008/docs
- 📋 **OpenAPI Spec**: http://localhost:8008/openapi.json

## API Endpoints
- `GET /api/rules`: Retrieve all available expectation rules
- `POST /api/validation`: Validate data against specified rules

### Example Validation Request
```json
{
  "rules": [
    {
      "rule_name": "ExpectColumnToExist",
      "column_name": "id"
    },
    {
      "rule_name": "ExpectColumnValuesToBeBetween",
      "column_name": "age",
      "value": {"min_value": 18, "max_value": 65}
    }
  ],
  "dataset": [
    {"id": 1, "name": "John", "age": 25},
    {"id": 2, "name": "Jane", "age": 30}
  ]
}
```

## Testing

### Run all tests
```bash
pytest tests/ -v
```

### Run specific test files
```bash
# Test validators
pytest tests/test_validators_pytest.py -v

# Test rules
pytest tests/test_rules.py -v

# Run standalone test script
python tests/test_validators.py
```

## Available Validators
The system includes 16+ data quality validators:

- **Column Existence**: `ExpectColumnToExist`
- **Data Types**: `ExpectColumnValuesToBeOfType`
- **Null Checks**: `ExpectColumnValuesToNotBeNone`
- **Uniqueness**: `ExpectColumnValuesToBeUnique`, `ExpectCompoundColumnsToBeUnique`
- **Value Ranges**: `ExpectColumnValuesToBeBetween`, `ExpectColumnValuesToBeGreaterThan`, `ExpectColumnValuesToBeLessThan`
- **Set Membership**: `ExpectColumnValuesToBeInSet`, `ExpectColumnValuesToNotBeInSet`
- **Pattern Matching**: `ExpectColumnValuesToMatchRegex`, `ExpectColumnValuesToBeValidEmail`
- **Statistical**: `ExpectColumnMeanToBeBetween`
- **Table Level**: `ExpectTableRowCountToBeBetween`
- And more...

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.