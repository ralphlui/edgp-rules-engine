# edgp-rules-engine

## Overview
The `edgp-rules-engine` is a FastAPI microservice designed for managing expectation rules. It provides a RESTful API to create, retrieve, and manage rules that define expectations for data validation.

## Project Structure
```
edgp-rules-engine
├── app
│   ├── __init__.py
│   ├── main.py
│   ├── api
│   │   ├── __init__.py
│   │   └── routes.py
│   ├── core
│   │   ├── __init__.py
│   │   └── config.py
│   ├── models
│   │   ├── __init__.py
│   │   └── rule.py
│   └── rules
│       ├── __init__.py
│       └── expectation_rules.py
├── tests
│   ├── __init__.py
│   └── test_rules.py
├── .env
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
- Create a `.env` file in the root directory to configure environment variables. For example:
  ```
  PORT=8000
  HOST=localhost
  ```

## Running the Application
To start the FastAPI application, run:
```
uvicorn app.main:app --host $HOST --port $PORT --reload
```

## API Endpoints
- `GET /api/rules`: Retrieve all expectation rules. The response includes rule name, column name, and the value to be checked.

## Testing
To run the tests, use:
```
pytest tests/
```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.