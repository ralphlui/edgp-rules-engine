# Test Suite Documentation

This directory contains the comprehensive test suite for the EDGP Rules Engine.

## Test Structure

### Core Test Files

- **`test_core.py`** - Core functionality tests including validation models, API integration, SQS functionality, and performance tests
- **`test_validators.py`** - Individual validator function tests with proper error handling and edge cases
- **`test_all_validators.py`** - Parametrized tests covering all 20+ validation rules with success/failure scenarios
- **`test_sqs.py`** - Complete SQS integration tests including models, client, processor, and manager
- **`test_rules.py`** - Rule model validation tests
- **`test_cors.py`** - CORS functionality tests

### Test Coverage

#### Core Validation System ✅
- ValidationRequest/ValidationResponse models
- Data validation workflow
- Error handling and graceful degradation
- Performance with large datasets

#### Validator Registry ✅  
- 20+ validation rules (expect_column_to_exist, expect_column_values_to_be_between, etc.)
- Rule parameter validation
- Success and failure scenarios
- Custom validator implementations

#### SQS Integration ✅
- Message models (SQSValidationRequest, SQSValidationResponse) 
- Client receive/send operations
- Message processor with worker management
- SQS manager with async task handling
- Error handling and retry logic

#### API & Configuration ✅
- FastAPI route imports
- CORS configuration
- Environment settings
- Configuration validation

### Test Results

**Total: 61 passed, 4 skipped**
- All core functionality tests passing
- All validator tests passing with proper Great Expectations fallback
- All SQS integration tests passing
- All API and configuration tests passing

### Key Features

1. **Unified Model Architecture**: All tests use the new `app.models.validation` unified models
2. **Graceful Degradation**: Tests handle Great Expectations Python 3.13 compatibility issues gracefully
3. **Comprehensive Coverage**: Tests cover success cases, failure cases, edge cases, and error conditions
4. **Performance Testing**: Large dataset and multi-rule validation performance tests
5. **Async Support**: Full async/await support for SQS manager and processor tests

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test files
python -m pytest tests/test_core.py -v
python -m pytest tests/test_validators.py -v
python -m pytest tests/test_sqs.py -v

# Run with coverage
python -m pytest tests/ --cov=app --cov-report=html
```

### Test Categories

- **Unit Tests**: Individual function and class testing
- **Integration Tests**: Full workflow validation
- **Performance Tests**: Large dataset and multi-rule scenarios
- **Error Handling Tests**: Graceful failure and recovery scenarios
