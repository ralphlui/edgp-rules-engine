


from app.models.validation_request import ValidationRequest
from app.models.validation_response import ValidationResponse


def data_validator(request: ValidationRequest) -> ValidationResponse:

    rules = request.rules
    data = request.data
    
    return ValidationResponse(results=validation_results)