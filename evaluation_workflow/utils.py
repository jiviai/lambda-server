import decimal

def validate_dynamo_save_payload(
    obj
):
    if isinstance(obj, float):
        return decimal.Decimal(str(obj))
   
    elif isinstance(obj, dict):
        return {
            key: validate_dynamo_save_payload(value) for key, value in obj.items()
        }
    
    elif isinstance(obj, list):
        return [
            validate_dynamo_save_payload(item) for item in obj
        ]
    
    return obj
    