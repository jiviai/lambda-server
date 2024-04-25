import decimal

def flatten_list_of_dicts(
    list_of_dicts
):
    output = {}
    for dictionary in list_of_dicts:
        output[dictionary['caseStudyNo']] = {
            "caseStudyText": dictionary['caseStudyText'],
            "diagnosis": dictionary['diagnosis'],
            "conv":dictionary['conv'],
            "summary_history":dictionary['summary_history'],
            "jivi_system_summary":dictionary['jivi_system_summary'],
            "jivi_system_differential_diagnosis":dictionary['jivi_system_differential_diagnosis'],
            "evaluated_differential_diagnosis":dictionary['evaluated_differential_diagnosis']
        }
    return output

# Convert float values to Decimal
def convert_float_to_decimal(obj):
    if isinstance(obj, float):
        return decimal.Decimal(str(obj))
    elif isinstance(obj, dict):
        return {key: convert_float_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_float_to_decimal(item) for item in obj]
    return obj