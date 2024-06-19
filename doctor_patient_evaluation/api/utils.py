def merge_dicts_to_text(data):
    text = ""
    for item in data:
        text += f"{item.get('name',None)} - {item.get('description',None)}\n"
    return text