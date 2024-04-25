def merge_dicts_to_text(data):
    text = ""
    for item in data:
        text += f"{item['name']} - {item.get('description',None)} - {item['isConfirmed']}\n"
    return text