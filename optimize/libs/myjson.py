import ijson
def ijson_number(str_value):
    number = float(str_value)
    if not ('.' in str_value or 'e' in str_value or 'E' in str_value):
        number = int(number)
    return number
ijson.common.number = ijson_number

