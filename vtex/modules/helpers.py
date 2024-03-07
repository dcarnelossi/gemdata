from datetime import datetime, timedelta

def increment_one_day(d1):
    
    if not isinstance(d1, datetime):
        try:
            print("A variável é do tipo datetime.")
            d1 = datetime.strptime(d1, '%Y-%m-%d')
        except Exception as e:
            print(d1)
            print(f"Não foi possivel converter a data - {e}")
            return False
        
        
        d1 = datetime.strptime(d1, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Obtém a data de início como a data atual com a hora T01:59:59.999Z
    end_date = (d1.replace(hour=1, minute=59, second=59, microsecond=999000) + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    start_date = (d1.replace(hour=2, minute=00, second=00, microsecond=000000)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    print(start_date, end_date)

    return start_date, end_date



def flatten_json(json_data, sep='_', parent_key=''):
    """
    Flatten a nested JSON object by concatenating keys with specified separator.

    Parameters:
    - json_data: The input JSON object.
    - sep: Separator to use for concatenating keys. Default is '_'.
    - parent_key: Used for recursion, specifying the parent key.

    Returns:
    A flattened dictionary.
    """
    flattened = {}
    for key, value in json_data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened.update(flatten_json(value, sep, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                item_key = f"{new_key}{sep}{i}"
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_json({str(i): item}, sep, new_key))
                else:
                    flattened[item_key] = item
        else:
            flattened[new_key] = value
    return flattened

# inicia = datetime.now()

# decrement_days("2023-12-25T01:59:59.999000Z")
