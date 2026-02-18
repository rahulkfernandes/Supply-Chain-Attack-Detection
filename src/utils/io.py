import json

def load_config(path):
    with open(path, 'r') as file:
        config = json.load(file)
        file.close()
    
    return config

def save_to_json(data_dict: dict, output_file: str):
    """
    Saves dictionary to a json file.

    Args:
        data_dict (Dict): Dictionary containing data to be saved
        output_file (str | Path): Path to output file
    """
    with open(output_file, 'w') as json_file:
        json.dump(data_dict, json_file, indent=4)
        json_file.close()