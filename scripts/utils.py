import json

def load_config(path):
    with open(path, 'r') as file:
        config = json.load(file)
        file.close()
    
    return config