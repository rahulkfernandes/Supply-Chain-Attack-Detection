import os
from scripts.utils import load_config
from src.data_collection.pipeline import run_collection_pipeline

if __name__ == '__main__':
    paths_config = load_config(os.path.join('config', 'paths.json'))

    run_collection_pipeline(10, paths_config)  # Change to required number of packages

