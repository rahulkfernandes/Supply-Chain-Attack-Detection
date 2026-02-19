import os
from dotenv import load_dotenv
from src.utils.io import load_config
from src.data_processing.pipeline import run_builder_pipeline

load_dotenv()

if __name__ == '__main__':
    paths_config = load_config(os.path.join('config', 'paths.json'))
    
    run_builder_pipeline(paths_config, os.getenv('DATA_STORE'))
