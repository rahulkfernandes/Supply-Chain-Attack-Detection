from pathlib import Path
from src.data_collection.top_py_collection import TopPyPi


def run_collection_pipeline(paths_config: str):
    
    raw_path = Path(paths_config['data']['raw_dir'])
    
    top_pypi = TopPyPi(5, raw_path)
    top_pypi.get_top_pypi()
    top_pypi.download_packages()