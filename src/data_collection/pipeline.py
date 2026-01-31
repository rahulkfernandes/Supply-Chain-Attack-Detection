from pathlib import Path
from src.data_collection.top_pkg_collection import TopPyPi


def run_collection_pipeline(paths_config: str):
    
    raw_path = Path(paths_config['data']['pypi_dir'])
    
    top_pypi = TopPyPi(5, raw_path) # Change to required number of packages
    top_pypi.get_top_pypi()
    top_pypi.download_packages()