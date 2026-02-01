from pathlib import Path
from src.data_collection.top_pkg_collection import TopPyPi


def run_collection_pipeline(benign_pkgs: int, paths_config: str):
    print('\n', '='*20, ' Data Collection Pipeline ', '='*20)
    
    if not benign_pkgs or not paths_config:
        raise RuntimeError(
            'Number of benign packages and paths_config must be provided!'
        )
    
    pypi_pkgs_paths = Path(paths_config['data']['pypi_dir'])
    # raw_dir = Path(paths_config['data']['raw_dir'])
    
    top_pypi = TopPyPi(benign_pkgs, pypi_pkgs_paths)
    top_pypi.get_top_pypi()
    top_pypi.download_packages()