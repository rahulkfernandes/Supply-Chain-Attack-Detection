from pathlib import Path
from src.data_collection.top_pkg_collection import TopPyPi, TopNPM


def run_collection_pipeline(
        benign_pkgs: int,
        paths_config: dict,
        libraries_io_key: str
    ):
    """
    Run data collection pipeline to download meta data and packages from
    PyPI and npm.

    Args:
        benign_pkgs (int): Number of benign packages to be downloaded
        paths_config (dict): Dictionary containing paths to directories and files
        libraries_io_key (str): API key to Libraries.io
    """
    print('\n', '='*20, ' Data Collection Pipeline ', '='*20)
    
    if not benign_pkgs or not paths_config:
        raise RuntimeError(
            'Number of benign packages and paths_config must be provided!'
        )
    
    pypi_pkgs_path = Path(paths_config['data']['pypi_dir'])
    npm_pkgs_path = Path(paths_config['data']['npm_dir'])
    # raw_dir = Path(paths_config['data']['raw_dir'])
    
    pypi_downloader = TopPyPi(benign_pkgs, pypi_pkgs_path)
    pypi_downloader.fetch_top_pypi()
    pypi_downloader.download_packages()

    # npm_downloader = TopNPM(benign_pkgs, npm_pkgs_path, libraries_io_key)
    # # npm_downloader.fetch_top_packages()
    # npm_downloader.download_packages()