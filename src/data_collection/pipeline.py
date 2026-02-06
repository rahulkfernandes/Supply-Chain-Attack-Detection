from pathlib import Path
from src.data_collection.top_pkg_collection import TopPyPi, TopNPM


def run_collection_pipeline(
        benign_pkgs: int,
        paths_config: str,
        libraries_io_key: str
    ):
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

    npm_downloader = TopNPM(benign_pkgs, npm_pkgs_path, libraries_io_key)
    npm_downloader.fetch_top_npm()

    # TODO: Use Libraries.io for better npm list