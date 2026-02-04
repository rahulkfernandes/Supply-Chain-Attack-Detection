import os
import argparse
from scripts.utils import load_config
from src.data_collection.pipeline import run_collection_pipeline

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Data Collection Pipeline To Download Top PyPI and NPM Packages'
    )

    parser.add_argument(
        '-n',
        '--num_pkgs',
        type=int,
        help="Number of benign packages to be downloaded."
    )
    args = parser.parse_args()
    paths_config = load_config(os.path.join('config', 'paths.json'))
    
    run_collection_pipeline(args.num_pkgs, paths_config)  # Change to required number of packages
