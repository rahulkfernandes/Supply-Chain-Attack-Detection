from pathlib import Path
from src.data_processing.extract import PyPIDataExtractor

def run_builder_pipeline(paths_config: dict, data_store: str|Path=''):
    """
    Run dataset builder pipeline to unpack batch compressed packages with 
    their meta data, extract relevant features.

    Args:
        paths_config (dict): Dictionary containing paths to directories and files
        data_store (str): Path to directory where the data is stored. Default = None 
            (can be different if raw packages are stored in a different location)
    """
    print('\n', '='*20, 'Dataset Builder Pipeline', '='*20)
    
    pypi_pkgs_path = Path(paths_config['data']['pypi_dir'])
    
    # Logic to build path where raw data is stored
    if not data_store:
        print('Using <project root>/data/raw to extract data.') 
    elif isinstance(data_store, Path):
        print('Using <ext data store>/data/raw to extract data.') 
        pypi_pkgs_path = data_store / pypi_pkgs_path
    elif isinstance(data_store, str):
        print('Using <ext data store>/data/raw to extract data.') 
        pypi_pkgs_path = Path(data_store) / pypi_pkgs_path

    pypi_extractor = PyPIDataExtractor(pypi_pkgs_path)