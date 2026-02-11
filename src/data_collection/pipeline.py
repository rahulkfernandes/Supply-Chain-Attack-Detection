import sys
import signal
from pathlib import Path
from src.data_collection.top_pkg_collection import TopPyPi, TopNPM

_interrupted = False

def signal_handler(signum, frame):
    """Handle Ctrl+C and other interrupts"""
    global _interrupted
    
    if _interrupted:  # Already handling an interrupt
        print("\nForce quitting immediately...")
        sys.exit(1)
    
    _interrupted = True
    print(f"\n{'='*60}")
    print("INTERRUPTED - Cleaning up before exit...")
    print("Press Ctrl+C again to force quit immediately.")
    print(f"{'='*60}")
    
    # Call your cleanup function
    cleanup_on_interrupt()
    
    # Exit gracefully
    sys.exit(130 if signum == signal.SIGINT else 1)

def cleanup_on_interrupt():
    """Your cleanup logic for interrupts"""
    print('\nCleaning up...')
    
    # Kill child processes (if multiprocessing)
    try:
        import psutil
        current = psutil.Process()
        children = current.children(recursive=True)
        for child in children:
            try:
                child.terminate()
            except:
                pass
        print(f'Terminated {len(children)} child processes.')
    except:
        pass

    print('Cleanup complete.')

# Set up signal handlers
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

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
    
    try:
        pypi_pkgs_path = Path(paths_config['data']['pypi_dir'])
        npm_pkgs_path = Path(paths_config['data']['npm_dir'])
        # raw_dir = Path(paths_config['data']['raw_dir'])
        
        print('\n', '-'*20, ' Downloading PyPI Packages', '-'*20)
        pypi_downloader = TopPyPi(benign_pkgs, pypi_pkgs_path, max_workers=64)
        pypi_downloader.fetch_top_packages()
        pypi_downloader.download_packages()

        print('\n', '-'*20, ' Downloading NPM Packages', '-'*20)
        npm_downloader = TopNPM(benign_pkgs, npm_pkgs_path, libraries_io_key, max_workers=64)
        npm_downloader.fetch_top_packages()
        npm_downloader.download_packages()

    except SystemExit:
        # Already handled by signal handler
        pass
    except Exception as e:
        print(f"\nPipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)  