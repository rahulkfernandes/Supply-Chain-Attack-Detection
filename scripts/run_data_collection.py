import os
import sys
import signal
import argparse
from dotenv import load_dotenv
from src.utils.io import load_config
from src.data_collection.pipeline import run_collection_pipeline

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
    load_dotenv()

    try:

        paths_config = load_config(os.path.join('config', 'paths.json'))

        libraries_io_key = os.getenv('LIBRARIES_IO_KEY')
        
        run_collection_pipeline(args.num_pkgs, paths_config, libraries_io_key)
    
    except SystemExit:
        # Already handled by signal handler
        pass
    except Exception as e:
        print(f"\nPipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)  
