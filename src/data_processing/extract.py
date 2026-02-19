from pathlib import Path

class PyPIDataExtractor:
    def __init__(self, in_dir: str | Path):
        for file in in_dir.iterdir():
            print(file)