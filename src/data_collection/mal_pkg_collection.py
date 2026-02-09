from pathlib import Path
from src.data_collection.top_pkg_collection import ParentDownloader

class MaliciousPyPi(ParentDownloader):
    def __init__(
            self, num_packs: int, out_dir: str | Path, list_url: str, **kwargs
        ):
        super().__init__(num_packs, out_dir, list_url, **kwargs)

    def fetch_top_packages(self):
        """
        Method to fetch a list of top packages from a well known top list.
        Must be implemented in subclass.
        """
        pass

    def download_packages(self):
        """
        Method to download and orchestrate storage.
        Must be implmented in subclass.
        """
        pass