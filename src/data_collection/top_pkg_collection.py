import os
import json
import time
import random
import hashlib
import requests
from tqdm import tqdm
from pathlib import Path
from typing import Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


TOP_URL = 'https://hugovk.github.io/top-pypi-packages/top-pypi-packages.json'
PYPI_URL = 'https://pypi.org/pypi/'
MAX_WORKERS = 16

class TopPyPi:
    timeout = 30 # Request timeout
    max_retries = 3
    pypi_json_endpoint = 'json'
    uni_rnd_lim = (0, 0.2) # Random time limits
    hash_algo = 'sha256'
    chunk_size = 65536 # 64kb

    def __init__(
            self, 
            num_packs: int, 
            out_dir: str | Path, 
            list_url: str = TOP_URL,
            pypi_url: str = PYPI_URL, 
            max_workers: int = MAX_WORKERS
        ):
        """
        Constructor of Top N PyPi package downloader.
        
        Args:
            num_packs: Number of top packages to be dowloaded
            out_dir: Output directory path
            list_url: URL for top pypi packages list.
                Default = hugovk.github.io/...
            pypi_url: PyPi URL
            max_workers: Maximum number of worker threads.
                Default = 16
        """
        self.num_packs = num_packs

        if not isinstance(out_dir, Path) and isinstance(out_dir, str):
            self.out_dir = Path(out_dir)
        else:
            self.out_dir = out_dir
        
        self.list_url = list_url
        self.pypi_url = pypi_url
        self.max_workers = max_workers

        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'supply-chain-collector/1.0 (+you@example.com)'}
        )
        
        self.topN_list = []
        
    def _save_to_json(self, data_dict: dict, output_file: str):
        """
        Saves dictionary to a json file.

        Args:
            data_dict: Dictionary containing data to be saved
            output_file: Path to output file
        """
        with open(output_file, 'w') as json_file:
            json.dump(data_dict, json_file, indent=4)
            json_file.close()

    def get_top_pypi(self):
        """
        Get Top PyPi packages list.

        Raises:
            ValueError: Unexpected JSON layout
        """
        resp = requests.get(self.list_url, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        # Extract last updated date
        last_update_dt = data['last_update']
        last_update_dt.replace(' ', '_')

        self._save_to_json(
            data,
            self.out_dir / f'top{self.num_packs}_pypi_{last_update_dt}.json'
        )

        # Data format: {'rows': [{...}, {...}], ....}
        # The rows are dicts keyed by column names; one common column is 'project'
        rows = data.get('rows') or []
        if not rows:
            raise ValueError('Unexpected JSON layout from hugovk feed!')
        
        self.topN_list = [row.get('project') for row in rows[:self.num_packs]]
    
    def _fetch_latest_vers(self, pkg) -> str:
        """
        Fetch latest version of the given package.

        Args:
            pkg: Package name
        
        Returns:
            version: Version number as string
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(
                    self.pypi_url + f'{pkg}/{self.pypi_json_endpoint}', 
                    timeout=self.timeout
                )
                resp.raise_for_status()
                info = resp.json().get('info', {})
                return info.get('version')
            except Exception as e:
                wait = 2 ** attempt + random.random()
                print(f'[meta] {pkg} attempt {attempt} failed: {e}. retrying in {wait:.1f}s')
                time.sleep(wait)
        return None

    def _stream_download(
            self,
            url: str, 
            out_path: str | Path, 
            expected_hash: str | None = None
        ) -> Path:  
        """
        Stream and download one package@version from PyPI..

        Args:
            url: URL for package endpoint in PyPI
            out_path: Output path where the package file must be stored
            expected_hash: Expected hash string

        Returns:
            out_path: Output path where the package file is downloaded
        
        Raises:
            ValueError: checksum mismatch
        """     
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_path.with_suffix(out_path.suffix + '.part')

        h = hashlib.new(self.hash_algo) if expected_hash else None

        # Streaming session in parts using chunk size
        with self.session.get(url, stream=True, timeout=self.timeout) as r:
            r.raise_for_status()
            with open(tmp, 'wb') as fh:
                for chunk in r.iter_content(chunk_size=self.chunk_size):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    if h:
                        h.update(chunk)

        if expected_hash:
            got = h.hexdigest()
            if got.lower() != expected_hash.lower():
                tmp.unlink(missing_ok=True)
                raise ValueError(f'checksum mismatch: expected {expected_hash} got {got}')

        tmp.rename(out_path)
        return out_path

    def _download_pkg_sdist(self, package: str, version: str) -> Dict:
        """
        Downloads sdist for package@version from PyPI via JSON metadata.

        Args:
            package: Package name
            version: Version number as string
    
        Returns:
            dwnld_info: Information and downloaded status for given package@version

        """
        dwnld_info = {
            'package': package, 'version': version, 'downloaded': None, 'message': None
        }
        try:
            # Build package@version URL
            meta_url = f'{self.pypi_url}{package}/{version}/{self.pypi_json_endpoint}'
            resp = self.session.get(meta_url, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            urls = data.get('urls', [])  # list of artifacts for this version
            # prefer sdist
            sdist = None
            for item in urls:
                if item.get('packagetype') == 'sdist':
                    sdist = item
                    break
            if sdist is None and urls:
                # fallback to first artifact
                sdist = urls[0]

            if sdist is None:
                dwnld_info.update({'downloaded': False, 'message': 'no-artifact-found'})
                return  dwnld_info

            # Extracting url, name and hash algo to downloaded package
            url = sdist['url']
            filename = sdist['filename']
            sha256 = sdist.get('digests', {}).get(self.hash_algo)
            out_path = self.out_dir / filename

            # retry loop
            for attempt in range(1, self.max_retries + 1):
                try:
                    self._stream_download(
                        url,
                        out_path,
                        expected_hash=sha256
                    )
                    dwnld_info.update({'downloaded': True, 'message': str(out_path)})
                    return dwnld_info
                except Exception as e:
                    if attempt == self.max_retries:
                        dwnld_info.update({'downloaded': False, 'message': f'download-failed:{e}'})
                        return dwnld_info
                    backoff = 2 ** attempt + random.random()
                    time.sleep(backoff)
            dwnld_info.update({'downloaded': False, 'message': 'unreachable'})
            return dwnld_info
        except Exception as e:
            dwnld_info.update({'downloaded': False, 'message': f'meta-error:{e}'})
            return dwnld_info
    
    def _process_one_pack(self, pkg: str) -> Dict:
        """
        Collects latest version of a package from PyPI and 
        then download the package file.
        This is a target function for child threads.

        Args:
            pkg: Package name
        
        Returns:
            download_info: Dictionary containing information about the package download
        """
        # fetch metadata first
        version = self._fetch_latest_vers(pkg)
        # small randomized sleep to spread requests (optional)
        time.sleep(random.uniform(self.uni_rnd_lim[0], self.uni_rnd_lim[1]))
        
        return self._download_pkg_sdist(pkg, version)

    def download_packages(self):
        """
        Downloads required number of top N packages from PyPI and 
        saves the download status report.

        Raises:
            RuntimeError: When `get_top_pypi` is not run prior to running this method
        """
        # Check if get_pypi was run
        if not self.topN_list:
            raise RuntimeError('Run `get_top_pypi` before running this method!')
        
        results = []
        # Run downloading with threads
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {
                ex.submit(self._process_one_pack, pkg): pkg for pkg in self.topN_list
            }
            for fut in tqdm(as_completed(futures), total=len(futures), desc='Downloading'):
                pkg = futures[fut]
                try:
                    results.append(fut.result())
                except Exception as e:
                    results.append(
                        {
                            'package': pkg,
                            'version': None,
                            'downloaded': False,
                            'message': f'Unexpected Error:{e}'
                        }
                    )

        # Save report
        self._save_to_json(
            results,
            self.out_dir / f'top{self.num_packs}_dwnld_report.json'
        )
        
        # Print download summary
        ok_count = 0
        for pkg_res in results:
            if pkg_res.get('downloaded'):
                ok_count += 1

        print(f'Successful: {ok_count}/{len(results)}')

    def set_timeout(self, timeout: int):
        """
        Setter method to set a different request timeout.
        
        Args:
            timeout: Number of seconds for request timeout.
        """
        self.timeout = timeout

    def set_max_retries(self, max_retries: int):
        """
        Setter method to set a maximum number of request retries.

        Args:
            max_retries: Maximum number of retries
        """
        self.max_retries = max_retries
    
    def set_max_workers(self, max_workers: int):
        """
        Setter method to set maximum number of worker threads.

        Args:
            max_workers: Maximum number of worker threads
        """
        self.max_workers = max_workers
    
    def set_uni_rnd_lim(self, uni_rnd_lim: Tuple[float, float]):
        """
        Setter method to set the limits of uniform random time.
        This random time is used to add delay between requests.

        Args:
            uni_rnd_lim: (start, end)
        """
        self.uni_rnd_lim = uni_rnd_lim
    
    def set_hash_algo(self, hash_algo: str):
        """
        Setter method to set hash algorithm to be used for package metadata.

        Args:
            hash_algo: Hash algorithm type as string
        """
        self.hash_algo = hash_algo
    
    def set_chunk_size(self, chunk_size: int):
        """
        Setter method to set chunk size for stream downloading

        Args:
            chunk_size: Chunk size as int
        """
        self.chunk_size = chunk_size