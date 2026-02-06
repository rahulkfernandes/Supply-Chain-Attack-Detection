import json
import time
import math
import shlex
import shutil
import random
import hashlib
import tarfile
import requests
import subprocess
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed


TOP_PYPI_URL = 'https://hugovk.github.io/top-pypi-packages/top-pypi-packages.json'
PYPI_URL = 'https://pypi.org/pypi/'
TOP_NPM_URL = 'https://libraries.io/api/search'

MAX_WORKERS = 16

class ParentDownloader(ABC):
    timeout = 30 # Request timeout
    max_retries = 3
    num_batches = 10
    uni_rnd_lim = (0, 0.5) # Random time limits

    def __init__(
            self,
            num_packs: int,
            out_dir: str|Path,
            list_url: str,
            max_workers: int
        ):
        """
        Constructor for parent downloader abstract class containing 
        common functionality of package downloading.
        
        Args:
            num_packs (int): Number of top packages to be dowloaded
            out_dir (str | Path): Output directory path
            list_url (str): URL for top packages list
            max_workers (int): Maximum number of worker threads.
        """
        self.num_packs = num_packs
        if self.num_packs % 10 != 0 :
            raise ValueError(
                'Number of packages to be downloaded must be multiple of 10.'
            )
        
        if not isinstance(out_dir, Path) and isinstance(out_dir, str):
            self.out_dir = Path(out_dir)
        else:
            self.out_dir = out_dir
        
        self.list_url = list_url
        self.max_workers = max_workers
    
    @staticmethod
    def _save_to_json(data_dict: Dict, output_file: str|Path):
        """
        Saves dictionary to a json file.

        Args:
            data_dict (Dict): Dictionary containing data to be saved
            output_file (str | Path): Path to output file
        """
        with open(output_file, 'w') as json_file:
            json.dump(data_dict, json_file, indent=4)
            json_file.close()
    
    @abstractmethod
    def download_packages():
        """
        Method to download and orchestrate storage.
        Must be implmented in subclass
        """
        pass

    def set_timeout(self, timeout: int):
        """
        Setter method to set a different request timeout.
        
        Args:
            timeout (int): Number of seconds for request timeout.
        """
        self.timeout = timeout

    def set_max_retries(self, max_retries: int):
        """
        Setter method to set a maximum number of request retries.

        Args:
            max_retries (int): Maximum number of retries
        """
        self.max_retries = max_retries
    
    def set_max_workers(self, max_workers: int):
        """
        Setter method to set maximum number of worker threads.

        Args:
            max_workers (int): Maximum number of worker threads
        """
        self.max_workers = max_workers
    
    def set_uni_rnd_lim(self, uni_rnd_lim: Tuple[float, float]):
        """
        Setter method to set the limits of uniform random time.
        This random time is used to add delay between requests.

        Args:
            uni_rnd_lim (Tuple[float, float]): (start, end)
        """
        self.uni_rnd_lim = uni_rnd_lim
    
    def set_num_batches(self, num_batches: int):
        """
        Setter method to set batch size for downlaoding and compression.
        num_packs % num_batches must be = 0.

        Args:
            batch_size (int): size of each batch as integer
        """
        self.num_batches = num_batches

class TopPyPi(ParentDownloader):
    pypi_json_endpoint = 'json'
    hash_algo = 'sha256'
    chunk_size = 65536 # 64kb

    def __init__(
            self, 
            num_packs: int,
            out_dir: str | Path, 
            list_url: str = TOP_PYPI_URL,
            max_workers: int = MAX_WORKERS,
            pypi_url: str = PYPI_URL
        ):
        """
        Constructor for Top N PyPi package downloader.
        
        Args:
            num_packs (int): Number of top packages to be dowloaded
            out_dir (str | Path): Output directory path
            list_url (str): URL for top pypi packages list.
                Default = hugovk.github.io/...
            pypi_url (str): PyPi URL
            max_workers (int): Maximum number of worker threads.
                Default = MAX_WORKERS (16)
        """
        super().__init__(num_packs, out_dir, list_url, max_workers)
        
        self.pypi_url = pypi_url

        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'supply-chain-collector/1.0 (+you@example.com)'}
        )
        
        self.topN_list = []

    def fetch_top_pypi(self):
        """
        Fetch Top PyPi packages list from hugovk's top PyPI packages list.
        Downloads and saves the entire list with the data from hugovk's, 
        but slices the package names list to required number of packages.

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
            self.out_dir / f'top_pypi_list{last_update_dt}.json'
        )

        # Data format: {'rows': [{...}, {...}], ....}
        # The rows are dicts keyed by column names; one common column is 'project'
        rows = data.get('rows') or []
        if not rows:
            raise ValueError('Unexpected JSON layout from hugovk feed!')
        
        self.topN_list = [row.get('project') for row in rows[:self.num_packs]]
    
    def _fetch_latest_vers(self, pkg: str) -> str:
        """
        Fetch latest version of the given package.

        Args:
            pkg (str): Package name
        
        Returns:
            version (str): Version number as string
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
                # print(f'[meta] {pkg} attempt {attempt} failed: {e}. retrying in {wait:.1f}s')
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
            url (str): URL for package endpoint in PyPI
            out_path (str | Path): Output path where the package file must be stored
            expected_hash (str | None): Expected hash string

        Returns:
            out_path (Path): Output path where the package file is downloaded
        
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

    def _download_pkg_sdist(
            self, package: str, version: str, dwnld_dir: Path
        ) -> Dict:
        """
        Downloads sdist for package@version from PyPI via JSON metadata.

        Args:
            package (str): Package name
            version (str): Version number as string
            dwnld_dir (Path) : Path to where the downloaded files should be stored
    
        Returns:
            dwnld_info (Dict): Information and downloaded status for given package@version

        """
        dwnld_info = {
            'package': package, 'version': version, 'downloaded': None, 'message': None
        }
        if not version:
                dwnld_info.update({'downloaded': False, 'message': 'no version'})
                return dwnld_info
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
            out_path = dwnld_dir / filename

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
                    wait = 2 ** attempt + random.random()
                    time.sleep(wait)
            dwnld_info.update({'downloaded': False, 'message': 'unreachable'})
            return dwnld_info
        except Exception as e:
            dwnld_info.update({'downloaded': False, 'message': f'meta-error:{e}'})
            return dwnld_info
    
    def _process_one_pack(self, pkg: str, output_dir: Path) -> Dict:
        """
        Collects latest version of a package from PyPI and 
        then download the package file.
        This is a target function for child threads.

        Args:
            pkg (str): Package name
            output_dir (Path): Path to where the downloaded files should be stored
        
        Returns:
            download_info (Dict): Dictionary containing information 
                about the package download
        """
        # fetch metadata first
        version = self._fetch_latest_vers(pkg)
        # small randomized sleep to spread requests (optional)
        time.sleep(random.uniform(self.uni_rnd_lim[0], self.uni_rnd_lim[1]))
        
        return self._download_pkg_sdist(pkg, version, output_dir)

    @staticmethod
    def _sha256_file(path: str|Path, chunk_size: int = 65536) -> str:
        """
        Creates a sha256 checksum to be used for compression integrity validation.

        Args:
            path (str | Path): Path to directory or file
            chunk_size (int): Size of chunks streamed. Default = 64kb (65536)
        
        Returns:
            hexdigest (str): Digest value of a string of hexadecimal values
        """
        h = hashlib.sha256()
        with open(path, 'rb') as fh:
            for chunk in iter(lambda: fh.read(chunk_size), b''):
                h.update(chunk)
        return h.hexdigest()

    def _compress_and_cleanup_batch(
            self, batch_dir: Path, compression_pref: str = 'zstd'
        ):
        """
        Compress a batch directory into a compressed archive.

        If ``compression_preference`` is ``"zstd"`` and the ``zstd`` CLI is available,
        the directory is streamed using ``tar`` and compressed with ``zstd`` without
        creating an intermediate uncompressed tar file. Otherwise, a ``.tar.xz``
        archive is created using Python's ``tarfile`` module.

        Args:
            batch_dir (Path): Directory containing downloaded packages for the batch.
            compression_preference (str): Preferred compression method ("zstd" or "xz").

        Returns:
            dict: Metadata for the created archive with keys:
                - archive (str): Path to the archive file.
                - sha256 (str): SHA256 checksum of the archive.
                - size (int): Size of the archive in bytes.
                - status (str): Compression status ("ok").

        Raises:
            FileNotFoundError: If ``batch_dir`` does not exist or is not a directory.
            Exception: If compression fails (partial archive is removed).
        """
        if not batch_dir.exists() or not batch_dir.is_dir():
            raise FileNotFoundError(f'batch_dir not found or not a directory: {batch_dir}')

        final_archive = None
        try:
            zstd_available = shutil.which('zstd') is not None
            if compression_pref == 'zstd' and zstd_available:
                # stream tar -> zstd (no intermediate .tar file)
                final_archive = self.out_dir / f'{batch_dir.name}.tar.zst'
                # safe quoting of paths/names
                parent_dir_quoted = shlex.quote(str(batch_dir.parent))
                batch_name_quoted = shlex.quote(batch_dir.name)
                out_quoted = shlex.quote(str(final_archive))
                cmd = f'tar -C {parent_dir_quoted} -cf - {batch_name_quoted} | zstd -19 -T0 -o {out_quoted}'
                subprocess.run(['bash', '-lc', cmd], check=True)
            else:
                # fallback: write .tar.xz using tarfile (pure Python)
                final_archive = self.out_dir / f'{batch_dir.name}.tar.xz'
                with tarfile.open(final_archive, 'w:xz') as tarf:
                    tarf.add(batch_dir, arcname=batch_dir.name)

            # compute sha256 using your helper (assumes self._sha256_file exists)
            sha256 = self._sha256_file(final_archive)
            archive_size = final_archive.stat().st_size

            # cleanup original directory now that archive is good
            shutil.rmtree(batch_dir)

            return {
                'archive': str(final_archive),
                'sha256': sha256,
                'size': archive_size,
                'status': 'ok'
            }

        except Exception:
            # remove any partial archive
            try:
                if final_archive is not None and final_archive.exists():
                    final_archive.unlink()
            except Exception:
                pass
            raise # Raises Exception on main function (download_packages)

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
        
        batch_size = math.ceil(self.num_packs / self.num_batches)

        overall_results = []

        for batch_idx in range(self.num_batches):
            start = batch_idx * batch_size
            end = min(start + batch_size, self.num_packs)
            batch_pkgs = self.topN_list[start:end]
            if not batch_pkgs:
                continue

            batch_name = f'batch_{batch_idx+1:02d}'
            batch_out_dir = self.out_dir / batch_name
            batch_out_dir.mkdir(parents=True, exist_ok=True)
            
            print(f'Processing {batch_name} packages: {start}..{end-1}')

            # submit downloads for this batch
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                futures = {
                    ex.submit(
                        self._process_one_pack, pkg, batch_out_dir
                    ): pkg for pkg in batch_pkgs
                }
                
                for fut in tqdm(as_completed(futures), total=len(futures), desc='Downloading'):
                    pkg = futures[fut]
                    try:
                        results.append(fut.result())
                    except Exception as e:
                        results.append({
                            'package': pkg,
                            'version': None,
                            'downloaded': False,
                            'message': f'Unexpected Error:{e}'
                        })

            # Save per-batch report
            batch_report_path = batch_out_dir / f'{batch_name}_dwnld_report.json'
            self._save_to_json(results, batch_report_path)
            overall_results.extend(results)
        
            # Compress the whole batch and cleanup uncompressed files
            try:
                compress_res = self._compress_and_cleanup_batch(
                    batch_out_dir, compression_pref='zstd')
                # write batch manifest with archive info + download report
                batch_manifest = {
                    'batch': batch_name,
                    'start_index': start,
                    'end_index': end - 1,
                    'num_packages': len(batch_pkgs),
                    'archive_info': compress_res,
                    'download_report': str(batch_report_path)
                }
                manifest_path = self.out_dir / f'{batch_name}_manifest.json'

                self._save_to_json(batch_manifest, manifest_path)

                print(f'Compressed {batch_name} -> {compress_res['archive']}')
            
            except Exception as e:
                print(f'Failed to compress {batch_name}: {e}')
                continue
        
        # Save overall summary
        self._save_to_json(
            overall_results,
            self.out_dir / f'top{self.num_packs}_dwnld_report.json'
        )
        # Print download summary
        ok_count = sum(1 for r in overall_results if r.get('downloaded'))
        print(f'Successful: {ok_count}/{len(overall_results)}')
    
    def set_hash_algo(self, hash_algo: str):
        """
        Setter method to set hash algorithm to be used for package metadata.

        Args:
            hash_algo (str): Hash algorithm type as string
        """
        self.hash_algo = hash_algo
    
    def set_chunk_size(self, chunk_size: int):
        """
        Setter method to set chunk size in bytes for stream downloading.

        Args:
            chunk_size (int): Chunk size as int
        """
        self.chunk_size = chunk_size


class TopNPM(ParentDownloader):
    filter_pkgs = '@types/'
    
    def __init__(
            self,
            num_packs: int,
            out_dir: str | Path,
            libraries_io_key: str,
            list_url: str = TOP_NPM_URL,
            max_workers: int = MAX_WORKERS
        ):
        """
        Constructor for Top N npm package downloader.
        
        Args:
            num_packs (int): Number of top packages to be dowloaded
            out_dir (str | Path): Output directory path
            libraries_io_key: (str): API key for Libraries.io
            list_url (str): URL for top pypi packages list.
                Default = libraries.io/...
            max_workers (int): Maximum number of worker threads.
                Default = MAX_WORKERS (16)
        """
        super().__init__(num_packs, out_dir, list_url, max_workers)

        self.libraries_io_key = libraries_io_key

        self.topN_list = []

    def fetch_top_npm(self):
        """
        Fetch top npm packages using Libraries.io API (fresh data!).
        Sorted by download_count.
        Aggregates metadata and names.
        Saves count metrics.
        Filters out @types/ packages before aggregation to ensure we fetch enough.

        Raises:
            ValueError: Unexpected JSON layout
        """
        all_results = []  # Full project metadata (filtered)
        remaining = self.num_packs
        page = 1  # Libraries.io uses page-based pagination (1-based)
        per_page = min(25, remaining)  # Use max per_page for efficiency

        while remaining > 0:
            params = {
                'platforms': 'NPM',  # Only npm packages
                'sort': 'downloads_count',  # Or 'dependents_count'
                'order': 'desc',  # Highest first
                'page': page,
                'per_page': per_page,
                'api_key': self.libraries_io_key
            }
            resp = requests.get(self.list_url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()

            # libraries.io search returns list directly (array of projects)
            if not isinstance(data, list):
                raise ValueError('Unexpected JSON layout from libraries.io - expected array')

            if not data and remaining > 0:
                print("Warning: No more results returned (possible end of results)")
                break

            # Filter out @types/ packages here, before aggregation
            filtered_results = [
                project for project in data if not project['name'].startswith(self.filter_pkgs)
            ]

            # Aggregate only filtered metadata
            all_results.extend(filtered_results)

            # Extract only filtered names
            page_packages = [project['name'] for project in filtered_results]
            self.topN_list.extend(page_packages)

            # Update based on filtered fetched count
            fetched = len(filtered_results)
            remaining -= fetched

            # If we got fewer than per_page after filter, 
            # might be end - but continue if remaining >0
            page += 1

            if remaining > 0:
                time.sleep(1.0)  # Conservative delay — 60 req/min limit

        # Trim to exact request
        self.topN_list = self.topN_list[:self.num_packs]
        all_results = all_results[:self.num_packs]

        actual_fetched = len(self.topN_list)

        last_update_dt = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        full_data = {
            'last_update': last_update_dt,
            'requested_count': self.num_packs,
            'actual_fetched_count': actual_fetched,
            'sort_used': 'downloads_count desc',
            'results': all_results
        }
        
        # Save all downloaded data
        self._save_to_json(
            full_data,
            self.out_dir / f'top_npm_list{last_update_dt}.json'
        )

    def download_packages(self):
        pass