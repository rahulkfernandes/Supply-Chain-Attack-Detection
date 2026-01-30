import os
import json
import time
import random
import hashlib
import requests
from tqdm import tqdm
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed


TOP_URL = 'https://hugovk.github.io/top-pypi-packages/top-pypi-packages.json'
PYPI_URL = 'https://pypi.org/pypi/'
MAX_WORKERS = 16

class TopPyPi:
    timeout = 30
    max_retries = 3
    pypi_json_endpoint = 'json'
    uni_rnd_lim = (0, 0.2)
    hash_algo = 'sha256'
    chunk_size = 65536 # 64kb
    
    def __init__(
            self, 
            num_packs: int, 
            out_dir: str, 
            list_url: str = TOP_URL,
            pypi_url: str = PYPI_URL, 
            max_workers: int = MAX_WORKERS
        ):
        self.num_packs = num_packs
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
        with open(output_file, 'w') as json_file:
            json.dump(data_dict, json_file, indent=4)
            json_file.close()

    def get_top_pypi(self):
        resp = requests.get(self.list_url, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        # Extract last updated date
        last_update_dt = data['last_update']
        last_update_dt.replace(' ', '_')

        self._save_to_json(
            data,
            os.path.join(self.out_dir, f'top_pypi_{last_update_dt}.json')
        )

        # Data format: {'rows': [{...}, {...}], ....}
        # The rows are dicts keyed by column names; one common column is 'project'
        rows = data.get('rows') or []
        if not rows:
            raise SystemExit('Unexpected JSON layout from hugovk feed')
        
        self.topN_list = [row.get('project') for row in rows[:self.num_packs]]
    
    def _fetch_latest_vers(self, pkg):
        
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
            url: str, out_path: str, 
            expected_hash: str = None
        ):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_path.with_suffix(out_path.suffix + '.part')

        h = hashlib.new(self.hash_algo) if expected_hash else None

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

    def _download_pkg_sdist(self, package: str, version: str) -> Tuple:
        """
        Downloads sdist for package@version from PyPI via JSON metadata.
        Returns (True, path) on success or (False, error_str) on failure.
        """
        try:
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
                return (package, False, 'no-artifact-found')

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
                    return (package, True, str(out_path))
                except Exception as e:
                    if attempt == self.max_retries:
                        return (package, False, f'download-failed:{e}')
                    backoff = 2 ** attempt + random.random()
                    time.sleep(backoff)
            return (package, False, 'unreachable')
        except Exception as e:
            return package(False, f'meta-error:{e}')
    
    def _process_one_pack(self, pkg: str):
        # fetch metadata first
        version = self._fetch_latest_vers(pkg)
        # small randomized sleep to spread requests (optional)
        time.sleep(random.uniform(self.uni_rnd_lim[0], self.uni_rnd_lim[1]))
        return self._download_pkg_sdist(pkg, version)

    def download_packages(self):
        
        # Check if get_pypi was run
        if not self.topN_list:
            raise('Run `get_top_pypi` before running this method!')
        
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {
                ex.submit(self._process_one_pack, pkg): pkg for pkg in self.topN_list
            }
            for fut in tqdm(as_completed(futures), total=len(futures), desc='Downloading'):
                pkg = futures[fut]
                try:
                    pkg, ok, msg = fut.result()
                    results.append((pkg, ok, msg))
                except Exception as e:
                    results.append((pkg, False, f'Unexpected Error:{e}'))

        print(results)
