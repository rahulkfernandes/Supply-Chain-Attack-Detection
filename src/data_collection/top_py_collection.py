import os
import json
import tqdm
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


TOP_URL = 'https://hugovk.github.io/top-pypi-packages/top-pypi-packages.json'
PYPI_URL = 'https://pypi.org/pypi/'
MAX_WORKERS = 16

class TopPyPi:
    timeout = 30
    max_retries = 3
    pypi_json_endpoint = 'json'
    
    def __init__(
            self, 
            num_packs: int, 
            raw_dir: str, 
            list_url: str = TOP_URL,
            pypi_url: str = PYPI_URL, 
            max_workers: int = MAX_WORKERS
        ):
        self.num_packs = num_packs
        self.raw_dir = raw_dir
        self.list_url = list_url
        self.pypi_url = pypi_url
        self.max_workers = max_workers
        
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

        # Uncommect after completing function!!!!
        # self._save_to_json(
        #     data,
        #     os.path.join(self.raw_dir, f'top_pypi_{last_update_dt}.json')
        # )

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
                print(f"[meta] {pkg} attempt {attempt} failed: {e}. retrying in {wait:.1f}s")
                time.sleep(wait)
        return None
    
    def _download_pkg_sdist(self, pkg, version):
        # TODO: download pankage here
        pass
    
    def _process_one_pack(self, pkg: str):
        # fetch metadata first
        version = self._fetch_latest_vers(pkg)
        # small randomized sleep to spread requests (optional)
        time.sleep(random.uniform(0, 0.2))
        
        # TODO: Download package here and return to child thread
        # res = download_pkg_sdist(pkg, version)
        # return res

    def pull_top_packages(self):
        
        # Check if get_pypi was run
        if not self.topN_list:
            raise('Run `get_top_pypi` before running this method!')
        
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self._process_one_pack, pkg): pkg for pkg in self.topN_list}
            for fut in tqdm(as_completed(futures), total=len(futures), desc='Downloading'):
                pkg = futures[fut]
                try:
                    pkg, ok, msg = fut.result()
                    results.append((pkg, ok, msg))
                except Exception as e:
                    results.append((pkg, False, f"exception:{e}"))
