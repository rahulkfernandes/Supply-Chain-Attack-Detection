import os
import json
import requests


URL = 'https://hugovk.github.io/top-pypi-packages/top-pypi-packages.json'

class TopPyPi:
    def __init__(
            self, num_packs: int, raw_dir: str, list_url: str = URL, timeout:int = 30
            ):
        self.num_packs = num_packs
        self.raw_dir = raw_dir
        self.list_url = list_url
        self.timeout = timeout
        self.top_list = []

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
        
        self.top_list = [row.get('project') for row in rows[:self.num_packs]]
    
    