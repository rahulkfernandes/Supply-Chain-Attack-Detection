import pytest
import hashlib
import json
import tempfile
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.data_collection.top_pkg_collection import TopPyPi

@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Prevent actual sleeping during tests."""
    monkeypatch.setattr('time.sleep', lambda *_: None)

# ----------- TopPyPi Tests ---------- #
def test_constructor_invalid_num_packs(tmp_path):
    with pytest.raises(ValueError):
        TopPyPi(num_packs=15, out_dir=tmp_path)  # 15 is not multiple of 10

def test_setters_and_attributes(tmp_path):
    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    td.set_timeout(5)
    td.set_max_retries(2)
    td.set_num_batches(5)
    td.set_max_workers(4)
    td.set_uni_rnd_lim((0.1, 0.2))
    td.set_hash_algo('md5')
    td.set_chunk_size(1024)

    assert td.timeout == 5
    assert td.max_retries == 2
    assert td.num_batches == 5
    assert td.max_workers == 4
    assert td.uni_rnd_lim == (0.1, 0.2)
    assert td.hash_algo == 'md5'
    assert td.chunk_size == 1024

def test_save_to_json_and_get_top_pypi(monkeypatch, tmp_path):
    # Prepare fake response for requests.get used by get_top_pypi
    fake_data = {
        'last_update': '2025-01-01 12:00:00',
        'rows': [{'project': f'pkg{i}'} for i in range(1, 11)]
    }

    class FakeResp:
        def raise_for_status(self): pass
        def json(self): return fake_data

    monkeypatch.setattr('requests.get', lambda *a, **k: FakeResp())

    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    td.fetch_top_pypi()

    # Check topN_list filled correctly
    assert td.topN_list == [f'pkg{i}' for i in range(1, 11)]

    # File should have been written
    expected_file = tmp_path / f'top_pypi_list{fake_data['last_update']}.json'
    assert expected_file.exists()
    # verify contents
    loaded = json.loads(expected_file.read_text())
    assert loaded['rows'][0]['project'] == 'pkg1'

def test_fetch_latest_vers_retries(monkeypatch):
    # First call raises, then returns a valid version
    calls = {'count': 0}

    class FakeResp:
        def raise_for_status(self): pass
        def json(self): return {'info': {'version': '9.9.9'}}

    def fake_requests_get(url, timeout):
        calls['count'] += 1
        if calls['count'] == 1:
            raise ValueError('temp failure')
        return FakeResp()

    monkeypatch.setattr('requests.get', fake_requests_get)
    td = TopPyPi(num_packs=10, out_dir=Path(tempfile.mkdtemp()))
    td.set_max_retries(3)
    v = td._fetch_latest_vers('somepkg')
    assert v == '9.9.9'

def test_sha256_file(tmp_path):
    f = tmp_path / 'file.bin'
    data = b'hello world'
    f.write_bytes(data)
    # compute expected
    expected = hashlib.sha256(data).hexdigest()

    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    got = td._sha256_file(f)
    assert got == expected

def make_streaming_response(content_bytes: bytes):
    """Return an object suitable for `with session.get(...) as r:` usage in the code."""
    class Resp:
        def __enter__(self_inner):
            return self_inner
        def __exit__(self_inner, exc_type, exc, tb):
            return False
        def raise_for_status(self_inner):
            return None
        def iter_content(self_inner, chunk_size=65536):
            # yield in chunks
            for i in range(0, len(content_bytes), chunk_size):
                yield content_bytes[i:i+chunk_size]
    return Resp()

def test_stream_download_success_and_checksum(tmp_path):
    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    # prepare fake session.get to return streaming response
    content = b'some data for package'
    resp_obj = make_streaming_response(content)

    # patch the instance session.get
    td.session.get = lambda *a, **k: resp_obj

    outfile = tmp_path / 'pkg-1.0.tar.gz'
    # expected sha256 of content
    expected = hashlib.sha256(content).hexdigest()

    out_path = td._stream_download('http://example.com/pkg', outfile, expected_hash=expected)
    assert out_path.exists()
    assert out_path.read_bytes() == content

    # Now checksum mismatch should raise ValueError and not leave .part file
    td.session.get = lambda *a, **k: make_streaming_response(b'other content')
    bad_out = tmp_path / 'pkg2-1.0.tar.gz'
    with pytest.raises(ValueError):
        td._stream_download('http://example.com/pkg2', bad_out, expected_hash=expected)
    # part file should be removed (if implementation attempted it)
    assert not (bad_out.with_suffix(bad_out.suffix + '.part')).exists()

def test__download_pkg_sdist_variants(monkeypatch, tmp_path):
    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    # 1) version == None
    res = td._download_pkg_sdist('pkg', None, tmp_path)
    assert res['downloaded'] is False
    assert 'no version' in res['message']

    # 2) no urls -> no-artifact-found
    class MetaResp:
        def raise_for_status(self): pass
        def json(self): return {'urls': []}

    td.session.get = lambda *a, **k: MetaResp()
    res = td._download_pkg_sdist('pkg', '1.0.0', tmp_path)
    assert res['downloaded'] is False
    assert res['message'] == 'no-artifact-found'

    # 3) meta error (session.get raises)
    td.session.get = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('boom'))
    res = td._download_pkg_sdist('pkg', '1.0.0', tmp_path)
    assert res['downloaded'] is False
    assert 'meta-error' in res['message']

    # 4) successful download path: stub _stream_download to write the file
    def fake_stream_write(url, out_path, expected_hash=None):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b'payload')
        return out_path

    monkeypatch.setattr(TopPyPi, '_stream_download', staticmethod(fake_stream_write))

    # prepare a metadata response with a sdist entry
    sdist = {
        'url': 'http://fake',
        'filename': 'pkg-1.0.tar.gz',
        'digests': {'sha256': hashlib.sha256(b'payload').hexdigest()},
        'packagetype': 'sdist'
    }
    class MetaRespOk:
        def raise_for_status(self): pass
        def json(self): return {'urls': [sdist]}

    td.session.get = lambda *a, **k: MetaRespOk()
    outdir = tmp_path / 'dwnlds'
    res = td._download_pkg_sdist('pkg', '1.0', outdir)
    assert res['downloaded'] is True
    assert str(outdir / sdist['filename']) in res['message']
    # ensure file exists
    assert (outdir / sdist['filename']).exists()

def test_compress_and_cleanup_batch_xz(tmp_path):
    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    # create a fake batch dir with some files
    batch_dir = tmp_path / "batch_01"
    batch_dir.mkdir()
    (batch_dir / "a.txt").write_text("hello")
    (batch_dir / "sub").mkdir()
    (batch_dir / "sub" / "b.txt").write_text("world")

    res = td._compress_and_cleanup_batch(batch_dir, compression_pref='xz')
    # archive returned should be .tar.xz
    assert res["archive"].endswith(".tar.xz")
    # archive file must exist
    archive_path = Path(res["archive"])
    assert archive_path.exists()
    # original batch dir removed
    assert not batch_dir.exists()
    # sha256 matches file contents
    assert res["sha256"] == td._sha256_file(archive_path)
    assert res["status"] == "ok"

def test_compress_and_cleanup_batch_zstd(monkeypatch, tmp_path):
    td = TopPyPi(num_packs=10, out_dir=tmp_path)
    # create batch dir
    batch_dir = tmp_path / "batch_02"
    batch_dir.mkdir()
    (batch_dir / "file.txt").write_text("abc")

    # force zstd_available True
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/zstd")

    # intercept subprocess.run and create the archive file the code expects
    def fake_run(args, check=True):
        # Inspect args to find out output file path (last token after -o)
        # We will just write an archive path with .tar.zst name based on batch_dir name
        final_archive = td.out_dir / f"{batch_dir.name}.tar.zst"
        # Create a small file so sha256 can be computed
        final_archive.write_bytes(b"fake-zstd-archive")
        return 0

    monkeypatch.setattr("subprocess.run", fake_run)

    res = td._compress_and_cleanup_batch(batch_dir, compression_pref='zstd')
    assert res["archive"].endswith(".tar.zst")
    assert Path(res["archive"]).exists()
    # cleanup done
    assert not batch_dir.exists()
    assert res["sha256"] == td._sha256_file(Path(res["archive"]))
    assert res["status"] == "ok"

MODULE = "src.data_collection.top_pkg_collection"


class TestTopPyPiDownloader(unittest.TestCase):
    """Tests for TopPyPi.download_packages method"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.out_dir = Path(self.test_dir) / "output"
        self.out_dir.mkdir(parents=True, exist_ok=True)
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
    
    def test_download_packages_raises_on_empty_list(self):
        """Test that download_packages raises RuntimeError when topN_list is empty"""
        
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = []  # Empty list
        
        with self.assertRaises(RuntimeError) as context:
            downloader.download_packages()
        
        self.assertIn("Run `get_top_pypi` before", str(context.exception))

    def test_download_packages_successful_flow(self):
        """
        Test successful download flow where all packages are downloaded
        and batches are compressed successfully.
        """
        
        # Setup downloader
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = [f"package_{i}" for i in range(10)]
        downloader.num_batches = 1
        
        # Mock tqdm to just return the iterable
        with patch(f'{MODULE}.tqdm', side_effect=lambda iterable, *args, **kwargs: iterable):
            # Mock ThreadPoolExecutor
            with patch(f'{MODULE}.ThreadPoolExecutor') as mock_executor_class:
                mock_executor_instance = MagicMock()
                mock_executor_class.return_value.__enter__.return_value = mock_executor_instance
                
                # Create futures with results
                futures_dict = {}
                for i in range(10):
                    future = MagicMock()
                    future.result.return_value = {
                        'package': f'package_{i}',
                        'version': '1.0.0',
                        'downloaded': True,
                        'message': f'{self.out_dir}/batch_01/package_{i}.tar.gz'
                    }
                    futures_dict[future] = f'package_{i}'
                
                # Mock submit to return the futures
                futures_keys = list(futures_dict.keys())
                
                def submit_side_effect(func, pkg, batch_dir):
                    # Find and return the future for this package
                    for future, pkg_name in futures_dict.items():
                        if pkg_name == pkg:
                            return future
                    return MagicMock()
                
                mock_executor_instance.submit.side_effect = submit_side_effect
                
                # Mock as_completed to return the futures in order
                with patch(f'{MODULE}.as_completed') as mock_as_completed:
                    # as_completed should return an iterator over the futures
                    mock_as_completed.return_value = iter(futures_dict.keys())
                    
                    # Mock compress
                    with patch.object(downloader, '_compress_and_cleanup_batch') as mock_compress:
                        mock_compress.return_value = {
                            'archive': f'{self.out_dir}/batch_01.tar.zst',
                            'sha256': 'hash',
                            'size': 1024,
                            'status': 'ok'
                        }
                        
                        # Mock save_to_json
                        with patch.object(downloader, '_save_to_json'):
                            with patch('builtins.print') as mock_print:
                                downloader.download_packages()
                                
                                # Check that success summary was printed
                                success_found = False
                                for call_args in mock_print.call_args_list:
                                    args = call_args[0]
                                    if args and len(args) > 0 and 'Successful:' in args[0]:
                                        success_found = True
                                        # It should say 10/10
                                        self.assertIn('10/10', args[0])
                                        break
                                
                                self.assertTrue(success_found, "Success summary not printed")
    
    def test_download_packages_failure_flow(self):
        """
        Test failure flow where no packages are downloaded successfully.
        """
        
        # Setup downloader
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = [f"package_{i}" for i in range(10)]
        downloader.num_batches = 1
        
        # Mock tqdm
        with patch(f'{MODULE}.tqdm', side_effect=lambda iterable, *args, **kwargs: iterable):
            # Mock ThreadPoolExecutor
            with patch(f'{MODULE}.ThreadPoolExecutor') as mock_executor_class:
                mock_executor_instance = MagicMock()
                mock_executor_class.return_value.__enter__.return_value = mock_executor_instance
                
                # Create futures with failure results
                futures_dict = {}
                for i in range(10):
                    future = MagicMock()
                    future.result.return_value = {
                        'package': f'package_{i}',
                        'version': None,
                        'downloaded': False,
                        'message': 'download-failed:Network error'
                    }
                    futures_dict[future] = f'package_{i}'
                
                # Mock submit
                def submit_side_effect(func, pkg, batch_dir):
                    # Find and return the future for this package
                    for future, pkg_name in futures_dict.items():
                        if pkg_name == pkg:
                            return future
                    return MagicMock()
                
                mock_executor_instance.submit.side_effect = submit_side_effect
                
                # Mock as_completed
                with patch(f'{MODULE}.as_completed') as mock_as_completed:
                    mock_as_completed.return_value = iter(futures_dict.keys())
                    
                    # Mock compress
                    with patch.object(downloader, '_compress_and_cleanup_batch') as mock_compress:
                        mock_compress.return_value = {
                            'archive': f'{self.out_dir}/batch_01.tar.zst',
                            'sha256': 'empty',
                            'size': 0,
                            'status': 'ok'
                        }
                        
                        # Mock save_to_json
                        with patch.object(downloader, '_save_to_json'):
                            with patch('builtins.print') as mock_print:
                                downloader.download_packages()
                                
                                # Check that failure summary was printed
                                failure_found = False
                                for call_args in mock_print.call_args_list:
                                    args = call_args[0]
                                    if args and len(args) > 0 and 'Successful:' in args[0]:
                                        failure_found = True
                                        # It should say 0/10
                                        self.assertIn('0/10', args[0])
                                        break
                                
                                self.assertTrue(failure_found, "Failure summary not printed")
    
    def test_download_packages_compression_success(self):
        """
        Test that successful compression works properly.
        """
        
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = [f"package_{i}" for i in range(10)]
        downloader.num_batches = 1
        
        # Create a batch directory
        batch_dir = self.out_dir / "batch_01"
        batch_dir.mkdir()
        
        with patch(f'{MODULE}.tqdm', side_effect=lambda iterable, *args, **kwargs: iterable):
            with patch(f'{MODULE}.ThreadPoolExecutor') as mock_executor_class:
                mock_executor_instance = MagicMock()
                mock_executor_class.return_value.__enter__.return_value = mock_executor_instance
                
                # Mock futures
                futures_dict = {}
                for i in range(10):
                    future = MagicMock()
                    future.result.return_value = {
                        'package': f'package_{i}',
                        'version': '1.0.0',
                        'downloaded': True,
                        'message': f'{batch_dir}/package_{i}.tar.gz'
                    }
                    futures_dict[future] = f'package_{i}'
                
                def submit_side_effect(func, pkg, batch_dir):
                    for future, pkg_name in futures_dict.items():
                        if pkg_name == pkg:
                            return future
                    return MagicMock()
                
                mock_executor_instance.submit.side_effect = submit_side_effect
                
                with patch(f'{MODULE}.as_completed') as mock_as_completed:
                    mock_as_completed.return_value = iter(futures_dict.keys())
                    
                    # Mock _save_to_json to capture what's saved
                    saved_data = []
                    
                    def save_side_effect(data, filename):
                        saved_data.append((data, str(filename)))
                    
                    with patch.object(downloader, '_save_to_json', side_effect=save_side_effect):
                        with patch.object(downloader, '_compress_and_cleanup_batch') as mock_compress:
                            # Mock successful compression
                            mock_compress.return_value = {
                                'archive': f'{self.out_dir}/batch_01.tar.zst',
                                'sha256': 'test_hash_123',
                                'size': 5000,
                                'status': 'ok'
                            }
                            
                            downloader.download_packages()
                            
                            # Verify compression was called with correct batch directory
                            mock_compress.assert_called_once()
                            args, _ = mock_compress.call_args
                            self.assertEqual(args[0], batch_dir)
                            
                            # Verify manifest contains compression info
                            manifest_data = None
                            for data, filename in saved_data:
                                if 'manifest' in filename:
                                    manifest_data = data
                                    break
                            
                            self.assertIsNotNone(manifest_data, "Manifest not saved")
                            self.assertEqual(manifest_data['batch'], 'batch_01')
                            self.assertEqual(manifest_data['archive_info']['sha256'], 'test_hash_123')
                            self.assertEqual(manifest_data['archive_info']['status'], 'ok')
    
    def test_download_packages_compression_failure(self):
        """
        Test that compression failure is handled gracefully.
        """
        
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = [f"package_{i}" for i in range(10)]
        downloader.num_batches = 1
        
        # Create a batch directory
        batch_dir = self.out_dir / "batch_01"
        batch_dir.mkdir()
        
        with patch(f'{MODULE}.tqdm', side_effect=lambda iterable, *args, **kwargs: iterable):
            with patch(f'{MODULE}.ThreadPoolExecutor') as mock_executor_class:
                mock_executor_instance = MagicMock()
                mock_executor_class.return_value.__enter__.return_value = mock_executor_instance
                
                # Mock futures
                futures_dict = {}
                for i in range(10):
                    future = MagicMock()
                    future.result.return_value = {
                        'package': f'package_{i}',
                        'version': '1.0.0',
                        'downloaded': True,
                        'message': f'{batch_dir}/package_{i}.tar.gz'
                    }
                    futures_dict[future] = f'package_{i}'
                
                def submit_side_effect(func, pkg, batch_dir):
                    for future, pkg_name in futures_dict.items():
                        if pkg_name == pkg:
                            return future
                    return MagicMock()
                
                mock_executor_instance.submit.side_effect = submit_side_effect
                
                with patch(f'{MODULE}.as_completed') as mock_as_completed:
                    mock_as_completed.return_value = iter(futures_dict.keys())
                    
                    with patch.object(downloader, '_save_to_json'):
                        with patch.object(downloader, '_compress_and_cleanup_batch') as mock_compress:
                            # Mock compression failure
                            mock_compress.side_effect = Exception("Compression failed: Disk full")
                            
                            with patch('builtins.print') as mock_print:
                                downloader.download_packages()
                                
                                # Verify error message was printed
                                error_printed = False
                                for call_args in mock_print.call_args_list:
                                    args = call_args[0]
                                    if args and len(args) > 0 and 'Failed to compress' in args[0]:
                                        error_printed = True
                                        self.assertIn('Failed to compress', args[0])
                                        self.assertIn('batch_01', args[0])
                                        break
                                
                                self.assertTrue(error_printed, "Compression failure not reported")
    def test_download_packages_mixed_results(self):
        """
        Test flow with mixed results (some success, some failure).
        """
        
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = [f"package_{i}" for i in range(10)]
        downloader.num_batches = 1
        
        with patch(f'{MODULE}.tqdm', side_effect=lambda iterable, *args, **kwargs: iterable):
            with patch(f'{MODULE}.ThreadPoolExecutor') as mock_executor_class:
                mock_executor_instance = MagicMock()
                mock_executor_class.return_value.__enter__.return_value = mock_executor_instance
                
                # Create mixed results: first 5 succeed, last 5 fail
                futures_dict = {}
                for i in range(10):
                    future = MagicMock()
                    if i < 5:
                        future.result.return_value = {
                            'package': f'package_{i}',
                            'version': '1.0.0',
                            'downloaded': True,
                            'message': f'{self.out_dir}/batch_01/package_{i}.tar.gz'
                        }
                    else:
                        future.result.return_value = {
                            'package': f'package_{i}',
                            'version': None,
                            'downloaded': False,
                            'message': 'download-failed:Network error'
                        }
                    futures_dict[future] = f'package_{i}'
                
                def submit_side_effect(func, pkg, batch_dir):
                    for future, pkg_name in futures_dict.items():
                        if pkg_name == pkg:
                            return future
                    return MagicMock()
                
                mock_executor_instance.submit.side_effect = submit_side_effect
                
                with patch(f'{MODULE}.as_completed') as mock_as_completed:
                    mock_as_completed.return_value = iter(futures_dict.keys())
                    
                    with patch.object(downloader, '_compress_and_cleanup_batch') as mock_compress:
                        mock_compress.return_value = {
                            'archive': f'{self.out_dir}/batch_01.tar.zst',
                            'sha256': 'mixed_hash',
                            'size': 2500,
                            'status': 'ok'
                        }
                        
                        with patch.object(downloader, '_save_to_json') as mock_save:
                            saved_data = []
                            
                            def save_side_effect(data, filename):
                                saved_data.append((data, str(filename)))
                            
                            mock_save.side_effect = save_side_effect
                            
                            with patch('builtins.print') as mock_print:
                                downloader.download_packages()
                                
                                # Verify mixed results summary
                                summary_found = False
                                for call_args in mock_print.call_args_list:
                                    args = call_args[0]
                                    if args and len(args) > 0 and 'Successful:' in args[0]:
                                        summary_found = True
                                        # Should say 5/10
                                        self.assertIn('5/10', args[0])
                                        break
                                
                                self.assertTrue(summary_found, "Mixed results summary not printed")
                                
                                # Verify batch report contains correct counts
                                batch_report = None
                                for data, filename in saved_data:
                                    if 'dwnld_report' in filename:
                                        batch_report = data
                                        break
                                
                                self.assertIsNotNone(batch_report, "Batch report not saved")
                                
                                # Count successes and failures
                                successes = sum(1 for r in batch_report if r['downloaded'])
                                failures = sum(1 for r in batch_report if not r['downloaded'])
                                
                                self.assertEqual(successes, 5)
                                self.assertEqual(failures, 5)
    
    def test_download_packages_exception_in_future(self):
        """
        Test that exceptions raised in futures are caught and handled.
        """
        
        downloader = TopPyPi(num_packs=10, out_dir=self.out_dir)
        downloader.topN_list = [f"package_{i}" for i in range(10)]
        downloader.num_batches = 1
        
        with patch(f'{MODULE}.tqdm', side_effect=lambda iterable, *args, **kwargs: iterable):
            with patch(f'{MODULE}.ThreadPoolExecutor') as mock_executor_class:
                mock_executor_instance = MagicMock()
                mock_executor_class.return_value.__enter__.return_value = mock_executor_instance
                
                # Create futures where one raises an exception
                futures_dict = {}
                for i in range(10):
                    future = MagicMock()
                    if i == 5:  # One specific future raises an exception
                        future.result.side_effect = Exception("Test exception in future")
                    else:
                        future.result.return_value = {
                            'package': f'package_{i}',
                            'version': '1.0.0',
                            'downloaded': True,
                            'message': f'{self.out_dir}/batch_01/package_{i}.tar.gz'
                        }
                    futures_dict[future] = f'package_{i}'
                
                def submit_side_effect(func, pkg, batch_dir):
                    for future, pkg_name in futures_dict.items():
                        if pkg_name == pkg:
                            return future
                    return MagicMock()
                
                mock_executor_instance.submit.side_effect = submit_side_effect
                
                with patch(f'{MODULE}.as_completed') as mock_as_completed:
                    mock_as_completed.return_value = iter(futures_dict.keys())
                    
                    with patch.object(downloader, '_compress_and_cleanup_batch'):
                        with patch.object(downloader, '_save_to_json') as mock_save:
                            saved_data = []
                            
                            def save_side_effect(data, filename):
                                saved_data.append((data, str(filename)))
                            
                            mock_save.side_effect = save_side_effect
                            
                            downloader.download_packages()
                            
                            # Find the batch report
                            batch_report = None
                            for data, filename in saved_data:
                                if 'dwnld_report' in filename:
                                    batch_report = data
                                    break
                            
                            self.assertIsNotNone(batch_report, "Batch report not saved")
                            
                            # Check that package_5 has error message
                            for result in batch_report:
                                if result['package'] == 'package_5':
                                    self.assertFalse(result['downloaded'])
                                    self.assertIn('Unexpected Error', result['message'])
                                    break
                            else:
                                self.fail("Package_5 not found in batch report")


# @patch('concurrent.futures.ThreadPoolExecutor')
# def test_download_packages_raises_on_empty_list(mock_executor, tmp_path):
#     top_pypi_instance = TopPyPi(num_packs=20, out_dir=tmp_path)
#     top_pypi_instance.max_workers = 2  # Small for testing
    
#     top_pypi_instance.num_batches = 1
#     top_pypi_instance.topN_list = []  # Empty list
#     with pytest.raises(RuntimeError) as exc_info:
#         top_pypi_instance.download_packages()
#     assert str(exc_info.value) == 'Run `get_top_pypi` before running this method!'
#     mock_executor.assert_not_called()  # No threads if no packages