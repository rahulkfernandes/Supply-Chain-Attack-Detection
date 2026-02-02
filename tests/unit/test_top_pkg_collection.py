import pytest
import hashlib
import json
import tempfile
import shutil
from pathlib import Path
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
    td.get_top_pypi()

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