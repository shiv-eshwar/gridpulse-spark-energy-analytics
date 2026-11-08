import hashlib
import io
import zipfile
from pathlib import Path
import pytest
from scripts.download import prepare, MEMBER


def fixture_archive():
    payload=b'fixture data\n'
    buffer=io.BytesIO()
    with zipfile.ZipFile(buffer,'w') as z:z.writestr(MEMBER,payload)
    return payload,buffer.getvalue()


def test_partial_cache_and_changed_text_are_repaired_atomically(tmp_path,monkeypatch):
    payload,archive=fixture_archive();calls=[]
    def fetch(*args,**kwargs):
        calls.append(1)
        if len(calls)==1:raise OSError('temporary connection failure')
        return io.BytesIO(archive)
    monkeypatch.setattr('urllib.request.urlopen',fetch)
    (tmp_path/'household.zip').write_bytes(b'interrupted download')
    args=dict(archive_hash=hashlib.sha256(archive).hexdigest(),text_hash=hashlib.sha256(payload).hexdigest(),retry_delay=0)
    result=prepare(tmp_path,**args)
    assert result['pinned_hash_validation']=='PASS' and len(calls)==2
    (tmp_path/MEMBER).write_text('changed locally')
    prepare(tmp_path,**args)
    assert (tmp_path/MEMBER).read_bytes()==payload and len(calls)==2
    assert not list(tmp_path.glob('*.part'))


def test_failed_download_never_publishes_partial_archive(tmp_path,monkeypatch):
    monkeypatch.setattr('urllib.request.urlopen',lambda *a,**k:io.BytesIO(b'bad source bytes'))
    with pytest.raises(ValueError,match='pinned'):prepare(tmp_path,retry_delay=0)
    assert not (tmp_path/'household.zip').exists() and not list(tmp_path.glob('*.part'))
