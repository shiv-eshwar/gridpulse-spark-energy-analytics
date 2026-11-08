"""Pinned UCI source, retryable atomic download, CRC and extracted-byte validation."""
from pathlib import Path
import hashlib
import json
import os
import tempfile
import time
import urllib.request
import zipfile

URL = 'https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip'
ARCHIVE_SHA256 = '9f84b46ade8a2d8e1286ec4b2b6c2987a45a755c59f263be3b3b3d10dfbda3ff'
TEXT_SHA256 = '4259c9d7ece5dbee9ab8d53682baac68d791c864f0f64a52b4043cb3b90894b7'
MEMBER = 'household_power_consumption.txt'


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def prepare(raw, url=URL, archive_hash=ARCHIVE_SHA256, text_hash=TEXT_SHA256,
            attempts=3, retry_delay=1):
    raw = Path(raw)
    raw.mkdir(parents=True, exist_ok=True)
    archive = raw / 'household.zip'
    if not archive.exists() or sha256(archive) != archive_hash:
        for attempt in range(attempts):
            fd, temporary = tempfile.mkstemp(prefix='uci-', suffix='.part', dir=raw)
            os.close(fd)
            temporary = Path(temporary)
            try:
                with urllib.request.urlopen(url, timeout=120) as response, temporary.open('wb') as dst:
                    while block := response.read(1024 * 1024):
                        dst.write(block)
                if sha256(temporary) != archive_hash:
                    raise ValueError('Downloaded archive differs from pinned UCI source')
                with zipfile.ZipFile(temporary) as z:
                    if z.testzip() is not None:
                        raise ValueError('ZIP CRC validation failed')
                os.replace(temporary, archive)
                break
            except Exception:
                if attempt + 1 == attempts:
                    raise
                time.sleep(retry_delay * (2 ** attempt))
            finally:
                temporary.unlink(missing_ok=True)
    with zipfile.ZipFile(archive) as z:
        if z.testzip() is not None:
            raise ValueError('Cached ZIP CRC validation failed')
        text = raw / MEMBER
        if not text.exists() or sha256(text) != text_hash:
            fd, temporary = tempfile.mkstemp(prefix='text-', suffix='.part', dir=raw)
            os.close(fd)
            temporary = Path(temporary)
            try:
                with z.open(MEMBER) as source, temporary.open('wb') as dst:
                    while block := source.read(1024 * 1024):
                        dst.write(block)
                if sha256(temporary) != text_hash:
                    raise ValueError('Extracted text differs from pinned UCI source')
                os.replace(temporary, text)
            finally:
                temporary.unlink(missing_ok=True)
    manifest = {'url': url, 'doi': '10.24432/C58K54', 'license': 'CC BY 4.0',
                'archive_sha256': sha256(archive), 'text_sha256': sha256(text),
                'pinned_hash_validation': 'PASS'}
    (raw / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == '__main__':
    print(json.dumps(prepare(Path(__file__).resolve().parents[1] / 'data/raw'), indent=2))
