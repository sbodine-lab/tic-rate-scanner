"""Stream a payer file over HTTPS without storing it.

The compressed bytes are hashed and counted as they are read, so every rate
carries the SHA-256 and byte length of the exact file it came from. gzip is
decompressed on the fly. ZIP needs random access, so only its compressed bytes
are spooled to scratch disk. Interrupted transfers resume with a byte range
only when the server proves it is the same object (ETag + 206 Content-Range).
"""
from __future__ import annotations

import codecs
import contextlib
import gzip
import hashlib
import io
import shutil
import ssl
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import config

USER_AGENT = config.subject()['user_agent']
CHUNK = 1 << 20
# Public intermediates for payer hosts that omit them from the TLS handshake
# (some serve only their leaf certificate); the configuration names the folder.
CA_CERTS_DIR = config.path('ca_certs')


def _ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    for pem in sorted(CA_CERTS_DIR.glob('*.pem')) if CA_CERTS_DIR else ():
        context.load_verify_locations(cafile=str(pem))
    return context


SSL_CONTEXT = _ssl_context()


class FetchError(RuntimeError):
    pass


@dataclass
class SourceMeta:
    url: str
    final_url: str | None = None
    status: int | None = None
    content_length: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    content_type: str | None = None
    compression: str | None = None
    bytes_read: int = 0
    sha256: str | None = None
    resumes: int = 0
    retrieved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec='seconds'))
    zip_member: str | None = None
    text_repairs: int = 0
    # Set by the caller when it stopped early on purpose; the hash then covers only bytes read.
    stopped_early: bool = False


def _request(url: str, headers: dict | None = None, method: str = 'GET'):
    request = urllib.request.Request(url, method=method, headers={'User-Agent': USER_AGENT, 'Accept-Encoding': 'identity', **(headers or {})})
    return urllib.request.urlopen(request, timeout=120, context=SSL_CONTEXT)


def head(url: str) -> dict:
    """Best-effort metadata; some hosts refuse HEAD, so fall back to a 1-byte range."""
    for method, headers in (('HEAD', {}), ('GET', {'Range': 'bytes=0-0'})):
        try:
            with _request(url, headers, method) as response:
                length = response.headers.get('Content-Length')
                content_range = response.headers.get('Content-Range') or ''
                if content_range and '/' in content_range:
                    length = content_range.rsplit('/', 1)[1]
                return {'status': response.status, 'final_url': response.geturl(),
                        'content_length': int(length) if length and length.isdigit() else None,
                        'etag': response.headers.get('ETag'), 'last_modified': response.headers.get('Last-Modified'),
                        'accept_ranges': response.headers.get('Accept-Ranges') or ('bytes' if response.status == 206 else None)}
        except urllib.error.HTTPError as error:
            if method == 'GET':
                return {'status': error.code, 'error': str(error)}
        except (urllib.error.URLError, TimeoutError) as error:
            if method == 'GET':
                return {'status': None, 'error': str(error)}
    return {'status': None}


class TextRepairReader(io.RawIOBase):
    """Valid UTF-8 passes through; only bytes that are invalid UTF-8 are read as
    Windows-1252 (publishers paste en dashes into descriptions). Every repair is
    counted on meta.text_repairs; numbers and identifiers are ASCII and unaffected."""

    def __init__(self, stream, meta: 'SourceMeta'):
        self.stream, self.meta = stream, meta
        self.decoder = codecs.getincrementaldecoder('utf-8')(errors='mrf_cp1252')
        self.pending = b''

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        while not self.pending:
            chunk = self.stream.read(CHUNK)
            text = self.decoder.decode(chunk, final=not chunk)
            self.pending = text.encode('utf-8')
            if not chunk:
                break
        size = min(len(buffer), len(self.pending))
        buffer[:size], self.pending = self.pending[:size], self.pending[size:]
        return size


def _cp1252_fallback(error: UnicodeDecodeError):
    bad = error.object[error.start:error.end]
    _REPAIRS.count += len(bad)
    return bad.decode('cp1252', errors='replace'), error.end


class _RepairCounter:
    count = 0


_REPAIRS = _RepairCounter()
codecs.register_error('mrf_cp1252', _cp1252_fallback)


class _HashingReader(io.RawIOBase):
    def __init__(self, url: str, meta: SourceMeta, max_resumes: int = 5):
        self.url, self.meta, self.max_resumes = url, meta, max_resumes
        self.hash = hashlib.sha256()
        self.response = None
        self.accept_ranges = False
        self._open(0)

    def _open(self, offset: int) -> None:
        headers = {}
        if offset:
            headers = {'Range': f'bytes={offset}-'}
            if self.meta.etag:
                headers['If-Range'] = self.meta.etag
        try:
            response = _request(self.url, headers)
        except urllib.error.HTTPError as error:
            raise FetchError(f'HTTP {error.code} for {self.url}') from error
        except (urllib.error.URLError, TimeoutError) as error:
            raise FetchError(f'Cannot reach {self.url}: {error}') from error
        if offset:
            content_range = response.headers.get('Content-Range') or ''
            etag = response.headers.get('ETag')
            if response.status != 206 or not content_range.startswith(f'bytes {offset}-') or (self.meta.etag and etag and etag != self.meta.etag):
                response.close()
                raise FetchError('Server did not resume the same object; restart the file from the beginning')
        else:
            length = response.headers.get('Content-Length')
            self.meta.status = response.status
            self.meta.final_url = response.geturl()
            self.meta.content_length = int(length) if length and length.isdigit() else None
            self.meta.etag = response.headers.get('ETag')
            self.meta.last_modified = response.headers.get('Last-Modified')
            self.meta.content_type = response.headers.get('Content-Type')
            self.accept_ranges = (response.headers.get('Accept-Ranges') or '').lower() == 'bytes'
        self.response = response

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        while True:
            try:
                data = self.response.read(len(buffer))
                break
            except (OSError, TimeoutError, urllib.error.URLError) as error:
                resumable = self.accept_ranges and self.meta.etag and self.meta.content_length
                if not resumable or self.meta.resumes >= self.max_resumes:
                    raise FetchError(f'Transfer interrupted at byte {self.meta.bytes_read}: {error}') from error
                self.meta.resumes += 1
                time.sleep(min(30, 2 ** self.meta.resumes))
                with contextlib.suppress(Exception):
                    self.response.close()
                self._open(self.meta.bytes_read)
        size = len(data)
        buffer[:size] = data
        self.hash.update(data)
        self.meta.bytes_read += size
        return size

    def drain(self) -> None:
        """Read any bytes the parser did not need so the hash covers the whole file."""
        while self.read(CHUNK):
            pass
        self.meta.sha256 = self.hash.hexdigest()
        if self.meta.content_length is not None and self.meta.bytes_read != self.meta.content_length:
            raise FetchError(f'Read {self.meta.bytes_read} bytes but the server declared {self.meta.content_length}')

    def finish(self, stopped_early: bool) -> None:
        if stopped_early:
            self.meta.sha256 = None  # not a whole-file hash; bytes_read records how far we read
        else:
            self.drain()

    def close(self) -> None:
        with contextlib.suppress(Exception):
            if self.response is not None:
                self.response.close()
        super().close()


@contextlib.contextmanager
def open_json_stream(url: str, *, scratch_dir: str | None = None, repair_text: bool = False):
    """Yield (decompressed JSON byte stream, SourceMeta). Meta is final after the block exits cleanly."""
    with _open_json_stream(url, scratch_dir) as (stream, meta):
        if not repair_text:
            yield stream, meta
            return
        before = _REPAIRS.count
        yield io.BufferedReader(TextRepairReader(stream, meta), CHUNK), meta
        meta.text_repairs = _REPAIRS.count - before


@contextlib.contextmanager
def _open_json_stream(url: str, scratch_dir: str | None):
    meta = SourceMeta(url=url)
    raw = _HashingReader(url, meta)
    buffered = io.BufferedReader(raw, CHUNK)
    try:
        magic = buffered.peek(4)[:4]
        if magic[:2] == b'\x1f\x8b':
            meta.compression = 'gzip'
            stream = gzip.GzipFile(fileobj=buffered, mode='rb')
            yield stream, meta
            raw.finish(meta.stopped_early)
        elif magic[:2] == b'PK':
            meta.compression = 'zip'
            with tempfile.TemporaryFile(dir=scratch_dir) as spool:
                shutil.copyfileobj(buffered, spool, CHUNK)
                raw.drain()
                spool.seek(0)
                with zipfile.ZipFile(spool) as archive:
                    members = [m for m in archive.infolist() if not m.is_dir() and m.filename.lower().endswith('.json')]
                    if len(members) != 1:
                        raise FetchError(f'ZIP holds {len(members)} JSON members; expected exactly one')
                    meta.zip_member = members[0].filename
                    with archive.open(members[0]) as stream:
                        yield stream, meta
        else:
            meta.compression = 'none'
            yield buffered, meta
            raw.finish(meta.stopped_early)
    finally:
        buffered.close()
