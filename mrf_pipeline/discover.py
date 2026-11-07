"""Find the newest in-network files for each configured payer.

Every adapter starts from what the payer publishes as its index (a CMS table
of contents, HealthSparq's metadata file, Cigna's latest.json, or the UHC and
Optum blob catalogs) and returns `FileTarget`s carrying the plans the index
maps to each file. Selection rules come from payers.toml; a payer whose
index cannot be read raises, so the run fails loudly instead of going quiet.
"""
from __future__ import annotations

import io
import json
import re
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta

import ijson

from .fetch import FetchError, _request, head

DATE_IN_NAME = re.compile(r'(20\d\d-\d\d-\d\d)')
HASH_SUFFIX = re.compile(r'[-_][0-9a-f]{40,64}(?=[_.-])')
# A leading edition month ("2026-11_140_13B0_...") also changes every month.
MONTH_PREFIX = re.compile(r'^20\d\d-\d\d(?=[_-])')
# Plans kept per file; the full distinct count is still recorded.
MAX_PLANS = 2000


class DiscoveryError(RuntimeError):
    pass


@dataclass
class FileTarget:
    payer_id: str
    url: str
    name: str
    series: str
    published: str | None
    index_url: str | None
    size: int | None = None
    description: str | None = None
    plans: list[dict] = field(default_factory=list)
    signed: bool = False
    plan_keys: set = field(default_factory=set, repr=False)

    def add_plan(self, plan: dict, limit: int = MAX_PLANS) -> None:
        """Count every distinct plan; keep the first `limit` (plan_count_total says how many were listed)."""
        key = json.dumps(plan, sort_keys=True)
        if key not in self.plan_keys:
            self.plan_keys.add(key)
            if len(self.plans) < limit:
                self.plans.append(plan)

    @property
    def plan_count_total(self) -> int:
        return len(self.plan_keys)

    def provenance_url(self) -> str:
        """Recorded URL. Signed query strings (CloudFront signatures, Azure SAS) are dropped."""
        if not self.signed:
            return self.url
        parts = urllib.parse.urlsplit(self.url)
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))


def file_name(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    named = urllib.parse.parse_qs(parts.query).get('filename')
    return urllib.parse.unquote(named[0] if named else parts.path.rsplit('/', 1)[-1])


def series_key(payer_id: str, name: str) -> str:
    base = urllib.parse.unquote(name.rsplit('/', 1)[-1].split('?', 1)[0])
    base = DATE_IN_NAME.sub('', base, count=1).strip('_-')
    base = MONTH_PREFIX.sub('', base).strip('_-')
    return f'{payer_id}:{HASH_SUFFIX.sub("", base)}'


def _get_bytes(url: str, limit: int = 200_000_000) -> bytes:
    try:
        with _request(url) as response:
            data = response.read(limit + 1)
    except Exception as error:
        raise DiscoveryError(f'Cannot read index {url}: {error}') from error
    if len(data) > limit:
        raise DiscoveryError(f'Index {url} exceeds {limit} bytes; stream it instead')
    return data


def _json_bytes(data: bytes):
    if data[:2] == b'\x1f\x8b':
        import gzip
        data = gzip.decompress(data)
    return json.loads(data)


def _matches(text: str, include: list[str], exclude: list[str]) -> bool:
    return any(re.search(p, text, re.I) for p in include or ['.']) and not any(re.search(p, text, re.I) for p in exclude or [])


def _plan(entry: dict) -> dict:
    keys = ('plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'issuer_name', 'plan_sponsor_name')
    camel = {'planName': 'plan_name', 'planIdType': 'plan_id_type', 'planId': 'plan_id', 'planMarketType': 'plan_market_type',
             'issuerName': 'issuer_name', 'planSponsorName': 'plan_sponsor_name'}
    out = {k: entry.get(k) for k in keys if entry.get(k) is not None}
    out.update({camel[k]: v for k, v in entry.items() if k in camel and v is not None})
    return {k: str(v) for k, v in out.items()}


def toc_targets(payer_id: str, toc_url: str, stream, include, exclude, *, url_fix=None, max_plans: int = MAX_PLANS) -> list[FileTarget]:
    """Stream a CMS table of contents; keep matching in-network files and their plans."""
    files: dict[str, FileTarget] = {}
    for structure in ijson.items(stream, 'reporting_structure.item'):
        plans = [_plan(p) for p in structure.get('reporting_plans') or []]
        for entry in structure.get('in_network_files') or []:
            location = (url_fix or (lambda u: u))(str(entry.get('location') or ''))
            text = f"{location} {entry.get('description') or ''}"
            if not location or not _matches(text, include, exclude):
                continue
            name = file_name(location)
            target = files.get(location) or FileTarget(payer_id, location, name, series_key(payer_id, name),
                                                       (DATE_IN_NAME.search(name) or [None])[0] if DATE_IN_NAME.search(name) else None,
                                                       toc_url, description=entry.get('description'))
            for plan in plans:
                target.add_plan(plan, max_plans)
            files[location] = target
    return list(files.values())


def discover(payer: dict, *, today: date) -> list[FileTarget]:
    method = payer['discovery']
    handler = {'healthsparq': _healthsparq, 'toc': _toc, 'cigna': _cigna, 'blob_catalog': _blob_catalog,
               'direct': _direct, 'listing': _listing}.get(method)
    if handler is None:
        raise DiscoveryError(f"{payer['id']}: unknown discovery method {method!r}")
    targets = handler(payer, today)
    for target in targets:
        target.signed = bool(payer.get('signed_urls'))
    if not targets:
        raise DiscoveryError(f"{payer['id']}: index read, but no in-network file matched the selection rules")
    limit = payer.get('max_files')
    if limit and len(targets) > limit:
        raise DiscoveryError(f"{payer['id']}: {len(targets)} files matched, above the reviewed limit of {limit}")
    return sorted(targets, key=lambda t: t.url)


def _healthsparq(payer: dict, today: date) -> list[FileTarget]:
    base = payer['base_url'].rstrip('/') + '/'
    index = base + 'latest_metadata.json'
    meta = _json_bytes(_get_bytes(index))
    targets: dict[str, FileTarget] = {}
    for entry in meta.get('files', []):
        if entry.get('fileSchema') != 'IN_NETWORK_RATES':
            continue
        name = entry.get('fileName') or ''
        if not _matches(name, payer.get('include'), payer.get('exclude')):
            continue
        # The metadata repeats a file once per reporting plan; merge them.
        target = targets.setdefault(entry['filePath'], FileTarget(payer['id'], base + entry['filePath'], name,
                                    series_key(payer['id'], name), entry.get('lastUpdatedOn'), index))
        for plan in (_plan(p) for p in entry.get('reportingPlans') or []):
            target.add_plan(plan)
    return list(targets.values())


def _probe_dated(template: str, today: date, days: int, months: int = 0) -> tuple[str, date]:
    """Newest dated URL that answers: by month for {month} templates, else by day."""
    if months or ('{month}' in template and '{date}' not in template):
        candidates = []
        year, month = today.year, today.month + 1  # payers post next month's edition early
        for _ in range(max(months, 2)):
            if month == 0:
                year, month = year - 1, 12
            candidates.append(date(year, month, 1) if month <= 12 else date(year + 1, 1, 1))
            month -= 1
    else:
        candidates = [today - timedelta(days=back) for back in range(-3, days)]
    for day in candidates:
        url = template.format(date=day.isoformat(), month=day.strftime('%Y-%m'))
        info = head(url)
        if info.get('status') in (200, 206):
            return url, day
    raise DiscoveryError(f'No index found for {template} in the last {days} days')


def _toc(payer: dict, today: date) -> list[FileTarget]:
    urls = payer.get('toc_urls') or []
    if payer.get('toc_url_template'):
        urls = urls + [_probe_dated(payer['toc_url_template'], today, payer.get('probe_days', 45))[0]]
    targets = []
    for url in urls:
        data = _get_bytes(url, payer.get('max_index_bytes', 200_000_000))
        if data[:2] == b'\x1f\x8b':
            import gzip
            data = gzip.decompress(data)
        elif data[:2] == b'PK':
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                members = [m for m in archive.infolist() if m.filename.endswith('.json')]
                if len(members) != 1:
                    raise DiscoveryError(f'{url}: expected one JSON in the index ZIP, found {len(members)}')
                data = archive.read(members[0])
        targets += toc_targets(payer['id'], url, io.BytesIO(data), payer.get('include'), payer.get('exclude'))
    return targets


def _cigna(payer: dict, today: date) -> list[FileTarget]:
    latest = _json_bytes(_get_bytes(payer.get('latest_url', 'https://www.cigna.com/static/mrf/latest.json')))
    tocs = [f for group in latest.get('mrfs', []) if group.get('kind') == 'TOC' for f in group.get('files', [])]
    if not tocs:
        raise DiscoveryError('Cigna latest.json lists no table of contents')
    targets = []
    for toc in tocs:
        data = _get_bytes(toc['url'])
        index_url = toc['url'].split('?', 1)[0]
        if data[:2] == b'PK':  # November 2026 onward: a ZIP of several index JSONs
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                for member in archive.infolist():
                    if member.filename.endswith('.json') and re.search(payer.get('index_member', r'cigna-health-life'), member.filename):
                        with archive.open(member) as stream:
                            targets += toc_targets(payer['id'], f'{index_url}#{member.filename}', stream, payer.get('include'), payer.get('exclude'))
        else:
            targets += toc_targets(payer['id'], index_url, io.BytesIO(data), payer.get('include'), payer.get('exclude'))
    return targets


def _blob_catalog(payer: dict, today: date) -> list[FileTarget]:
    catalog = _json_bytes(_get_bytes(payer['catalog_url']))
    blobs = [b for b in catalog.get('blobs', []) if re.search(payer.get('kind_pattern', r'in-network-rates'), b.get('name', ''))
             and _matches(b['name'], payer.get('include'), payer.get('exclude'))]
    if not blobs:
        return []
    newest: dict[str, dict] = {}
    for blob in blobs:  # one file per series: the newest dated edition listed
        key = series_key(payer['id'], blob['name'])
        if key not in newest or blob['name'] > newest[key]['name']:
            newest[key] = blob
    return [FileTarget(payer['id'], b['downloadUrl'], b['name'], series_key(payer['id'], b['name']),
                       (DATE_IN_NAME.search(b['name']) or [None])[0] if DATE_IN_NAME.search(b['name']) else None,
                       payer['catalog_url'], size=b.get('size')) for b in newest.values()]


def _direct(payer: dict, today: date) -> list[FileTarget]:
    targets = []
    for template in payer['url_templates']:
        try:
            url, day = _probe_dated(template, today, payer.get('probe_days', 62), payer.get('month_probe', 0)) if '{' in template else (template, None)
        except DiscoveryError:
            if payer.get('allow_missing_templates'):
                continue
            raise
        name = file_name(url)
        targets.append(FileTarget(payer['id'], url, name, series_key(payer['id'], file_name(template)), day.isoformat() if day else None, None))
    return targets


def linked_location(links: list[str]):
    """Map a table-of-contents location to the page's link for the same file on the same host.

    Some publishers' tables of contents give a path that answers 404 (an extra
    directory, different case) while their page links the same file name on the
    same host. The page's link is what can be read, so it is the file's URL; a
    location the page does not link exactly once is kept as listed (and fails
    loudly if it cannot be read).
    """
    by_name: dict[tuple[str, str], list[str]] = {}
    for link in links:
        by_name.setdefault((urllib.parse.urlsplit(link).netloc.lower(), file_name(link).lower()), []).append(link)
    listed = set(links)

    def resolve(location: str) -> str:
        if location in listed:
            return location
        same = by_name.get((urllib.parse.urlsplit(location).netloc.lower(), file_name(location).lower()), [])
        return same[0] if len(same) == 1 else location
    return resolve


def _listing(payer: dict, today: date) -> list[FileTarget]:
    """An HTML page of links: use the newest linked table of contents, else the newest file per series.

    `index_pattern` matches case-insensitively, as `include`/`exclude` do: a
    publisher may change the case of its index name between editions.
    """
    page = _get_bytes(payer['page_url'], 20_000_000).decode('utf-8', 'replace')
    links = sorted(set(re.findall(r'href=["\']([^"\']+)["\']', page)))
    links = [urllib.parse.urljoin(payer['page_url'], link) for link in links]
    if payer.get('index_pattern'):
        indexes = sorted((l for l in links if re.search(payer['index_pattern'], l, re.I)), key=lambda l: (DATE_IN_NAME.search(l) or [''])[0])
        if not indexes:
            raise DiscoveryError(f"{payer['id']}: no table of contents linked from {payer['page_url']}")
        data = _get_bytes(indexes[-1])
        return toc_targets(payer['id'], indexes[-1], io.BytesIO(data), payer.get('include'), payer.get('exclude'),
                           url_fix=linked_location(links))
    newest: dict[str, str] = {}
    for link in links:
        if _matches(link, payer.get('include'), payer.get('exclude')):
            key = series_key(payer['id'], file_name(link))
            if key not in newest or link > newest[key]:
                newest[key] = link
    return [FileTarget(payer['id'], l, file_name(l), series_key(payer['id'], file_name(l)),
                       (DATE_IN_NAME.search(l) or [None])[0] if DATE_IN_NAME.search(l) else None, payer['page_url']) for l in newest.values()]


__all__ = ['DiscoveryError', 'FetchError', 'FileTarget', 'discover', 'series_key', 'toc_targets']
