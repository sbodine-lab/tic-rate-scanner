"""Monthly run: discover each payer's newest files, stream them, publish current rates.

    python -m mrf_pipeline run --out runs/2026-10-30 [--payers payer_a,payer_b] [--workers 8]
    python -m mrf_pipeline discover [--payers ...]    # list files only, nothing streamed
    python -m mrf_pipeline combine runs/a runs/b --out runs/final   # merge runs
    python -m mrf_pipeline plan --out plan.json --shards 16          # cloud: discover once
    python -m mrf_pipeline run --plan plan.json --shard 3 --out shard-3
    python -m mrf_pipeline combine --plan plan.json shard-* --out final

Exit status is 1 when any TiC payer is missing an expected file, is older than
its allowed age, or has a file that could not be read completely; the run's
run.json says which and why. Outputs are written either way so a partial
month is inspectable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import tomllib
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import config
from . import discover as discovery
from .codes import TRACKED_CODES
from .rollup import current_rates

TIC = 'tic'


def load_payers(path: Path | None = None, only: list[str] | None = None) -> list[dict]:
    payers = tomllib.loads((path or config.path('payers')).read_text())['payer']
    for payer in payers:
        payer.setdefault('category', TIC)
    if only:
        unknown = set(only) - {p['id'] for p in payers}
        if unknown:
            raise SystemExit(f'Unknown payer id(s): {", ".join(sorted(unknown))}')
        payers = [p for p in payers if p['id'] in only]
    return payers


def process_file(target: dict, scope: str, scratch: str | None, attempts: int = 3) -> dict:
    """Worker: stream one file. Returns a file record and its annotated rows."""
    from .extract import ExtractionError, extract
    from .fetch import FetchError, open_json_stream
    from .match import Matcher
    matcher = Matcher.from_repo(include_market=_includes_market(scope))
    started = time.time()
    last_error = None
    repair = False
    for attempt in range(1, attempts + 1):
        try:
            with open_json_stream(target['url'], scratch_dir=scratch, repair_text=repair) as (stream, meta):
                result = extract(stream, matcher, stop_when_unmatched=not _includes_market(scope))
                meta.stopped_early = bool(result.stats.get('early_stop'))
            break
        except (FetchError, ExtractionError, EOFError, OSError) as error:
            last_error = f'{type(error).__name__}: {error}'
            if 'invalid bytes in UTF8' in last_error and not repair:
                repair = True  # re-read once with Windows-1252 repair of invalid bytes only
                continue
            if attempt == attempts:
                return {'file': {**_file_base(target), 'status': 'failed', 'error': last_error, 'attempts': attempt,
                                 'seconds': round(time.time() - started, 1)}, 'rows': []}
            time.sleep(10 * attempt)
    status = ('no_match_references_scanned' if result.stats.get('early_stop') else
              'complete' if result.complete else 'partial')
    file_record = {**_file_base(target), 'status': status, 'error': None, 'attempts': attempt,
                   'seconds': round(time.time() - started, 1), 'last_updated_on': result.header.get('last_updated_on'),
                   'reporting_entity_name': result.header.get('reporting_entity_name'),
                   'reporting_entity_type': result.header.get('reporting_entity_type'),
                   'schema_version': result.header.get('version'), 'compression': meta.compression,
                   'bytes_read': meta.bytes_read, 'content_length': meta.content_length, 'sha256': meta.sha256,
                   'etag': meta.etag, 'http_last_modified': meta.last_modified, 'retrieved_at': meta.retrieved_at,
                   'resumes': meta.resumes, 'text_repairs': meta.text_repairs, 'stats': result.stats, 'problems': result.problems,
                   'problem_examples': result.problem_examples}
    rows = [{**row, 'payer_id': target['payer_id'], 'payer_name': target['payer_name'],
             'source_file_url': target['provenance_url'], 'source_index_url': target['index_url'],
             'file_series': target['series'], 'file_last_updated_on': result.header.get('last_updated_on'),
             'file_sha256': meta.sha256, 'retrieved_at': meta.retrieved_at,
             'reporting_entity_name': result.header.get('reporting_entity_name'),
             'schema_version': result.header.get('version'), 'file_status': status,
             'plans': target['plans']} for row in result.rows]
    return {'file': file_record, 'rows': rows}


def _includes_market(scope: str) -> bool:
    """'subject+market' (default) reads market rows; any scope without '+market' is subject-only."""
    return scope.endswith('+market')


def _file_base(target: dict) -> dict:
    return {k: target[k] for k in ('payer_id', 'payer_name', 'name', 'series', 'published', 'size')} | {
        'url': target['provenance_url'], 'index_url': target['index_url'], 'plan_count': len(target['plans']),
        'plan_count_total': target.get('plan_count_total', len(target['plans'])),
        'plans_truncated': target.get('plan_count_total', len(target['plans'])) > len(target['plans'])}


def freshness(payer: dict, names: list[str], files: list[dict], today: date, discovery_error: str | None) -> dict:
    if payer['category'] != TIC:
        return {'payer_id': payer['id'], 'payer_name': payer['name'], 'status': 'not_tic', 'category': payer['category'],
                'rate_source': payer.get('rate_source'), 'rate_source_url': payer.get('rate_source_url')}
    verdict = {'payer_id': payer['id'], 'payer_name': payer['name'], 'category': TIC, 'files_discovered': len(names),
               'files_complete': sum(f['status'] in {'complete', 'no_match_references_scanned'} for f in files),
               'files_failed': [f['name'] for f in files if f['status'] == 'failed'],
               'files_partial': [f['name'] for f in files if f['status'] == 'partial']}
    if discovery_error:
        return {**verdict, 'status': 'failed', 'reason': discovery_error}
    missing = [p for p in payer.get('expected', []) if not any(re.search(p, n) for n in names)]
    dates = sorted(f['last_updated_on'] for f in files if f.get('last_updated_on'))
    newest = dates[-1] if dates else None
    oldest_allowed = (today - timedelta(days=payer.get('max_age_days', 45))).isoformat()
    verdict.update({'newest_last_updated_on': newest, 'oldest_last_updated_on': dates[0] if dates else None,
                    'oldest_allowed': oldest_allowed, 'missing_expected': missing})
    problems = []
    if missing:
        problems.append('missing expected file(s): ' + ', '.join(missing))
    if verdict['files_failed']:
        problems.append(f"{len(verdict['files_failed'])} file(s) could not be read")
    if verdict['files_partial']:
        problems.append(f"{len(verdict['files_partial'])} file(s) incomplete (undefined or unresolved provider references)")
    if not newest:
        problems.append('no file reported last_updated_on')
    elif newest < oldest_allowed:
        problems.append(f'newest file {newest} is older than {oldest_allowed}')
    verdict['status'] = 'ok' if not problems else 'failed'
    verdict['reason'] = '; '.join(problems) or None
    return verdict


def command_discover(args) -> int:
    today = date.fromisoformat(args.as_of) if args.as_of else date.today()
    failures = 0
    for payer in load_payers(only=args.payers):
        if payer['category'] != TIC:
            print(f"{payer['id']:16} not TiC -> {payer.get('rate_source')}")
            continue
        try:
            targets = discovery.discover(payer, today=today)
        except discovery.DiscoveryError as error:
            failures += 1
            print(f"{payer['id']:16} FAILED: {error}")
            continue
        total = sum(t.size or 0 for t in targets)
        print(f"{payer['id']:16} {len(targets):4} file(s){f', {total / 1e9:.2f} GB listed' if total else ''}")
        for target in targets[: args.show]:
            print(f"    {target.published or '?':10} {target.name[:110]} plans={target.plan_count_total}")
    return 1 if failures else 0


def _discover_jobs(payers: list[dict], today: date, *, allow_cloud_only: bool, default_scope: str):
    """Discover every TiC payer once. Returns (jobs, discovered file names per payer, discovery errors)."""
    jobs, discovered, errors = [], {}, {}
    for payer in payers:
        if payer['category'] != TIC:
            continue
        discovered[payer['id']] = []
        if payer.get('cloud_only') and not allow_cloud_only:
            errors[payer['id']] = 'cloud_only payer skipped on this host (pass --allow-cloud-only)'
            continue
        try:
            targets = discovery.discover(payer, today=today)
        except discovery.DiscoveryError as error:
            errors[payer['id']] = str(error)
            continue
        discovered[payer['id']] = [t.name for t in targets]
        for target in targets:
            jobs.append({'payer_id': payer['id'], 'payer_name': payer['name'], 'url': target.url, 'name': target.name,
                         'series': target.series, 'published': target.published, 'size': target.size,
                         'index_url': target.index_url, 'provenance_url': target.provenance_url(), 'plans': target.plans,
                         'plan_count_total': target.plan_count_total,
                         'scope': payer.get('scope', default_scope)})
    return jobs, discovered, errors


def _process_jobs(jobs: list[dict], workers: int, scratch: str | None) -> tuple[list[dict], list[dict]]:
    files, rows = [], []
    quiet = config.quiet_log()
    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(process_file, job, job['scope'], scratch) for job in jobs]
        for future in as_completed(futures):
            result = future.result()
            files.append(result['file'])
            rows.extend(result['rows'])
            record = result['file']
            where = '' if quiet else f"{record['payer_id']:12} "
            name = '' if quiet else f"  {record['name'][:80]}"
            print(f"[{len(files)}/{len(jobs)}] {where}{record['status']:28} {record.get('seconds', 0):7.1f}s "
                  f"{(record.get('bytes_read') or 0) / 1e6:9.1f} MB{name}", flush=True)
    return files, rows


def _finish(out: Path, payers: list[dict], discovered: dict, errors: dict, files: list[dict], rows: list[dict],
            today: date, extra: dict, members: dict[str, list[str]] | None = None) -> int:
    from .output import write_run
    current, excluded = current_rates(rows, today)
    for record in current if members else []:  # files whose rows equal a cited file's rows also publish the rate
        identical = sorted(u for source in record['source_files'] for u in members.get(source['url'], []) if u != source['url'])
        record['identical_source_files'] = identical
        record['identical_source_file_count'] = len(identical)
    verdicts = [freshness(p, discovered.get(p['id'], []), [f for f in files if f['payer_id'] == p['id']], today, errors.get(p['id']))
                for p in payers]
    manifest = {'as_of': today.isoformat(), 'finished_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                'tracked_codes': sorted(TRACKED_CODES), 'payers': verdicts, **extra,
                'counts': {'files': len(files), 'rows': len(rows), 'current_rates': len(current), 'excluded_rows': len(excluded),
                           'subject_current_rates': sum(r['provider_role'] != 'market' for r in current)}}
    write_run(out, rows=rows, files=files, current=current, excluded=excluded, manifest=manifest)
    failed = [v for v in verdicts if v['status'] == 'failed']
    if config.quiet_log():
        print(f"\n{len(verdicts) - len(failed)} payer(s) ok, {len(failed)} failed; details are in the encrypted run")
        return 1 if failed else 0
    print('\nPayer freshness')
    for verdict in verdicts:
        detail = verdict.get('reason') or verdict.get('newest_last_updated_on') or verdict.get('rate_source') or ''
        print(f"  {verdict['payer_id']:24} {verdict['status']:8} {detail}")
    print(f"\n{manifest['counts']['subject_current_rates']} current {config.subject()['name']} rate(s); outputs in {out}")
    if failed:
        print(f"FAILED: {', '.join(v['payer_id'] for v in failed)}", file=sys.stderr)
    return 1 if failed else 0


def command_plan(args) -> int:
    """Discover once and split the files into size-balanced shards for parallel hosts."""
    from concurrent.futures import ThreadPoolExecutor
    from .fetch import head
    today = date.fromisoformat(args.as_of) if args.as_of else date.today()
    payers = load_payers(only=args.payers)
    jobs, discovered, errors = _discover_jobs(payers, today, allow_cloud_only=True, default_scope=args.scope)
    unknown = [job for job in jobs if not job['size']]
    with ThreadPoolExecutor(max_workers=16) as pool:
        for job, info in zip(unknown, pool.map(lambda j: head(j['url']), unknown)):
            job['size'] = info.get('content_length')
    shards = [[] for _ in range(args.shards)]
    loads = [0] * args.shards
    for index in sorted(range(len(jobs)), key=lambda i: -(jobs[i]['size'] or 1_000_000_000)):
        target = loads.index(min(loads))  # largest file first onto the lightest shard
        shards[target].append(index)
        loads[target] += jobs[index]['size'] or 1_000_000_000
    plan = {'as_of': today.isoformat(), 'payer_ids': [p['id'] for p in payers], 'discovered': discovered, 'errors': errors,
            'jobs': jobs, 'shards': shards, 'shard_bytes': loads}
    Path(args.out).write_text(json.dumps(plan, indent=1) + '\n')
    print(f"{len(jobs)} file(s), {sum(loads) / 1e9:.1f} GB across {args.shards} shard(s); "
          f"largest shard {max(loads) / 1e9:.1f} GB; {len(errors)} discovery failure(s)")
    if args.github_matrix:
        with open(os.environ.get('GITHUB_OUTPUT', '/dev/stdout'), 'a') as handle:
            handle.write('matrix=' + json.dumps({'shard': [i for i, s in enumerate(shards) if s]}) + '\n')
    return 0


def command_run(args) -> int:
    out = Path(args.out)
    started = datetime.now(timezone.utc).isoformat(timespec='seconds')
    if args.plan:
        plan = json.loads(Path(args.plan).read_text())
        today = date.fromisoformat(plan['as_of'])
        jobs = [plan['jobs'][i] for i in plan['shards'][args.shard]]
        print(f'Shard {args.shard}: {len(jobs)} file(s)', flush=True)
        files, rows = _process_jobs(jobs, args.workers, args.scratch)
        from .output import write_run
        write_run(out, rows=rows, files=files, current=[], excluded=[],
                  manifest={'as_of': today.isoformat(), 'shard': args.shard, 'started_at': started, 'host': os.uname().nodename,
                            'counts': {'files': len(files), 'rows': len(rows)}})
        failed = [f['name'] for f in files if f['status'] == 'failed']
        print(f"Shard {args.shard} done: {len(files)} file(s), {len(failed)} failed")
        return 0  # verdicts are decided once, by combine --plan, across all shards
    today = date.fromisoformat(args.as_of) if args.as_of else date.today()
    payers = load_payers(only=args.payers)
    jobs, discovered, errors = _discover_jobs(payers, today, allow_cloud_only=args.allow_cloud_only, default_scope=args.scope)
    print(f'{len(jobs)} file(s) from {len(discovered)} payer(s); {len(errors)} discovery failure(s)', flush=True)
    files, rows = _process_jobs(jobs, args.workers, args.scratch)
    return _finish(out, payers, discovered, errors, files, rows, today,
                   {'started_at': started, 'host': os.uname().nodename, 'scope_default': args.scope})


def _read_rows(path: Path) -> list[dict]:
    from .output import read_rows
    return read_rows(path)


# Where a row sits in its file, not what it says: provenance, positions and the file's
# own reference numbers. Left out when comparing two files' rows.
FILE_LOCAL_FIELDS = frozenset({'source_file_url', 'source_index_url', 'file_sha256', 'retrieved_at', 'plans', 'file_series',
                               'item_ordinal', 'block_ordinal', 'price_ordinal', 'provider_group_id'})


def _row_digest(row: dict) -> int:
    content = {k: v for k, v in row.items() if k not in FILE_LOCAL_FIELDS}
    return int.from_bytes(hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).digest(), 'big')


def _plan_union(lists) -> list[dict]:
    merged = {}
    for listed in lists:
        for plan in listed:
            merged[(plan.get('plan_id_type'), plan.get('plan_id'), plan.get('plan_name'))] = plan
    return sorted(merged.values(), key=lambda p: (str(p.get('plan_name')), str(p.get('plan_id'))))


def _collapse_identical_files(directories: list[Path], files: list[dict]) -> tuple[list[dict], dict[str, list[str]]]:
    """Stream shard rows twice; keep one file's rows where several files publish exactly the same rows.

    Some payers post one file per plan with identical rates (Aetna: ~300 files,
    ~11,000 rows each). Two files are identical when they have the same payer,
    last_updated_on and the same multiset of rows, file-local fields aside (an
    order-independent sum of per-row SHA-256 digests plus the row count). The
    first URL of each group keeps its rows, carrying the union of the group's
    plans; the others are recorded on files.parquet as identical_to it.
    Returns (kept rows, representative URL -> every member URL).
    """
    from .output import iter_rows
    cache: dict = {}
    digest, count, plans, identity = defaultdict(int), defaultdict(int), {}, {}
    for directory in directories:
        for row in iter_rows(directory / 'rates.parquet', cache):
            url = row['source_file_url']
            digest[url] = (digest[url] + _row_digest(row)) % (1 << 256)
            count[url] += 1
            plans[url] = row.get('plans') or []
            identity[url] = (row['payer_id'], row.get('file_last_updated_on'))
    groups = defaultdict(list)
    for url in digest:
        groups[(*identity[url], digest[url], count[url])].append(url)
    representative, members, union = {}, {}, {}
    for urls in groups.values():
        urls.sort()
        members[urls[0]] = urls
        for url in urls:
            representative[url] = urls[0]
        if len(urls) > 1:
            union[urls[0]] = _plan_union(plans[url] for url in urls)
    for record in files:
        url = record['url']
        record['rows_sha256'] = format(digest[url], '064x') if url in digest else None
        record['identical_to'] = representative[url] if representative.get(url, url) != url else None
        record['identical_file_count'] = len(members[url]) if url in members else None
    rows = []
    for directory in directories:
        for row in iter_rows(directory / 'rates.parquet', cache):
            url = row['source_file_url']
            if representative[url] != url:
                continue
            if url in union:
                row['plans'] = union[url]
            rows.append(row)
    return rows, members


def command_combine(args) -> int:
    """Merge run directories into one current-rates output.

    With --plan, the directories are shards of one planned run: files and rows
    are unioned and each payer's verdict is decided from the plan's discovery
    and every shard's file records. Without it, the directory listed last that
    has a verdict for a payer wins for that payer.
    """
    if args.plan:
        plan = json.loads(Path(args.plan).read_text())
        today = date.fromisoformat(plan['as_of'])
        directories = [Path(d) for d in args.runs]
        files = [f for directory in directories for f in _read_rows(directory / 'files.parquet')]
        planned = {job['provenance_url'] for job in plan['jobs']}
        seen = {f['url'] for f in files}
        for job in plan['jobs']:  # a shard that died must not look like a quiet payer
            if job['provenance_url'] not in seen:
                files.append({**_file_base(job), 'status': 'failed', 'error': 'shard produced no result for this file'})
        files = [f for f in files if f['url'] in planned]
        rows, members = _collapse_identical_files(directories, files)
        payers = load_payers(only=plan['payer_ids'])
        return _finish(Path(args.out), payers, plan['discovered'], plan['errors'], files, rows, today,
                       {'combined_from': args.runs}, members=members)
    from .output import write_run
    today = date.fromisoformat(args.as_of) if args.as_of else date.today()
    verdicts, rows_by_payer, files_by_payer = {}, {}, {}
    for directory in map(Path, args.runs):
        manifest = json.loads((directory / 'run.json').read_text())
        rows, files = _read_rows(directory / 'rates.parquet'), _read_rows(directory / 'files.parquet')
        for verdict in manifest['payers']:
            if (verdict.get('reason') or '').startswith('cloud_only payer skipped') and verdict['payer_id'] in verdicts:
                continue
            payer = verdict['payer_id']
            verdicts[payer] = verdict
            rows_by_payer[payer] = [r for r in rows if r['payer_id'] == payer]
            files_by_payer[payer] = [f for f in files if f['payer_id'] == payer]
    rows = [r for group in rows_by_payer.values() for r in group]
    files = [f for group in files_by_payer.values() for f in group]
    current, excluded = current_rates(rows, today)
    manifest = {'as_of': today.isoformat(), 'combined_from': args.runs, 'payers': list(verdicts.values()),
                'counts': {'files': len(files), 'rows': len(rows), 'current_rates': len(current), 'excluded_rows': len(excluded),
                           'subject_current_rates': sum(r['provider_role'] != 'market' for r in current)}}
    write_run(Path(args.out), rows=rows, files=files, current=current, excluded=excluded, manifest=manifest)
    failed = [v['payer_id'] for v in verdicts.values() if v['status'] == 'failed']
    print(f"{manifest['counts']['subject_current_rates']} current {config.subject()['name']} rate(s); failed: {', '.join(failed) or 'none'}")
    return 1 if failed else 0


def command_check_references(args) -> int:
    from .references import check, load
    current = _read_rows(Path(args.run) / 'current_rates.parquet')
    results = check(load(), current)
    (Path(args.run) / 'reference_check.json').write_text(json.dumps(results, indent=1) + '\n')
    for r in [] if config.quiet_log() else results:
        print(f"{r['status']:9} {r['payer_id']:10} {r['code']:6} {'|'.join(r['modifiers']) or '-':6} expected {r['amount']:>8}  {r['detail'] or ''}")
    bad = [r for r in results if r['status'] != 'match']
    print(f"{len(results) - len(bad)}/{len(results)} reference rates matched")
    return 1 if bad else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='mrf_pipeline', description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('run', 'discover', 'plan'):
        cmd = sub.add_parser(name)
        cmd.add_argument('--payers', type=lambda s: [p.strip() for p in s.split(',') if p.strip()])
        cmd.add_argument('--as-of', help='Date used for expiration and freshness checks (default: today)')
    combine = sub.add_parser('combine')
    combine.add_argument('runs', nargs='+', help='Run directories, oldest first')
    combine.add_argument('--out', required=True)
    combine.add_argument('--as-of')
    combine.add_argument('--plan', help='plan.json when the run directories are shards of one planned run')
    refs = sub.add_parser('check-references')
    refs.add_argument('run', help='Run directory with current_rates.parquet')
    run, disc, plan = sub.choices['run'], sub.choices['discover'], sub.choices['plan']
    plan.add_argument('--out', required=True)
    plan.add_argument('--shards', type=int, default=16)
    plan.add_argument('--scope', default='subject+market', help="'subject+market' or 'subject' (subject rows only, early stop)")
    plan.add_argument('--github-matrix', action='store_true', help='Also write the shard matrix to $GITHUB_OUTPUT')
    run.add_argument('--plan', help='plan.json from the plan command; process only --shard')
    run.add_argument('--shard', type=int, default=0)
    run.add_argument('--out', required=True)
    run.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 2) - 1))
    run.add_argument('--scope', default='subject+market', help="'subject+market' or 'subject' (subject rows only, early stop)")
    run.add_argument('--scratch', help='Directory for ZIP spools (needs free space about the size of the largest ZIP)')
    run.add_argument('--allow-cloud-only', action='store_true')
    disc.add_argument('--show', type=int, default=5)
    args = parser.parse_args(argv)
    return {'run': command_run, 'discover': command_discover, 'combine': command_combine, 'plan': command_plan,
            'check-references': command_check_references}[args.command](args)


if __name__ == '__main__':
    raise SystemExit(main())
