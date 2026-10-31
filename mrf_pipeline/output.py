"""Write run outputs: Parquet tables for DuckDB plus a CSV staff can open.

Tables (one directory per run):
- rates.parquet: every extracted price row with full file provenance
- files.parquet: one row per source file, including failures and early stops
- current_rates.parquet / current_rates.csv: deduplicated current rates
- excluded_rates.parquet: rows left out of current rates, with the reason
- run.json: run manifest, freshness verdicts and problems
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

LIST_FIELDS = {'network_names', 'matched_npis', 'service_code', 'billing_code_modifier', 'bundled_tracked_codes',
               'networks', 'provider_group_ids'}
JSON_FIELDS = {'plans', 'source_files', 'problems', 'problem_examples', 'stats'}


def _arrow_table(rows: list[dict]):
    import pyarrow as pa
    if not rows:
        return pa.table({})
    columns = {}
    for key in sorted({k for row in rows for k in row}):
        values = [row.get(key) for row in rows]
        if key in LIST_FIELDS:
            columns[key] = pa.array([list(v) if v is not None else None for v in values], type=pa.list_(pa.string()))
        elif key in JSON_FIELDS:
            columns[key] = pa.array([json.dumps(v, sort_keys=True, default=str) if v is not None else None for v in values], type=pa.string())
        elif all(v is None or isinstance(v, bool) for v in values):
            columns[key] = pa.array(values, type=pa.bool_())
        elif all(v is None or (isinstance(v, int) and not isinstance(v, bool)) for v in values):
            columns[key] = pa.array(values, type=pa.int64())
        else:
            # Amounts stay exact decimal text; DuckDB can CAST(... AS DECIMAL(18,4)).
            columns[key] = pa.array([None if v is None else str(v) for v in values], type=pa.string())
    return pa.table(columns)


def write_parquet(path: Path, rows: list[dict]) -> None:
    import pyarrow.parquet as pq
    pq.write_table(_arrow_table(rows), path, compression='zstd')


CSV_COLUMNS = [
    'payer_name', 'rate_owner', 'billing_code', 'wildcard_scope', 'billing_code_modifier', 'value_kind', 'negotiated_type',
    'negotiated_rate', 'unit_label', 'hourly_equivalent', 'hourly_label', 'billing_class', 'setting', 'service_code',
    'expiration_date', 'networks', 'plan_count', 'plan_names', 'latest_file_last_updated_on', 'source_file_urls',
    'distinct_amounts_in_context', 'ambiguity_note', 'additional_information', 'entity_name', 'tin_value', 'payer_id',
]


def write_current_csv(path: Path, records: list[dict]) -> None:
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, CSV_COLUMNS)
        writer.writeheader()
        for record in records:
            row = {k: record.get(k) for k in CSV_COLUMNS}
            for key in ('billing_code_modifier', 'service_code', 'networks'):
                row[key] = '|'.join(record.get(key) or [])
            row['plan_names'] = '|'.join(sorted({p.get('plan_name', '') for p in record.get('plans') or []}))[:2000]
            row['source_file_urls'] = '|'.join(s['url'] for s in record.get('source_files') or [])
            writer.writerow(row)


def write_run(out_dir: Path, *, rows, files, current, excluded, manifest) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(out_dir / 'rates.parquet', rows)
    write_parquet(out_dir / 'files.parquet', files)
    write_parquet(out_dir / 'current_rates.parquet', current)
    write_parquet(out_dir / 'excluded_rates.parquet', excluded)
    write_current_csv(out_dir / 'current_rates.csv', current)
    (out_dir / 'run.json').write_text(json.dumps(manifest, indent=1, sort_keys=True, default=str) + '\n')
