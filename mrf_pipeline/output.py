"""Write run outputs: Parquet tables for DuckDB plus a CSV staff can open.

Tables (one directory per run):
- rates.parquet: every extracted price row with full file provenance
- files.parquet: one row per source file, including failures and early stops
- current_rates.parquet / current_rates.csv: deduplicated current rates
- held_rates.parquet: rates whose files state a date after their capture that the
  publisher's catalog does not corroborate (publication_date_conflict); never current
- excluded_rates.parquet: rows left out of current rates, with the reason
- run.json: run manifest, freshness verdicts and problems
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

LIST_FIELDS = {'network_names', 'matched_npis', 'service_code', 'billing_code_modifier', 'bundled_tracked_codes',
               'networks', 'provider_group_ids'}
JSON_FIELDS = {'plans', 'source_files', 'problems', 'problem_examples', 'stats', 'identical_source_files'}


def _json_column(values: list):
    """Dictionary-encoded JSON text: each distinct value is serialized and stored once.

    Every row of a file carries that file's plan list (up to 2,000 plans), so a
    plain string column repeats it per row and exhausts memory on a full month.
    """
    import pyarrow as pa
    by_object, by_text, dictionary, indices = {}, {}, [], []
    for value in values:
        if value is None:
            indices.append(None)
            continue
        index = by_object.get(id(value))
        if index is None:
            text = json.dumps(value, sort_keys=True, default=str)
            index = by_text.setdefault(text, len(dictionary))
            if index == len(dictionary):
                dictionary.append(text)
            by_object[id(value)] = index
        indices.append(index)
    return pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int32()), pa.array(dictionary, type=pa.string()))


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
            columns[key] = _json_column(values)
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


def _decode_json(array, cache: dict) -> list:
    """JSON text column (dictionary-encoded or plain) to shared Python objects."""
    def load(text):
        if text is None:
            return None
        if text not in cache:
            cache[text] = json.loads(text)
        return cache[text]
    if hasattr(array, 'dictionary'):
        decoded = [load(text) for text in array.dictionary.to_pylist()]
        return [None if i is None else decoded[i] for i in array.indices.to_pylist()]
    return [load(text) for text in array.to_pylist()]


def iter_rows(path: Path, cache: dict | None = None, batch_size: int = 50_000):
    """Yield a table's rows in batches; equal JSON values are one shared object (pass `cache` to share across files)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    if not path.exists():
        return
    json_columns = sorted(JSON_FIELDS & set(pq.read_schema(path).names))
    cache = {} if cache is None else cache
    for batch in pq.ParquetFile(path, read_dictionary=json_columns).iter_batches(batch_size=batch_size):
        table = pa.Table.from_batches([batch])
        decoded = {key: _decode_json(table.column(key).combine_chunks(), cache) for key in json_columns}
        rows = table.drop_columns(json_columns).to_pylist()
        for key, values in decoded.items():
            for row, value in zip(rows, values):
                row[key] = value
        yield from rows


def read_rows(path: Path) -> list[dict]:
    """Read a table back; equal JSON values come back as one shared object."""
    return list(iter_rows(path))


CSV_COLUMNS = [
    'payer_name', 'rate_owner', 'billing_code', 'wildcard_scope', 'bundled_tracked_codes', 'billing_code_modifier', 'value_kind',
    'negotiated_type', 'negotiation_arrangement', 'negotiated_rate', 'unit_label', 'hourly_equivalent', 'hourly_label',
    'billing_class', 'setting', 'service_code', 'expiration_date', 'networks', 'plan_count', 'plan_list_truncated', 'plan_names',
    'latest_file_last_updated_on', 'source_file_urls', 'distinct_amounts_in_context', 'ambiguity_note', 'additional_information',
    'entity_name', 'tin_value', 'payer_id', 'identical_source_file_count',
]


def write_current_csv(path: Path, records: list[dict]) -> None:
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, CSV_COLUMNS)
        writer.writeheader()
        for record in records:
            row = {k: record.get(k) for k in CSV_COLUMNS}
            for key in ('billing_code_modifier', 'service_code', 'networks', 'bundled_tracked_codes'):
                row[key] = '|'.join(record.get(key) or [])
            row['plan_names'] = '|'.join(sorted({p.get('plan_name', '') for p in record.get('plans') or []}))[:2000]
            row['source_file_urls'] = '|'.join(s['url'] for s in record.get('source_files') or [])
            writer.writerow(row)


def write_run(out_dir: Path, *, rows, files, current, excluded, manifest, held=()) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(out_dir / 'rates.parquet', rows)
    write_parquet(out_dir / 'files.parquet', files)
    write_parquet(out_dir / 'current_rates.parquet', current)
    write_parquet(out_dir / 'held_rates.parquet', list(held))
    write_parquet(out_dir / 'excluded_rates.parquet', excluded)
    write_current_csv(out_dir / 'current_rates.csv', current)
    (out_dir / 'run.json').write_text(json.dumps(manifest, indent=1, sort_keys=True, default=str) + '\n')
