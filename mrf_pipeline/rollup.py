"""Turn extracted price rows into current, deduplicated rates.

A rate's identity is everything that can change what is paid: payer, whose
rate it is (subject entity vs a named market organization), billing code or
wildcard scope, modifiers, negotiated_type, arrangement, billing class,
setting, place of service, expiration, additional information and the amount.
The same rate repeated across plans, files and provider groups collapses to
one record that lists every plan, network and source file it appears in.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from . import config
from .codes import FIFTEEN_MINUTE_CODES, PER_SERVICE_LABEL, UNIT_BASIS, UNIT_LABEL, hourly_equivalent

OPEN_ENDED = '9999-12-31'
VALUE_KINDS = {
    'negotiated': 'dollars_per_unit',
    'fee schedule': 'dollars_per_unit',
    'derived': 'derived_dollars',
    'per diem': 'dollars_per_day',
    'percentage': 'percent_of_billed_charges',
}


def value_kind(row: dict) -> str:
    if row.get('negotiation_arrangement') in {'bundle', 'capitation'}:
        return f"{row['negotiation_arrangement']}_amount"
    return VALUE_KINDS.get(row.get('negotiated_type') or '', 'unknown_type')


def expiration_status(row: dict, as_of: date) -> str:
    value = row.get('expiration_date')
    if not value:
        return 'missing'
    if value == OPEN_ENDED:
        return 'open_ended'
    try:
        expires = date.fromisoformat(value[:10])
    except ValueError:
        return 'unparsed'
    return 'expired' if expires < as_of else 'active'


def rate_key(row: dict) -> tuple:
    return (
        row['payer_id'], row['provider_role'], row['entity_id'], row.get('tin_value') if row['provider_role'] != 'market' else None,
        row.get('wildcard_scope') or row['billing_code'], tuple(row.get('billing_code_modifier') or ()),
        row.get('negotiated_type'), row.get('negotiation_arrangement'), row.get('billing_class'), row.get('setting'),
        tuple(row.get('service_code') or ()), row.get('expiration_date'), row.get('additional_information'),
        # Key on the value so "120.0" and "120.00" in two files are one rate; output keeps source text.
        Decimal(row['negotiated_rate']).normalize() if row.get('negotiated_rate') is not None else None,
    )


def latest_editions(rows: list[dict]) -> list[dict]:
    """Keep only rows from the newest edition of each file series."""
    newest: dict[str, tuple] = {}
    for row in rows:
        stamp = (row.get('file_last_updated_on') or '', row.get('retrieved_at') or '')
        series = row['file_series']
        if series not in newest or stamp > newest[series]:
            newest[series] = stamp
    return [r for r in rows if (r.get('file_last_updated_on') or '', r.get('retrieved_at') or '') == newest[r['file_series']]]


def current_rates(rows: list[dict], as_of: date) -> tuple[list[dict], list[dict]]:
    """Return (current rate records, excluded rows with reasons)."""
    excluded, grouped = [], defaultdict(list)
    for row in latest_editions(rows):
        status = expiration_status(row, as_of)
        reason = ('expired' if status == 'expired' else 'expiration_unparsed' if status == 'unparsed'
                  else 'identity_conflict' if row.get('identity_conflict')
                  else 'invalid_rate' if row.get('negotiated_rate') is None else None)
        if reason:
            excluded.append({**row, 'exclusion_reason': reason})
            continue
        grouped[rate_key(row)].append(row)
    records = []
    subject = config.subject()['name']
    for key, group in grouped.items():
        first = group[0]
        own = first['provider_role'] != 'market'
        kind = value_kind(first)
        amount = Decimal(first['negotiated_rate'])
        dollars = kind == 'dollars_per_unit' and not first.get('wildcard_scope')
        per_unit = dollars and first['billing_code'] in FIFTEEN_MINUTE_CODES
        plans = {}
        for row in group:
            for plan in row.get('plans') or []:
                plans[(plan.get('plan_id_type'), plan.get('plan_id'), plan.get('plan_name'))] = plan
        sources = {}
        for row in group:
            sources[row['source_file_url']] = {'url': row['source_file_url'], 'last_updated_on': row.get('file_last_updated_on'),
                                              'sha256': row.get('file_sha256'), 'retrieved_at': row.get('retrieved_at')}
        records.append({
            'payer_id': first['payer_id'], 'payer_name': first['payer_name'],
            'provider_role': first['provider_role'],
            'rate_owner': f'{subject} contracted rate (exact {subject} TIN/NPI match)' if own
                          else f"Market rate: {first['entity_name']} (not {subject})",
            'entity_id': first['entity_id'], 'entity_name': first['entity_name'], 'tin_value': first.get('tin_value') if own else None,
            'billing_code': first['billing_code'], 'wildcard_scope': first.get('wildcard_scope'),
            'billing_code_modifier': list(first.get('billing_code_modifier') or []),
            'negotiated_type': first.get('negotiated_type'), 'negotiation_arrangement': first.get('negotiation_arrangement'),
            'value_kind': kind, 'negotiated_rate': first['negotiated_rate'],
            'unit_basis': UNIT_BASIS if per_unit else 'per_service' if dollars else None,
            'unit_label': UNIT_LABEL if per_unit else PER_SERVICE_LABEL if dollars else None,
            'hourly_equivalent': format(hourly_equivalent(amount), 'f') if per_unit else None,
            'hourly_label': 'x4 conversion of the per-15-minute CPT unit amount; not a published hourly rate' if per_unit else None,
            'billing_class': first.get('billing_class'), 'setting': first.get('setting'),
            'service_code': list(first.get('service_code') or []), 'expiration_date': first.get('expiration_date'),
            'expiration_status': expiration_status(first, as_of), 'additional_information': first.get('additional_information'),
            'networks': sorted({n for r in group for n in r.get('network_names') or []}),
            'provider_group_ids': sorted({r['provider_group_id'] for r in group if r.get('provider_group_id')}),
            'plans': sorted(plans.values(), key=lambda p: (str(p.get('plan_name')), str(p.get('plan_id')))),
            'plan_count': len(plans),
            'source_files': sorted(sources.values(), key=lambda s: s['url']),
            'source_file_count': len(sources),
            'latest_file_last_updated_on': max((s['last_updated_on'] or '') for s in sources.values()) or None,
            'observation_count': len(group),
        })
    _flag_unresolved_ladders(records)
    records.sort(key=lambda r: (r['payer_id'], r['provider_role'], r['entity_id'], r['billing_code'], r['billing_code_modifier'], r['value_kind'], Decimal(r['negotiated_rate'])))
    return records, excluded


def _flag_unresolved_ladders(records: list[dict]) -> None:
    """Several amounts for one payer/entity/code/modifier context with nothing in the
    file to tell them apart (often clinician-level tiers). Flag; never pick one."""
    contexts = defaultdict(list)
    for record in records:
        contexts[(record['payer_id'], record['entity_id'], record['billing_code'], tuple(record['billing_code_modifier']),
                  record['value_kind'], record['billing_class'], tuple(record['service_code']))].append(record)
    for group in contexts.values():
        amounts = sorted({Decimal(r['negotiated_rate']) for r in group})
        group_sets = [set(r['provider_group_ids']) for r in group]
        separate_groups = all(a.isdisjoint(b) for i, a in enumerate(group_sets) for b in group_sets[i + 1:]) and all(group_sets)
        for record in group:
            record['distinct_amounts_in_context'] = len(amounts)
            if len(amounts) <= 1:
                record['ambiguity_note'] = None
            elif separate_groups:
                record['ambiguity_note'] = (f'{len(amounts)} amounts published for this code, each under different provider groups '
                                            '(separate contract contexts); the file does not say which applies to a given claim')
            else:
                record['ambiguity_note'] = (f'{len(amounts)} amounts published for this code with no modifier or other field to distinguish them; '
                                            'the file does not say which applies to a given claim')
