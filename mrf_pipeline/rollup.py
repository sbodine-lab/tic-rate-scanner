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
from email.utils import parsedate_to_datetime

from . import config
from .codes import FIFTEEN_MINUTE_CODES, PER_SERVICE_LABEL, UNIT_BASIS, UNIT_LABEL, hourly_equivalent

OPEN_ENDED = '9999-12-31'
# A file dated after its capture may be an early-posted edition only this far ahead (app/core/rates.py).
EARLY_POSTING_MAX_DAYS = 31
VALUE_KINDS = {
    'negotiated': 'dollars_per_unit',
    'fee schedule': 'dollars_per_unit',
    'derived': 'derived_dollars',
    'per diem': 'dollars_per_day',
    'percentage': 'percent_of_billed_charges',
}


def value_kind(row: dict) -> str:
    # A percentage of billed charges is never dollars, whatever the arrangement (CMS: the
    # negotiated_rate of a "percentage" price is a whole-number percent).
    if row.get('negotiated_type') == 'percentage':
        return VALUE_KINDS['percentage']
    if row.get('negotiation_arrangement') in {'bundle', 'capitation'}:
        return f"{row['negotiation_arrangement']}_amount"
    return VALUE_KINDS.get(row.get('negotiated_type') or '', 'unknown_type')


def exclusion_reason(row: dict, as_of: date, held_files=frozenset()) -> str | None:
    """Why an extracted price of the newest edition is not a current rate (None: it is).

    mrf_import._eligible_rows applies the same rule to the run's rows.
    """
    status = expiration_status(row, as_of)
    if status == 'expired':
        return 'expired'
    if status == 'unparsed':
        return 'expiration_unparsed'
    if row.get('identity_conflict'):
        return 'identity_conflict'
    if row.get('negotiated_rate') is None:
        return 'invalid_rate'
    if row.get('rate_problem') == 'non_positive_rate':
        return 'non_positive_rate'  # CMS: negotiated_rate has an exclusive minimum of 0
    if row.get('negotiated_type') not in VALUE_KINDS:
        return 'unknown_negotiated_type'  # outside the CMS enum: dollars or percent is unknown
    if row['source_file_url'] in held_files:
        return 'publication_date_conflict'
    return None


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


def _http_day(value: str | None) -> str | None:
    try:
        return parsedate_to_datetime(value).date().isoformat() if value else None
    except (TypeError, ValueError):
        return None


def publication_date_conflict(file: dict) -> bool:
    """A file dated after its own capture is conflicting evidence, not current.

    It is an early-posted edition instead only when the publisher's catalog
    states the same edition date, the transport copy predates capture and the
    date is at most EARLY_POSTING_MAX_DAYS ahead. The reader applies the same
    rule (app/core/rates.py _early_posted_on), so both call such a file's rates
    publication_date_conflict, never current.
    """
    stamp, captured = (file.get('last_updated_on') or '')[:10], (file.get('retrieved_at') or '')[:10]
    if not stamp or not captured or stamp <= captured:
        return False
    posted = _http_day(file.get('http_last_modified'))
    try:
        ahead = (date.fromisoformat(stamp) - date.fromisoformat(captured)).days
    except ValueError:
        return True
    return not (file.get('published') == stamp and posted and posted <= captured and ahead <= EARLY_POSTING_MAX_DAYS)


def _edition_stamp(row: dict) -> tuple:
    return (row.get('file_last_updated_on') or '', row.get('retrieved_at') or '')


def latest_editions(rows: list[dict]) -> list[dict]:
    """Keep only rows from the newest edition of each file series."""
    newest: dict[str, tuple] = {}
    for row in rows:
        stamp = _edition_stamp(row)
        series = row['file_series']
        if series not in newest or stamp > newest[series]:
            newest[series] = stamp
    return [r for r in rows if _edition_stamp(r) == newest[r['file_series']]]


def current_rates(rows: list[dict], as_of: date, held_files: frozenset | set = frozenset()) -> tuple[list[dict], list[dict]]:
    """Return (current rate records, excluded rows with reasons).

    Rows of `held_files` (see publication_date_conflict) are excluded as
    publication_date_conflict; held_rates() keeps them as labeled records.
    Rows of an older edition of a file series are listed as superseded_edition,
    so no extracted price leaves the run without a stated reason.
    """
    excluded, grouped = [], defaultdict(list)
    latest = latest_editions(rows)
    if len(latest) != len(rows):
        kept = {id(row) for row in latest}
        excluded.extend({**row, 'exclusion_reason': 'superseded_edition'} for row in rows if id(row) not in kept)
    for row in latest:
        reason = exclusion_reason(row, as_of, held_files)
        if reason:
            excluded.append({**row, 'exclusion_reason': reason})
            continue
        grouped[rate_key(row)].append(row)
    return _records(grouped, as_of), excluded


def held_rates(excluded: list[dict], as_of: date) -> list[dict]:
    """Records for rates whose files carry a publication date conflict: dated evidence, never current."""
    grouped = defaultdict(list)
    for row in excluded:
        if row['exclusion_reason'] == 'publication_date_conflict':
            clean = {key: value for key, value in row.items() if key != 'exclusion_reason'}
            grouped[rate_key(clean)].append(clean)
    return _records(grouped, as_of)


def _records(grouped: dict, as_of: date) -> list[dict]:
    records = []
    subject = config.subject()['name']
    merged_plans = {}  # records built from the same plan lists share one merged list
    for key, group in grouped.items():
        first = group[0]
        own = first['provider_role'] != 'market'
        kind = value_kind(first)
        amount = Decimal(first['negotiated_rate'])
        dollars = kind == 'dollars_per_unit' and not first.get('wildcard_scope')
        per_unit = dollars and first['billing_code'] in FIFTEEN_MINUTE_CODES
        lists = {id(row.get('plans') or ()): row.get('plans') or () for row in group}  # rows of one file share its list
        plan_key = tuple(sorted(lists))
        if plan_key not in merged_plans:
            plans = {}
            for listed in lists.values():
                for plan in listed:
                    plans[(plan.get('plan_id_type'), plan.get('plan_id'), plan.get('plan_name'))] = plan
            merged_plans[plan_key] = sorted(plans.values(), key=lambda p: (str(p.get('plan_name')), str(p.get('plan_id'))))
        record_plans = merged_plans[plan_key]
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
            # Tracked codes a bundle (bundled_codes) or capitation (covered_services) contains; the
            # amount is the whole arrangement's, never a price of one contained code.
            'bundled_tracked_codes': sorted({c for r in group for c in r.get('bundled_tracked_codes') or []}),
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
            'plans': record_plans,
            'plan_count': len(record_plans),
            'source_files': sorted(sources.values(), key=lambda s: s['url']),
            'source_file_count': len(sources),
            'latest_file_last_updated_on': max((s['last_updated_on'] or '') for s in sources.values()) or None,
            'observation_count': len(group),
        })
    _flag_unresolved_ladders(records)
    records.sort(key=lambda r: (r['payer_id'], r['provider_role'], r['entity_id'], r['billing_code'], r['billing_code_modifier'], r['value_kind'], Decimal(r['negotiated_rate'])))
    return records


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
