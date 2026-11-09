"""Single-pass streaming extraction of tracked ABA rates from one in-network file.

Memory stays bounded by the tracked-code rate blocks and the set of provider
group IDs, never by file size: non-ABA ``in_network`` items are skipped at the
token level without being built. ``provider_references`` may appear before or
after ``in_network`` (JSON objects are unordered); rate blocks are resolved
once both are known. Unknown values stay null — a missing or invalid
``negotiated_rate`` is never written as zero.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable

import ijson
from ijson.common import ObjectBuilder

from .codes import ABA_CODE_TYPES, TRACKED_CODES, wildcard_scope
from .match import GroupMatch, Matcher, identifier

# Root fields of a single-plan file (CMS: required when no table of contents is published).
PLAN_FIELDS = ('plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'issuer_name', 'plan_sponsor_name')
# `schema_version` is the draft CMS 3.0 name for `version`; recorded so such a file is recognized.
HEADER_FIELDS = frozenset({
    'reporting_entity_name', 'reporting_entity_type', 'last_updated_on', 'version', 'schema_version', *PLAN_FIELDS,
})
KNOWN_NEGOTIATED_TYPES = frozenset({'negotiated', 'derived', 'fee schedule', 'percentage', 'per diem'})
# CMS: bundled_codes list a bundle's codes, covered_services a capitation's.
CONTAINED_CODE_ARRANGEMENTS = frozenset({'bundle', 'capitation'})
# The in-network layouts this extractor reads: CMS 1.x (inline groups, remote references) and 2.x.
SUPPORTED_SCHEMA_MAJORS = frozenset({1, 2})
_START = frozenset({'start_map', 'start_array'})
_END = frozenset({'end_map', 'end_array'})


class ExtractionError(ValueError):
    """The file could not be read to the end; nothing from it is complete."""


class _NoMatchStop(Exception):
    """All provider references were read before any rate and none matched."""


@dataclass
class Extraction:
    header: dict
    rows: list[dict]
    stats: dict
    problems: dict[str, int]
    complete: bool
    problem_examples: dict[str, list] = field(default_factory=dict)


def reference_id(value) -> str | None:
    """Raw-text key for provider_group_id; Anthem publishes non-integral IDs."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return str(int(value)) if value == value.to_integral_value() else format(value.normalize(), 'f')
    if isinstance(value, float):
        return reference_id(Decimal(repr(value)))
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            return reference_id(Decimal(text))
        except InvalidOperation:
            return text
    return None


def normalize_type(value) -> str | None:
    if value is None:
        return None
    return ' '.join(str(value).strip().lower().replace('_', ' ').split()) or None


def parse_rate(value) -> tuple[str | None, str | None]:
    """Return (exact decimal text, problem). Strings are accepted but flagged."""
    problem = None
    if isinstance(value, bool) or value is None:
        return None, 'missing_rate'
    if isinstance(value, str):
        problem = 'rate_was_string'
        try:
            value = Decimal(value.strip().replace(',', '').lstrip('$'))
        except InvalidOperation:
            return None, 'invalid_rate'
    elif isinstance(value, float):
        value = Decimal(repr(value))
    elif isinstance(value, int):
        value = Decimal(value)
    if not isinstance(value, Decimal) or not value.is_finite():
        return None, 'invalid_rate'
    if value <= 0:
        return format(value, 'f'), 'non_positive_rate'
    return format(value, 'f'), problem


def schema_major(header: dict) -> int | None:
    """Major schema version the file declares; None when it declares none (allowed in CMS 1.x)."""
    match = re.match(r'\s*[vV]?(\d+)', header.get('schema_version') or header.get('version') or '')
    return int(match.group(1)) if match else None


def file_plan(header: dict) -> dict | None:
    """The plan a single-plan file names in its own root fields, as text (None when it names none)."""
    plan = {key: header[key] for key in PLAN_FIELDS if header.get(key)}
    return plan or None


def _identifier_text(value) -> str | None:
    """Source text of a TIN value; a JSON number is written as its integer digits, never '84.0'."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip()
    return identifier(value) or str(value)


def _string_list(value, *, upper: bool = False) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out = {str(v).strip().upper() if upper else str(v).strip() for v in items if v is not None and str(v).strip()}
    return sorted(out)


class _Extractor:
    def __init__(self, matcher: Matcher, codes: Iterable[str], resolve_remote: Callable[[dict], dict] | None):
        self.matcher = matcher
        self.codes = frozenset(c.strip().upper() for c in codes)
        self.resolve_remote = resolve_remote
        self.header: dict = {}
        self.rows: list[dict] = []
        self.problems: dict[str, int] = {}
        self.examples: dict[str, list] = {}
        self.defined: set[str] = set()
        self.matched_refs: dict[str, tuple[list[str], list[tuple[dict, GroupMatch]], str]] = {}
        self.duplicate_ids: set[str] = set()
        self.conflicted_ids: set[str] = set()
        self.unresolved_remote: set[str] = set()
        self.referenced: set[str] = set()
        self.deferred: list[tuple[dict, int, dict]] = []
        self.unread_contained_items = 0
        self.refs_complete = False
        self.saw_in_network = False
        self.order = None
        self.stats = {'in_network_items': 0, 'tracked_items': 0, 'wildcard_items': 0,
                      'provider_references': 0, 'matched_provider_references': 0,
                      'tracked_rate_blocks': 0}

    def problem(self, kind: str, example=None, count: int = 1) -> None:
        self.problems[kind] = self.problems.get(kind, 0) + count
        if example is not None and len(self.examples.setdefault(kind, [])) < 5:
            self.examples[kind].append(example)

    # --- provider references -------------------------------------------------
    def register_reference(self, ref: dict) -> None:
        self.stats['provider_references'] += 1
        if not isinstance(ref, dict):
            self.problem('invalid_provider_reference')
            return
        rid = reference_id(ref.get('provider_group_id'))
        if rid is None:
            self.problem('invalid_provider_group_id', ref.get('provider_group_id'))
            return
        groups = ref.get('provider_groups')
        if ref.get('location') and not groups:
            if self.resolve_remote is None:
                self.unresolved_remote.add(rid)
                self.problem('unresolved_remote_reference', ref.get('location'))
            else:
                try:
                    groups = (self.resolve_remote(ref) or {}).get('provider_groups')
                except Exception as error:  # a failed fetch leaves the reference unknown
                    self.unresolved_remote.add(rid)
                    self.problem('unresolved_remote_reference', f"{ref.get('location')}: {error}")
                    groups = None
        groups = groups if isinstance(groups, list) else []
        networks = _string_list(ref.get('network_name'))
        matches = [(g, m) for g in groups if (m := self.matcher.match(g))]
        digest = hashlib.sha256(json.dumps(groups, sort_keys=True, default=str).encode()).hexdigest() if (matches or rid in self.matched_refs) else ''
        if rid in self.defined:
            self.duplicate_ids.add(rid)
            previous = self.matched_refs.get(rid)
            if (previous is None and matches) or (previous is not None and previous[2] != digest):
                # Two different definitions of one ID cannot be joined safely.
                self.conflicted_ids.add(rid)
                self.problem('conflicting_provider_reference_definitions', rid)
        self.defined.add(rid)
        if matches and rid not in self.matched_refs:
            self.matched_refs[rid] = (networks, matches, digest)
            self.stats['matched_provider_references'] += 1

    # --- in-network items ----------------------------------------------------
    def relevance(self, item: dict):
        code_type, code = item.get('billing_code_type'), item.get('billing_code')
        if code_type is None or code is None:
            return None
        if wildcard_scope(code_type, code):
            return 'wildcard'
        if str(code_type).strip().upper() in ABA_CODE_TYPES and str(code).strip().upper() in self.codes:
            return 'code'
        return False

    def bundled_tracked(self, item: dict) -> list[str]:
        found = set()
        for key in ('bundled_codes', 'covered_services'):
            for child in item.get(key) or []:
                if (isinstance(child, dict) and str(child.get('billing_code_type', '')).upper() in ABA_CODE_TYPES
                        and str(child.get('billing_code', '')).strip().upper() in self.codes):
                    found.add(str(child['billing_code']).strip().upper())
        return sorted(found)

    def finish_item(self, item: dict, blocks: list[dict], ordinal: int, *, prices_skipped: bool = False) -> None:
        self.stats['in_network_items'] += 1
        relevance = self.relevance(item)
        if relevance is None:
            self.problem('in_network_item_without_code', ordinal)
            return
        bundled = self.bundled_tracked(item)
        if relevance is False:
            if not bundled:
                return
            if prices_skipped:
                # The prices streamed past before the item showed it bundles or capitates a
                # tracked code (its arrangement came after them). Unread, so the file is incomplete.
                if normalize_type(item.get('negotiation_arrangement')) in CONTAINED_CODE_ARRANGEMENTS | {None}:
                    self.unread_contained_items += 1
                    self.problem('contained_code_prices_not_read', ordinal)
                else:  # fee-for-service: contained codes have no CMS meaning there
                    self.problem('contained_codes_outside_bundle_or_capitation', ordinal)
                return
            relevance = 'bundle_contains'
        if relevance == 'wildcard':
            self.stats['wildcard_items'] += 1
        self.stats['tracked_items'] += 1
        context = {
            'item_ordinal': ordinal,
            'billing_code_type': str(item.get('billing_code_type')).strip(),
            'billing_code_type_version': item.get('billing_code_type_version'),
            'billing_code': str(item.get('billing_code')).strip().upper(),
            'code_name': item.get('name'),
            'code_description': item.get('description'),
            'negotiation_arrangement': normalize_type(item.get('negotiation_arrangement')),
            'wildcard_scope': wildcard_scope(item.get('billing_code_type'), item.get('billing_code')),
            'bundled_tracked_codes': bundled,
        }
        for index, block in enumerate(blocks, 1):
            self.stats['tracked_rate_blocks'] += 1
            refs = block.get('provider_references') if isinstance(block, dict) else None
            for ref in refs if isinstance(refs, list) else []:
                rid = reference_id(ref)
                if rid is not None:
                    self.referenced.add(rid)
            if self.refs_complete:
                self.emit(context, index, block)
            else:
                self.deferred.append((context, index, block))

    def emit(self, context: dict, block_ordinal: int, block: dict) -> None:
        if not isinstance(block, dict):
            self.problem('invalid_rate_block')
            return
        prices = block.get('negotiated_prices')
        if not isinstance(prices, list) or not prices:
            self.problem('rate_block_without_prices')
            return
        targets = []
        for ref in block.get('provider_references') or []:
            rid = reference_id(ref)
            if rid in self.matched_refs:
                networks, matches, _ = self.matched_refs[rid]
                targets.extend((rid, networks, group, match) for group, match in matches)
        for group in block.get('provider_groups') or []:  # schema 1.x inline groups
            if (match := self.matcher.match(group)):
                targets.append((None, [], group, match))
        for rid, networks, group, match in targets:
            tin = group.get('tin') if isinstance(group.get('tin'), dict) else {}
            npis = group.get('npi') if isinstance(group.get('npi'), list) else []
            for price_ordinal, price in enumerate(prices, 1):
                if not isinstance(price, dict):
                    self.problem('invalid_price_object')
                    continue
                rate, rate_problem = parse_rate(price.get('negotiated_rate'))
                if rate_problem:
                    self.problem(rate_problem, price.get('negotiated_rate'))
                negotiated_type = normalize_type(price.get('negotiated_type'))
                if negotiated_type not in KNOWN_NEGOTIATED_TYPES:
                    self.problem('unknown_negotiated_type', price.get('negotiated_type'))
                self.rows.append({
                    **context,
                    'block_ordinal': block_ordinal,
                    'price_ordinal': price_ordinal,
                    'provider_group_id': rid,
                    'network_names': networks,
                    'tin_type': normalize_type(tin.get('type')),
                    'tin_value': _identifier_text(tin.get('value')),
                    'tin_business_name': tin.get('business_name'),
                    'npi_count': len(npis),
                    'matched_npis': list(match.matched_npis),
                    'provider_role': match.role,
                    'match_reason': match.reason,
                    'entity_id': match.entity_id,
                    'entity_name': match.entity_name,
                    'negotiated_type': negotiated_type,
                    'negotiated_type_raw': price.get('negotiated_type'),
                    'negotiated_rate': rate,
                    'rate_problem': rate_problem,
                    'expiration_date': str(price['expiration_date']).strip() if price.get('expiration_date') is not None else None,
                    'billing_class': normalize_type(price.get('billing_class')),
                    'setting': normalize_type(price.get('setting')),
                    'service_code': _string_list(price.get('service_code'), upper=True),
                    'billing_code_modifier': _string_list(price.get('billing_code_modifier'), upper=True),
                    'additional_information': price.get('additional_information'),
                    'identity_conflict': rid in self.conflicted_ids if rid else False,
                })

    def finish(self) -> Extraction:
        self.refs_complete = True
        for context, index, block in self.deferred:
            self.emit(context, index, block)
        self.deferred.clear()
        for row in self.rows:
            if row['provider_group_id'] in self.conflicted_ids:
                row['identity_conflict'] = True
        undefined = self.referenced - self.defined
        if undefined:
            self.problem('undefined_provider_references', sorted(undefined)[:5][0], count=len(undefined))
        unresolved_used = self.referenced & self.unresolved_remote
        major = schema_major(self.header)
        unsupported = 'schema_version' in self.header or (major is not None and major not in SUPPORTED_SCHEMA_MAJORS)
        if unsupported:
            # A later CMS layout renames fields this reader keys on (draft 3.0: tin.ein, billing_code_modifiers);
            # reading it as 2.x could report "no subject rates" from a file it did not understand.
            self.problem('unsupported_schema_version', self.header.get('schema_version') or self.header.get('version'))
        complete = (self.saw_in_network and not undefined and not unresolved_used
                    and not self.unread_contained_items and not unsupported)
        if not self.saw_in_network:
            self.problem('missing_in_network_array')
        self.stats.update({'subject_rows': sum(r['provider_role'] != 'market' for r in self.rows),
                           'market_rows': sum(r['provider_role'] == 'market' for r in self.rows),
                           'duplicate_provider_group_ids': len(self.duplicate_ids),
                           'key_order': self.order})
        return Extraction(self.header, self.rows, self.stats, self.problems, complete, self.examples)


def extract(stream, matcher: Matcher, *, codes: Iterable[str] = TRACKED_CODES,
            resolve_remote: Callable[[dict], dict] | None = None, buf_size: int = 1 << 20,
            stop_when_unmatched: bool = False) -> Extraction:
    """Extract tracked-code rates for matched provider groups from decompressed JSON bytes.

    With ``stop_when_unmatched``, a schema 2.x file whose complete
    provider_references array precedes in_network and matches nobody ends
    there: schema 2.x forbids inline provider groups, so no rate can match.
    """
    state = _Extractor(matcher, codes, resolve_remote)
    builder, target, depth, skip = None, None, 0, 0
    item, blocks, item_ordinal, prices_skipped = None, None, 0, False
    try:
        for prefix, event, value in ijson.parse(stream, buf_size=buf_size):
            if skip:
                if event in _START:
                    skip += 1
                elif event in _END:
                    skip -= 1
                continue
            if builder is not None:
                builder.event(event, value)
                if event in _START:
                    depth += 1
                elif event in _END:
                    depth -= 1
                    if not depth:
                        built, builder = builder.value, None
                        if target == 'rate':
                            blocks.append(built)
                        elif target == 'ref':
                            state.register_reference(built)
                        else:
                            item[target] = built
                continue
            if prefix == 'in_network.item.negotiated_rates.item':
                if event == 'start_map':
                    builder, target, depth = ObjectBuilder(), 'rate', 1
                    builder.event(event, value)
                continue
            if prefix == 'in_network.item.negotiated_rates':
                if event == 'start_array':
                    relevance = state.relevance(item)
                    if (relevance is False
                            and normalize_type(item.get('negotiation_arrangement')) not in CONTAINED_CODE_ARRANGEMENTS):
                        # Token-level skip of a non-ABA item's prices. A bundle or capitation may
                        # contain a tracked code, so its prices are kept; when the arrangement comes
                        # after the prices, finish_item says the skipped prices were needed.
                        skip, prices_skipped = 1, True
                    elif relevance is None:
                        # The code follows its prices in this item; buffer until it is known.
                        state.stats['items_buffered_before_code'] = state.stats.get('items_buffered_before_code', 0) + 1
                continue
            if prefix.startswith('in_network.item.'):
                key = prefix[16:]
                if '.' not in key:
                    if event in _START:
                        builder, target, depth = ObjectBuilder(), key, 1
                        builder.event(event, value)
                    elif event != 'map_key':
                        item[key] = value
                continue
            if prefix == 'in_network.item':
                if event == 'start_map':
                    item, blocks, item_ordinal, prices_skipped = {}, [], item_ordinal + 1, False
                elif event == 'end_map':
                    state.finish_item(item, blocks, item_ordinal, prices_skipped=prices_skipped)
                    item, blocks = None, None
                continue
            if prefix == 'provider_references.item':
                if event == 'start_map':
                    builder, target, depth = ObjectBuilder(), 'ref', 1
                    builder.event(event, value)
                continue
            if prefix == 'provider_references':
                if event == 'start_array':
                    state.order = state.order or 'provider_references_first'
                elif event == 'end_array':
                    state.refs_complete = True
                    version = state.header.get('version', '')
                    if (stop_when_unmatched and state.order == 'provider_references_first' and not state.matched_refs
                            and version and not version.lstrip('vV').startswith('1')):
                        raise _NoMatchStop
                continue
            if prefix == 'in_network':
                if event == 'start_array':
                    state.saw_in_network = True
                    state.order = state.order or 'in_network_first'
                continue
            if prefix in HEADER_FIELDS and event in {'string', 'number'}:
                state.header[prefix] = str(value)
    except _NoMatchStop:
        state.saw_in_network = True
        result = state.finish()
        result.stats['early_stop'] = 'no_matching_provider_references'
        return result
    except (ijson.JSONError, ijson.IncompleteJSONError) as error:
        raise ExtractionError(f'Malformed or truncated JSON: {error}') from error
    if builder is not None or skip or item is not None:
        raise ExtractionError('JSON ended inside an open object')
    return state.finish()
