"""Decide whether a TiC provider group is the subject, a market organization, or neither.

The subject (the organization whose own contracted rates a run tracks) is
matched only on an exact organization identifier: one of its EINs in
``tin.value`` (``tin.type`` ein), an organization NPI in the ``npi`` array, or
an organization NPI placed in ``tin.value`` with ``tin.type`` npi. An
individual clinician NPI never establishes the subject on its own.

"Market" means a group that lists an organization NPI from the configured
roster. It is evidence of what another in-network organization is paid,
never the subject's contracted rate.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from . import config


def digits(value) -> str:
    return re.sub(r'\D', '', str(value if value is not None else ''))


def identifier(value, *, width: int | None = None) -> str:
    """Digits of an NPI or EIN as the file states it; '' when the value cannot be one.

    The CMS schema types NPIs as JSON numbers, which ijson yields as int or
    Decimal. Some publishers serialize them as 1234567893.0 or 1.234567893E9;
    the identifier is the number's value, never the digits of its text
    (digits('1.234567893E+9') is a different NPI). A non-integral number is no
    identifier. ``width`` restores leading zeros of an EIN written as a number.
    """
    if value is None or isinstance(value, bool):
        return ''
    if isinstance(value, float):
        value = Decimal(repr(value))
    if isinstance(value, str) and re.fullmatch(r'\s*\d+\.0*\s*', value):
        value = Decimal(value.strip())
    if isinstance(value, Decimal):
        try:
            if not value.is_finite() or value != value.to_integral_value() or value < 0:
                return ''
            value = int(value)
        except InvalidOperation:
            return ''
    if isinstance(value, int):
        text = str(value) if value >= 0 else ''
        return text.zfill(width) if text and width else text
    return digits(value)


@dataclass(frozen=True)
class GroupMatch:
    role: str               # the configured subject role, or 'market'
    reason: str             # tin | org_npi | tin_npi | roster_npi
    entity_id: str          # subject entity id from the identities file, or roster NPI
    entity_name: str
    matched_npis: tuple[str, ...]


@dataclass
class Matcher:
    subject_tins: dict[str, tuple[str, str]] = field(default_factory=dict)      # ein digits -> (id, name)
    subject_org_npis: dict[str, tuple[str, str]] = field(default_factory=dict)  # npi -> (id, name)
    market_npis: dict[str, str] = field(default_factory=dict)                   # npi -> org name
    excluded_tins: frozenset[str] = frozenset()
    role: str = 'subject'

    @classmethod
    def from_repo(cls, *, include_market: bool = True) -> 'Matcher':
        identities = json.loads(config.path('identities').read_text())
        tins, npis = {}, {}
        for provider in identities['providers']:
            entry = (provider['id'], provider['name'])
            if provider.get('tin'):
                tins[digits(provider['tin'])] = entry
            if provider.get('npi'):
                npis[digits(provider['npi'])] = entry
        excluded = frozenset(digits(e['tin']) for e in identities.get('exclusions', []) if e.get('tin'))
        market = {}
        if include_market:
            roster = json.loads(config.path('roster').read_text())
            states = config.market_states()
            for provider in roster['providers']:
                npi = digits(provider.get('npi'))
                if (provider.get('provider_kind') == 'organization' and npi and npi not in npis
                        and provider.get('state') in states):
                    market[npi] = provider.get('name') or npi
        return cls(tins, npis, market, excluded, config.subject()['role'])

    def match(self, group: dict) -> GroupMatch | None:
        if not isinstance(group, dict):
            return None
        tin = group.get('tin') if isinstance(group.get('tin'), dict) else {}
        tin_type = str(tin.get('type') or '').strip().lower()
        tin_value = identifier(tin.get('value'), width=9 if tin_type == 'ein' else None)
        raw_npis = group.get('npi') if isinstance(group.get('npi'), list) else []
        npis = [identifier(n) for n in raw_npis]
        if tin_value and tin_value in self.excluded_tins:
            return None
        if tin_type == 'ein' and tin_value in self.subject_tins:
            entity_id, name = self.subject_tins[tin_value]
            return GroupMatch(self.role, 'tin', entity_id, name, tuple(n for n in npis if n in self.subject_org_npis))
        if tin_type == 'npi' and tin_value in self.subject_org_npis:
            entity_id, name = self.subject_org_npis[tin_value]
            return GroupMatch(self.role, 'tin_npi', entity_id, name, (tin_value,))
        org_hits = [n for n in npis if n in self.subject_org_npis]
        if org_hits:
            entity_id, name = self.subject_org_npis[org_hits[0]]
            return GroupMatch(self.role, 'org_npi', entity_id, name, tuple(org_hits))
        market_hits = [n for n in npis if n in self.market_npis]
        if tin_type == 'npi' and tin_value in self.market_npis:
            market_hits.append(tin_value)
        if market_hits:
            first = market_hits[0]
            return GroupMatch('market', 'roster_npi', first, self.market_npis[first], tuple(sorted(set(market_hits))))
        return None
