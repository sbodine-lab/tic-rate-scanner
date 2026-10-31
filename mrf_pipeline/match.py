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

from . import config


def digits(value) -> str:
    return re.sub(r'\D', '', str(value if value is not None else ''))


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
        tin_value = digits(tin.get('value'))
        raw_npis = group.get('npi') if isinstance(group.get('npi'), list) else []
        npis = [digits(n) for n in raw_npis]
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
