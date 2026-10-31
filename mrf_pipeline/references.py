"""Check current subject rates against known-good amounts from contracts or remittances.

The references file holds reviewed reference amounts (never claim lines or PHI —
only payer, code, modifier, amount, unit, effective date and where the figure
came from). Every reference must be matched exactly by a current subject rate
for the same payer, code and modifiers; anything else is reported as a
mismatch or a missing rate to explain, never averaged away.
"""
from __future__ import annotations

import tomllib
from decimal import Decimal
from pathlib import Path

from . import config


def load(path: Path | None = None) -> list[dict]:
    path = path or config.path('references')
    return tomllib.loads(path.read_text()).get('reference', []) if path and path.exists() else []


def check(references: list[dict], current: list[dict]) -> list[dict]:
    results = []
    for ref in references:
        modifiers = sorted(m.upper() for m in ref.get('modifiers', []))
        candidates = [r for r in current if r['provider_role'] != 'market' and r['payer_id'] == ref['payer_id']
                      and r['billing_code'] == ref['code'] and sorted(r.get('billing_code_modifier') or []) == modifiers
                      and r['value_kind'] == 'dollars_per_unit']
        expected = Decimal(str(ref['amount']))
        amounts = sorted({Decimal(r['negotiated_rate']) for r in candidates})
        if not candidates:
            status, detail = 'missing', f"No current published {config.subject()['name']} dollar rate for this payer, code and modifier"
        elif expected in amounts:
            status, detail = 'match', None
        else:
            nearest = min(amounts, key=lambda a: abs(a - expected))
            status = 'mismatch'
            detail = f'Published {", ".join(map(str, amounts))}; nearest differs by {nearest - expected:+}'
        results.append({**ref, 'modifiers': modifiers, 'status': status, 'published_amounts': [str(a) for a in amounts],
                        'detail': detail, 'source_files': sorted({s['url'] for r in candidates for s in r.get('source_files') or []})})
    return results
