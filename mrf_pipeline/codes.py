"""ABA billing codes tracked by the pipeline and what their unit means.

Transparency in Coverage files carry no unit of measure. The ten CPT codes and
H2019 are defined per 15 minutes by their descriptors, so a published amount
is read as "per 15-minute unit"; any hourly figure is a labeled x4 conversion,
never a source value. H0031, H0032 and T1023 have no time unit, so their
amounts are per service and get no hourly figure.
"""
from __future__ import annotations

from decimal import Decimal

TRACKED_CODES: dict[str, str] = {
    '97151': 'Behavior identification assessment by QHP, each 15 minutes',
    '97152': 'Behavior identification supporting assessment by technician, each 15 minutes',
    '97153': 'Adaptive behavior treatment by protocol, technician, each 15 minutes',
    '97154': 'Group adaptive behavior treatment by protocol, technician, each 15 minutes',
    '97155': 'Adaptive behavior treatment with protocol modification by QHP, each 15 minutes',
    '97156': 'Family adaptive behavior treatment guidance by QHP, each 15 minutes',
    '97157': 'Multiple-family group adaptive behavior treatment guidance by QHP, each 15 minutes',
    '97158': 'Group adaptive behavior treatment with protocol modification by QHP, each 15 minutes',
    '0362T': 'Behavior identification supporting assessment, each 15 minutes of technician time',
    '0373T': 'Adaptive behavior treatment with protocol modification, each 15 minutes of technician time',
    # HCPCS codes some payers (notably Medicaid-style schedules) use for ABA; already tracked by the app.
    'H0031': 'Mental health assessment, by non-physician (no time unit in the descriptor)',
    'H0032': 'Mental health service plan development by non-physician (no time unit in the descriptor)',
    'H2019': 'Therapeutic behavioral services, per 15 minutes',
    'T1023': 'Screening to determine appropriateness for a specified program, per encounter',
}
# Codes whose descriptor defines a 15-minute unit; the rest have no time unit.
FIFTEEN_MINUTE_CODES = frozenset(c for c, d in TRACKED_CODES.items() if '15 minutes' in d)

# The custom TiC code meaning "every billing code of this billing_code_type".
WILDCARD_CODE = 'CSTM-00'
WILDCARD_TYPE = 'CSTM-ALL'
# Code types a tracked ABA code can appear under, directly or via a wildcard.
ABA_CODE_TYPES = frozenset({'CPT', 'HCPCS'})

UNIT_BASIS = '15_min_cpt_unit'
UNIT_LABEL = 'per 15-minute unit (from the code descriptor; the file states no unit)'
UNITS_PER_HOUR = 4
PER_SERVICE_LABEL = 'per service as the payer defines it (the code descriptor has no time unit; the file states no unit)'


def hourly_equivalent(per_unit: Decimal | None) -> Decimal | None:
    """Labeled conversion for display; callers must keep UNIT_LABEL beside it."""
    return None if per_unit is None else per_unit * UNITS_PER_HOUR


def wildcard_scope(billing_code_type: str | None, billing_code: str | None) -> str | None:
    """Return the scope a wildcard row covers, or None for a specific code."""
    code_type = (billing_code_type or '').strip().upper()
    code = (billing_code or '').strip().upper()
    if code_type == WILDCARD_TYPE:
        return 'all_code_types'
    if code == WILDCARD_CODE and code_type in ABA_CODE_TYPES:
        return f'all_{code_type.lower()}_codes'
    return None
