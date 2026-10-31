"""Synthetic checks for the scanner: matching, extraction and current-rate rollup.

Every identifier and amount here is invented; the real configuration is private.
"""
from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path

SUBJECT_EIN = '000000001'
SUBJECT_NPI = '1000000001'
MARKET_NPI = '1000000002'
CLINICIAN_NPI = '1000000003'

CONFIG = """
[subject]
role = "subject"
name = "Example Org"

[market]
states = ["ZZ"]

[paths]
payers = "payers.toml"
identities = "identities.json"
roster = "roster.json"
"""


def _configure() -> None:
    root = Path(tempfile.mkdtemp(prefix='scanner-test-config-'))
    (root / 'config.toml').write_text(CONFIG)
    (root / 'payers.toml').write_text('[[payer]]\nid = "example"\nname = "Example Payer"\ndiscovery = "direct"\n')
    (root / 'identities.json').write_text(json.dumps({'providers': [
        {'id': 'example_org', 'name': 'EXAMPLE ORG', 'tin': SUBJECT_EIN, 'npi': SUBJECT_NPI}]}))
    (root / 'roster.json').write_text(json.dumps({'providers': [
        {'npi': MARKET_NPI, 'name': 'OTHER ORG', 'provider_kind': 'organization', 'state': 'ZZ'}]}))
    os.environ['MRF_CONFIG_DIR'] = str(root)


_configure()

from mrf_pipeline.extract import extract  # noqa: E402
from mrf_pipeline.match import Matcher  # noqa: E402
from mrf_pipeline.rollup import current_rates  # noqa: E402


def document(rate_text: str = '25.00', modifiers=None, expiration='9999-12-31') -> io.BytesIO:
    price = {'negotiated_type': 'negotiated', 'negotiated_rate': '__RATE__', 'expiration_date': expiration,
             'service_code': ['11'], 'billing_class': 'professional'}
    if modifiers:
        price['billing_code_modifier'] = modifiers
    text = json.dumps({
        'reporting_entity_name': 'Example Payer', 'reporting_entity_type': 'Health Insurance Issuer',
        'last_updated_on': '2026-10-01', 'version': '2.0.0',
        'provider_references': [
            {'provider_group_id': 1, 'provider_groups': [{'npi': [int(CLINICIAN_NPI)], 'tin': {'type': 'ein', 'value': SUBJECT_EIN}}]},
            {'provider_group_id': 2, 'provider_groups': [{'npi': [int(MARKET_NPI)], 'tin': {'type': 'ein', 'value': '000000009'}}]},
            {'provider_group_id': 3, 'provider_groups': [{'npi': [int(CLINICIAN_NPI)], 'tin': {'type': 'ein', 'value': '000000008'}}]},
        ],
        'in_network': [
            {'negotiation_arrangement': 'ffs', 'name': 'Adaptive behavior treatment', 'billing_code_type': 'CPT',
             'billing_code_type_version': '2026', 'billing_code': '97153', 'description': 'each 15 minutes',
             'negotiated_rates': [{'provider_references': [1, 2, 3], 'negotiated_prices': [price]}]},
            {'negotiation_arrangement': 'ffs', 'name': 'Office visit', 'billing_code_type': 'CPT',
             'billing_code_type_version': '2026', 'billing_code': '99213', 'description': 'not tracked',
             'negotiated_rates': [{'provider_references': [1], 'negotiated_prices': [price]}]},
        ],
    })
    return io.BytesIO(text.replace('"__RATE__"', rate_text).encode())


class ScannerTests(unittest.TestCase):
    def setUp(self):
        self.matcher = Matcher.from_repo(include_market=True)

    def test_subject_matched_by_ein_market_by_roster_npi_clinician_alone_ignored(self):
        result = extract(document(), self.matcher)
        self.assertTrue(result.complete)
        roles = sorted(r['provider_role'] for r in result.rows)
        self.assertEqual(roles, ['market', 'subject'])
        self.assertEqual({r['billing_code'] for r in result.rows}, {'97153'})

    def test_exact_amount_text_and_unit_label_survive_rollup(self):
        rows = [{**r, 'payer_id': 'example', 'payer_name': 'Example Payer', 'source_file_url': 'https://example.test/a.json',
                 'file_series': 'a', 'file_last_updated_on': '2026-10-01', 'plans': []}
                for r in extract(document('25.00'), self.matcher).rows]
        current, excluded = current_rates(rows, date(2026, 10, 30))
        subject = [r for r in current if r['provider_role'] == 'subject']
        self.assertEqual([r['negotiated_rate'] for r in subject], ['25.00'])
        self.assertIn('15-minute', subject[0]['unit_label'])
        self.assertEqual(subject[0]['hourly_equivalent'], '100.00')
        self.assertIn('Example Org contracted rate', subject[0]['rate_owner'])
        self.assertEqual(excluded, [])

    def test_modifier_is_part_of_the_rate_key_and_expired_rates_are_excluded(self):
        base = {'payer_id': 'example', 'payer_name': 'Example Payer', 'source_file_url': 'https://example.test/a.json',
                'file_series': 'a', 'file_last_updated_on': '2026-10-01', 'plans': []}
        rows = [{**r, **base} for r in extract(document('25.00'), self.matcher).rows]
        rows += [{**r, **base} for r in extract(document('30.00', modifiers=['HN']), self.matcher).rows]
        rows += [{**r, **base} for r in extract(document('40.00', expiration='2026-01-31'), self.matcher).rows]
        current, excluded = current_rates(rows, date(2026, 10, 30))
        subject = sorted((tuple(r['billing_code_modifier']), r['negotiated_rate']) for r in current if r['provider_role'] == 'subject')
        self.assertEqual(subject, [((), '25.00'), (('HN',), '30.00')])
        self.assertEqual({r['exclusion_reason'] for r in excluded}, {'expired'})

    def test_missing_rate_is_never_zero(self):
        rows = extract(document('null'), self.matcher).rows
        self.assertTrue(rows)
        self.assertTrue(all(r['negotiated_rate'] is None for r in rows))


if __name__ == '__main__':
    unittest.main()
