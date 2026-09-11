"""Independent dual-universe regressions for exact accounting and plotted points."""
import csv
import gzip
import sys
import unittest
from collections import defaultdict
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cross_domain_components import build_cross_domain_components
PAPER = ROOT / 'processing/Processing/output/paper'


class IdeologicalAccountingTests(unittest.TestCase):
    def test_primary_and_robustness_points(self):
        expected = {
            'seat_winning': {'k=0': (20, 7), 'k=1': (234, 100)},
            'all_parties': {'k=0': (20, 6), 'k=1': (259, 84)},
        }
        for universe in expected:
            frame = build_cross_domain_components(PAPER, domains=('cabinet', 'k=0', 'k=1'), universe=universe)
            with (PAPER / 'raw/cabinet_coalition_metrics.csv').open(newline='', encoding='utf-8') as handle:
                cabinet = list(csv.DictReader(handle))
            expected_cabinet = (len(cabinet), sum(2 * int(r['votes']) < int(r['national_vote_total']) and
                                                int(r['seats']) >= 257 for r in cabinet))
            self.assertEqual((len(frame[frame.domain == 'cabinet']),
                              int(frame.loc[frame.domain == 'cabinet', 'inversion'].sum())), expected_cabinet)
            for domain, counts in expected[universe].items():
                subset = frame[frame.domain == domain]
                self.assertEqual((len(subset), int(subset.inversion.sum())), counts)
                self.assertEqual(set(subset.ideological_universe), {universe})
            self.assertTrue((abs(frame.A_C + frame.B_C - frame.d_C) < 1e-10).all())
        primary = build_cross_domain_components(PAPER)
        inversions = primary[(primary.domain == 'k=0') & primary.inversion]
        self.assertTrue((inversions.A_C > 0).all())
        self.assertEqual(int(((inversions.election == 2018)).sum()), 1)

    def test_every_coalition_and_party_closes_exactly(self):
        coalitions = {}
        with (PAPER / 'raw/ideology_k_gap_accounting_both_universes.csv').open(newline='', encoding='utf-8') as handle:
            for row in csv.DictReader(handle):
                key = row['election'], row['ideological_universe'], row['k'], row['coalition_id']
                self.assertNotIn(key, coalitions)
                q, d, a, b = (Fraction(row[c + '_exact']) for c in ('q_C', 'd_C', 'A_C', 'B_C'))
                self.assertEqual(q, Fraction(513 * int(row['votes']), int(row['V'])))
                self.assertEqual(d, int(row['seats']) - q)
                self.assertEqual(a + b, d)
                if row['inversion'] == 'true':
                    self.assertLess(2 * int(row['votes']), int(row['V']))
                    self.assertGreaterEqual(int(row['seats']), 257)
                coalitions[key] = (d, int(row['votes']), int(row['seats']), int(row['party_count']))
        sums = defaultdict(lambda: [Fraction(0), 0, 0, 0])
        with gzip.open(PAPER / 'raw/ideology_k_gap_party_contributions_both_universes.csv.gz', 'rt', newline='', encoding='utf-8') as handle:
            for row in csv.DictReader(handle):
                key = row['election'], row['ideological_universe'], row['k'], row['coalition_id']
                if row['ideological_universe'] == 'seat_winning':
                    self.assertGreater(int(row['seats']), 0)
                total = sums[key]
                total[0] += Fraction(row['d_i_exact'])
                total[1] += int(row['votes'])
                total[2] += int(row['seats'])
                total[3] += 1
        self.assertEqual(set(sums), set(coalitions))
        for key, total in sums.items():
            self.assertEqual(tuple(total), coalitions[key])


if __name__ == '__main__':
    unittest.main()
