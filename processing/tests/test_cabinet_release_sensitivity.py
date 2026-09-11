"""Release-consumer regressions: mapped mergers, provenance, bounded alternatives."""
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import cabinet_v5 as c

class CabinetV5Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pin,cls.release,cls.mapping=c.validate_inputs()

    def test_merger_maps_before_compression(self):
        before=c.translate({'DEM','PSL','PP'},2018,self.mapping)
        after=c.translate({'UNIAO','PP'},2018,self.mapping)
        self.assertEqual(before,after)
        fixture=[]
        for t,ids,display in [('2022-02-07','DEM;PSL;PP','DEM;PSL;PP'),('2022-02-08','UNIAO;PP','PP;UNIÃO')]:
            fixture.append(dict(date=t,administration='bolsonaro',election_year=2018,election_party_set=before,
                provisional_day=t.endswith('08'),historical_party_set=display,historical_party_ids=ids,
                provisional_person_ids='test-person' if t.endswith('08') else '',primary_assumption_ids='',
                sensitivity_ids='',historical_period_id='h-'+t,source_release='fixture'))
        periods=c.compress(fixture)
        self.assertEqual(len(periods),1)
        self.assertEqual(periods[0]['days'],2)
        self.assertEqual(periods[0]['historical_status'],'mixed')
        self.assertEqual(periods[0]['provisional_days'],1)
        self.assertEqual(len(c.tokens(periods[0]['source_period_ids'])),2)

    def test_unaudited_identity_fails(self):
        with self.assertRaises(AssertionError):c.translate({'INVENTED'},2018,self.mapping)

    def test_provisional_is_not_historical_unaffiliated(self):
        affiliations={r['affiliation_id']:r for r in c.read(self.release/'affiliations.csv')}
        assumptions=c.read(self.release/'primary_assumptions.csv')
        self.assertEqual(len({r['person_id'] for r in assumptions}),9)
        for r in assumptions:
            self.assertEqual(affiliations[r['affiliation_id']]['state'],'UNKNOWN')
            self.assertEqual(r['assumed_state'],'UNAFFILIATED')
            self.assertFalse(r['assumed_party_id'])

    def test_date_candidates_are_the_released_bounds(self):
        for name in ('sensitivity_constraints.csv','service_sensitivity_constraints.csv'):
            for r in c.read(self.release/name):
                self.assertEqual(sorted(c.tokens(r['candidate_dates'])),c.dates(r['earliest_effective_date'],c.tomorrow(r['latest_effective_date'])))
        residual=c.read(self.release/'residual_person_uncertainty.csv')
        self.assertFalse(any('Sachsida' in r['person_name'] and r['alternatives']!='[]' for r in residual))

    def test_all_released_concrete_records_evaluated(self):
        results=c.read(c.OUT/'cabinet_sensitivity_results.csv')
        present={r['sensitivity_id'] for r in results}
        for name in ('sensitivity_constraints.csv','service_sensitivity_constraints.csv'):
            for r in c.read(self.release/name):self.assertIn(r['uncertainty_id'],present)
        for r in c.read(self.release/'residual_person_uncertainty.csv'):
            if r['alternatives']!='[]' and r['alternative_scope']!='linked_date_window':self.assertIn(r['uncertainty_id'],present)
        for r in results:
            for prefix in ('primary','alternative'):
                expected=float(r[prefix+'_vote_share'])<.5 and int(r[prefix+'_seats'])>=257
                self.assertEqual(c.truth(r[prefix+'_inversion_status']),expected)

if __name__=='__main__':unittest.main()
