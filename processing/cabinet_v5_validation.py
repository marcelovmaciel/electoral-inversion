#!/usr/bin/env python3
"""Fail-loud, independent serialized-output checks for the V5 integration."""
import argparse
import gzip
import hashlib
import io
import json
from collections import defaultdict
from fractions import Fraction
from cabinet_v5 import *

REFERENCE=DATA/'cabinet_v5_comparison_baseline/noncabinet_signatures.json'
BEFORE=ROOT/'audit/cabinet_v5_before'
CABINET_FIELDS={'ever_in_cabinet','cabinet_observation_count','cabinet_source_period_count','cabinet_days',
    'identified_cabinet_days','calendar_cabinet_days','unidentified_cabinet_days','cabinet_participation_status',
    'established_cabinet_days','provisional_cabinet_days','primary_covered_cabinet_days'}
ORDINALS={'focal_order','registry_order','case_order'}


def csvrows(path):
    if str(path).endswith('.gz'):
        with gzip.open(path,'rt',newline='') as f:return list(csv.DictReader(f))
    return read(path)


def ideological(r):
    return r.get('domain','').startswith('k=') or r.get('domain') in ('ideological','ideology') or r.get('case_domain')=='ideological'


def selected(path, mode):
    rows=csvrows(path)
    if mode=='ideological_slice':rows=[r for r in rows if ideological(r)]
    if mode=='ideological_summaries':rows=[r for r in rows if 'cabinet' not in r.get('summary','') and 'observed' not in r.get('summary','')]
    exclude=ORDINALS | (CABINET_FIELDS if mode=='party_accounts' else set())
    return sorted(json.dumps({k:v for k,v in r.items() if k not in exclude},sort_keys=True,ensure_ascii=False) for r in rows)


def signature(rows):
    return hashlib.sha256('\n'.join(rows).encode()).hexdigest()


def freeze():
    assert not REFERENCE.exists(), 'Never overwrite the preintegration reference'
    records=[]
    independent={'party_accounting_all_years.csv','party_district_accounting_all_years.csv',
        'district_accounting_all_years.csv','accounting_district_electoral_weight.csv',
        'party_size_correlations.csv','party_size_groups.csv','party_fragmentation_summary.csv',
        'table_accounting_minimal_ideological.csv'}
    for base in (BEFORE/'processing/Processing/output/paper',BEFORE/'processing/Processing/output/decomposition'):
        for p in sorted(base.rglob('*')):
            if not p.is_file() or not (p.suffix=='.csv' or p.name.endswith('.csv.gz')):continue
            if '/audit/' in str(p.relative_to(base)) or '/diagnostics/' in str(p.relative_to(base)):continue
            mode=None
            if p.name=='party_accounting_all_years.csv':mode='party_accounts'
            elif p.name in independent or any(s in p.name for s in ('ideolog','party_seat_differentials','party_representation','party_vote_share')) and 'cabinet' not in p.name:mode='full_csv'
            elif p.name=='prose_analysis_summaries.csv':mode='ideological_summaries'
            else:
                rr=csvrows(p)
                if any(ideological(r) for r in rr):mode='ideological_slice'
            if mode:
                rows=selected(p,mode)
                records.append(dict(path=str(p.relative_to(BEFORE)),mode=mode,rows=len(rows),signature=signature(rows),before_sha256=digest(p)))
    REFERENCE.write_text(json.dumps(records,indent=2)+'\n')
    print('Froze',len(records),'non-cabinet signatures, including mixed-output ideological rows.')


def check_invariance():
    checks=[]
    for ref in json.loads(REFERENCE.read_text()):
        path=ROOT/ref['path'];rows=selected(path,ref['mode']) if path.exists() else []
        passed=len(rows)==ref['rows'] and signature(rows)==ref['signature']
        checks.append(dict(**ref,after_sha256=digest(path) if path.exists() else '',after_signature=signature(rows),passed=passed))
    write(OUT/'noncabinet_invariance.csv',checks)
    failures=[r['path'] for r in checks if not r['passed']]
    assert not failures,'STOP: unexplained non-cabinet numerical change: '+str(failures)
    print('Non-cabinet invariance:',len(checks),'protected numerical products/slices unchanged.')


def quantitative():
    pin,release,mapping=validate_inputs()
    source=read(release/'daily_coverage.csv')
    daily=read(OUT/'cabinet_analysis_daily.csv');periods=read(OUT/'cabinet_analysis_periods.csv')
    assert [r['date'] for r in daily]==dates(START,END)
    assert sum(truth(r['provisional_day']) for r in daily)==100
    assert sum(int(r['established_days']) for r in periods)==3996
    byperiod=defaultdict(list)
    for d,h in zip(daily,source):
        assert int(d['election_year'])==election(d['date'])
        assert d['election_party_set']==translate(tokens(h['party_ids']),election(d['date']),mapping)
        assert truth(d['provisional_day'])==(h['composition_status']=='primary_provisional')
        assert d['provisional_person_ids']==h['provisional_person_ids']
        byperiod[d['analytical_period_id']].append(d)
    party=read(DECOMP/'raw/party_accounting_all_years.csv')
    district=read(DECOMP/'raw/party_district_accounting_all_years.csv')
    checks=[]
    for i,p in enumerate(periods):
        dd=byperiod[p['analytical_period_id']]
        assert len(dd)==int(p['days'])==int(p['established_days'])+int(p['provisional_days'])
        assert [d['date'] for d in dd]==dates(p['start_inclusive'],p['end_exclusive'])
        if i:
            prev=periods[i-1]
            assert prev['end_exclusive']==p['start_inclusive']
            assert (prev['election_year'],prev['election_party_set'])!=(p['election_year'],p['election_party_set'])
        members=tokens(p['election_party_set']);y=p['election_year']
        rr=[r for r in party if r['election_year']==y and r['party'] in members]
        assert len(rr)==len(members)
        v=sum(int(r['v_i']) for r in rr);s=sum(int(r['s_i']) for r in rr);V=int(rr[0]['V'])
        q=Fraction(513*v,V);d=Fraction(s)-q
        A=B=Fraction()
        for unit in {r['electoral_unit'] for r in district if r['election_year']==y}:
            cells=[r for r in district if r['election_year']==y and r['electoral_unit']==unit and r['party'] in members]
            vd=sum(int(r['v_id']) for r in cells);sd=sum(int(r['s_id']) for r in cells)
            localquota=Fraction(int(cells[0]['S_d'])*vd,int(cells[0]['V_d']))
            A+=sd-localquota;B+=localquota-Fraction(513*vd,V)
        assert A+B==d
        assert A==sum(Fraction(r['A_i_exact'].replace('//','/')) for r in rr)
        assert B==sum(Fraction(r['B_i_exact'].replace('//','/')) for r in rr)
        expected=dict(votes=v,seats=s,vote_share=v/V,seat_share=s/513,q_C=q,d_C=d,R_C=Fraction(s)/q,A_C=A,B_C=B)
        residual=max(abs(float(p[k])-float(value)) for k,value in expected.items())
        assert residual<1e-10
        assert truth(p['inversion_status'])==(2*v<V and s>=257)
        for day in dd:
            assert all(day[k]==p[k] for k in expected)
            assert truth(day['inversion_status'])==truth(p['inversion_status'])
        checks.append(dict(analytical_period_id=p['analytical_period_id'],days=len(dd),
            max_absolute_residual=residual,exact_party_district_closure=True,daily_values_constant=True,passed=True))
    # Every scenario is a complete recompressed calendar; no hypothetical UNKNOWN party is generated.
    scenarios=defaultdict(list)
    for s in read(OUT/'cabinet_sensitivity_periods.csv'):scenarios[s['scenario_id']].append(s)
    for sid,rr in scenarios.items():
        assert sum(int(r['days']) for r in rr)==4096
        for a,b in zip(rr,rr[1:]):
            assert a['end_exclusive']==b['start_inclusive']
            assert (a['election_year'],a['election_party_set'])!=(b['election_year'],b['election_party_set'])
    comparison=read(OUT/'cabinet_analysis_before_after.csv')
    for basis in ('manuscript_reported','preintegration_generated'):
        rr=[r for r in comparison if r['comparison_baseline']==basis]
        assert sum(int(r['overlap_days']) for r in rr)==4096, ('comparison coverage',basis)
        covered=[t for r in rr for t in dates(r['overlap_start'],r['overlap_end_exclusive'])]
        assert sorted(covered)==dates(START,END), ('comparison daily overlap',basis)
    write(OUT/'quantitative_validation.csv',checks)
    print('Independent rational party/district validation:',len(checks),'periods;',len(scenarios),'complete sensitivity calendars.')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('stage',choices=['freeze','check','quantitative']);args=parser.parse_args()
    {'freeze':freeze,'check':check_invariance,'quantitative':quantitative}[args.stage]()
