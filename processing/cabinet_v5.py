#!/usr/bin/env python3
"""Released daily cabinet history -> election identities -> maximal periods.

The primary analysis NEVER reconstructs history. Sensitivities change one released
fact locally, using dated witnesses to retain every other officeholder. No history
builder, scraper, network request, or manuscript-prose writer is imported.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, timedelta
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'processing/Processing/data'
OUT = ROOT / 'generated/cabinet_v5'
PAPER = ROOT / 'processing/Processing/output/paper'
DECOMP = ROOT / 'processing/Processing/output/decomposition'
START, END = '2015-01-01', '2026-03-20'
COMMAND = 'JULIA_BIN=processing/julia_paper_runtime.sh processing/rebuild_manuscript.sh'


def read(path):
    with Path(path).open(newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def write(path, rows, fields=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields or list(rows[0]), lineterminator='\n')
        w.writeheader()
        w.writerows(rows)


def digest(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def tokens(value):
    return set(filter(None, (x.strip() for x in str(value).split(';'))))


def joined(values):
    return ';'.join(sorted(set(values)))


def truth(value):
    return str(value).lower() in ('true', 'yes', '1')


def dates(start, end):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [(s + timedelta(days=i)).isoformat() for i in range((e-s).days)]


def tomorrow(t):
    return (date.fromisoformat(t) + timedelta(days=1)).isoformat()


def election(t):
    assert START <= t < END
    return 2014 if t < '2019-01-01' else 2018 if t < '2023-01-01' else 2022


def validate_inputs():
    pin = json.loads((DATA / 'cabinet_release_pin.json').read_text())
    release = ROOT / pin['release_path']
    assert digest(release/'metadata.json') == pin['metadata_sha256'], 'Metadata pin mismatch'
    meta = json.loads((release/'metadata.json').read_text())
    assert meta['schema_version'] == pin['schema_version'] == 3
    assert meta['data_version'] == pin['data_version']
    assert meta['cutoff_exclusive'] == pin['cutoff_exclusive'] == END
    assert meta['primary_complete'] and meta['primary_covered_days'] == 4096
    assert meta['identified_days'] == 3996 and meta['provisional_days'] == 100
    for name, h in meta['file_hashes'].items():
        assert pin['file_hashes'][name] == h, name
    for name, h in pin['file_hashes'].items():
        assert digest(release/name) == h, f'Frozen release changed: {name}'
    assert digest(DATA/'cabinet_release_election_crosswalk.csv') == pin['crosswalk_sha256']
    cross = read(DATA/'cabinet_release_election_crosswalk.csv')
    mapping = defaultdict(set)
    seen = set()
    for r in cross:
        key = (int(r['election_year']), r['party_id'], r['election_party'])
        assert key not in seen and r['basis'] and r['mapping_type'] in {
            'explicit_identity', 'crosswalk_rename', 'crosswalk_fusion_expansion'}
        seen.add(key)
        mapping[key[:2]].add(key[2])
    return pin, release, mapping


def translate(historical, year, mapping):
    result = set()
    for p in historical:
        assert (year, p) in mapping and mapping[year,p], f'Unaudited historical identity: {year}/{p}'
        result.update(mapping[year, p])
    return joined(result)


def sensitivity_register(release):
    """Every recorded date/person uncertainty, including unbounded blockers."""
    result = []
    for r in read(release/'date_sensitivity.csv'):
        result.append(dict(sensitivity_id=r['uncertainty_id'], start=r['start_inclusive'],
                           end=r['end_exclusive'], kind=r['aggregate_effect']))
    for r in read(release/'residual_person_uncertainty.csv'):
        result.append(dict(sensitivity_id=r['uncertainty_id'], start=r['start_inclusive'],
                           end=r['end_exclusive'], kind=r['aggregate_effect']))
    return result


def compress(daily):
    """Only translated membership (and its election) defines a boundary."""
    groups = []
    for r in daily:
        key = (r['election_year'], r['election_party_set'])
        if groups and key == groups[-1][0] and tomorrow(groups[-1][1][-1]['date']) == r['date']:
            groups[-1][1].append(r)
        else:
            groups.append((key, [r]))
    result, annual = [], Counter()
    for i, (_, group) in enumerate(groups, 1):
        first, last = group[0], group[-1]
        annual[first['date'][:4]] += 1
        period = f"{first['date'][:4]}.{annual[first['date'][:4]]}"
        pid = f'CV5-{i:03d}'
        npro = sum(truth(r['provisional_day']) for r in group)
        status = 'established' if not npro else 'provisional' if npro == len(group) else 'mixed'
        for r in group:
            r['analytical_period_id'], r['period'] = pid, period
        union = lambda field: joined(x for r in group for x in tokens(r[field]))
        result.append(dict(analytical_period_id=pid, period=period,
            administration=joined(r['administration'] for r in group),
            election_year=first['election_year'], start_inclusive=first['date'],
            end_exclusive=tomorrow(last['date']), days=len(group),
            historical_party_sets=json.dumps(sorted({r['historical_party_set'] for r in group}), ensure_ascii=False),
            historical_party_ids=union('historical_party_ids'), election_party_set=first['election_party_set'],
            established_days=len(group)-npro, provisional_days=npro, proportion_provisional=npro/len(group),
            historical_status=status, provisional_person_ids=union('provisional_person_ids'),
            blocker_ids=union('provisional_person_ids'), primary_assumption_ids=union('primary_assumption_ids'),
            sensitivity_ids=union('sensitivity_ids'),
            source_period_ids=joined(r['historical_period_id'] for r in group),
            source_release=first['source_release']))
    return result


def prepare():
    pin, release, mapping = validate_inputs()
    # Read all released provenance inputs; evidence is not re-adjudicated.
    required = ['daily_coverage.csv','periods.csv','membership.csv','services.csv','affiliations.csv',
        'transitions.csv','witnesses.csv','primary_assumptions.csv','completion_blockers.csv',
        'residual_person_uncertainty.csv','date_sensitivity.csv','service_sensitivity_constraints.csv',
        'evidence.csv','decisions.csv','historical_comparison.csv']
    tables = {name: read(release/name) for name in required}
    source = tables['daily_coverage.csv']
    assert [r['date'] for r in source] == dates(START, END), 'Daily gap, duplicate or disorder'
    assert Counter(r['historical_status'] for r in source) == {'established':3996, 'unidentified':100}
    memberships = defaultdict(set)
    for r in tables['membership.csv']:
        memberships[r['period_id']].add(r['party_id'])
    periods = {r['period_id']: r for r in tables['periods.csv']}
    aff = {r['affiliation_id']: r for r in tables['affiliations.csv']}
    assumptions = {r['assumption_id']: r for r in tables['primary_assumptions.csv']}
    assert len({r['person_id'] for r in assumptions.values()}) == 9
    for r in assumptions.values():
        assert r['historical_state'] == aff[r['affiliation_id']]['state'] == 'UNKNOWN'
        assert r['assumed_state'] == 'UNAFFILIATED' and not r['assumed_party_id']
    sensitivities = sensitivity_register(release)
    daily = []
    audit_mappings = set()
    for r in source:
        t, y = r['date'], election(r['date'])
        hist = tokens(r['party_ids'])
        p = periods[r['period_id']]
        assert p['start_inclusive'] <= t < p['end_exclusive']
        assert hist == memberships[r['period_id']] == tokens(p['party_ids'])
        assert not r['unfilled_primary_dependencies']
        pro = r['composition_status'] == 'primary_provisional'
        assert pro == bool(r['primary_assumption_ids']) == bool(r['provisional_person_ids'])
        for aid in tokens(r['primary_assumption_ids']):
            assert assumptions[aid]['start_inclusive'] <= t < assumptions[aid]['end_exclusive']
        for h in hist:
            for target in mapping[y,h]:
                audit_mappings.add((y,h,target))
        daily.append(dict(date=t, administration=r['administration_id'],
            historical_party_set=r['party_labels'], historical_party_ids=joined(hist),
            election_year=y, election_party_set=translate(hist,y,mapping),
            historical_period_id=r['period_id'], source_release=pin['release_path'],
            primary_set_status=r['composition_status'], historical_status=r['historical_status'],
            provisional_day=pro, provisional_person_ids=r['provisional_person_ids'],
            primary_assumption_ids=r['primary_assumption_ids'],
            sensitivity_ids=joined(s['sensitivity_id'] for s in sensitivities if s['start'] <= t < s['end'])))
    analytical = compress(daily)
    assert sum(p['days'] for p in analytical) == 4096
    assert sum(p['provisional_days'] for p in analytical) == 100
    assert sum(p['established_days'] for p in analytical) == 3996
    assert all((a['election_year'],a['election_party_set']) != (b['election_year'],b['election_party_set'])
               for a,b in zip(analytical, analytical[1:]))
    write(OUT/'cabinet_analysis_daily.csv',daily)
    write(OUT/'cabinet_analysis_periods.csv',analytical)
    write(OUT/'election_mapping_validation.csv', [dict(election_year=y,historical_party_id=h,election_party=p)
        for y,h,p in sorted(audit_mappings)])
    write(OUT/'sensitivity_register.csv',sensitivities)
    linkage = []
    for p in analytical:
        for sid in tokens(p['source_period_ids']):
            h = periods[sid]
            linkage.append(dict(analytical_period_id=p['analytical_period_id'],period=p['period'],
                historical_period_id=sid, historical_start=h['start_inclusive'],historical_end_exclusive=h['end_exclusive'],
                overlap_start=max(p['start_inclusive'],h['start_inclusive']),
                overlap_end_exclusive=min(p['end_exclusive'],h['end_exclusive']),
                historical_party_ids=h['party_ids'],election_party_set=p['election_party_set']))
    write(OUT/'historical_analytical_linkage.csv',linkage)
    (OUT/'provenance.json').write_text(json.dumps(dict(source_release=pin['release_path'],
        source_metadata_sha256=pin['metadata_sha256'],crosswalk_sha256=pin['crosswalk_sha256'],
        source_file_hashes=pin['file_hashes'],build_command=COMMAND,
        transformation='daily historical set -> audited election-year mapping -> maximal recompression -> electoral quantities',
        days=4096,established_days=3996,provisional_days=100,historical_periods=len(periods),
        analytical_periods=len(analytical),read_inputs={k:len(v) for k,v in tables.items()}),indent=2)+'\n')
    print(f'V5 adapter: {len(periods)} historical periods -> {len(analytical)} analytical periods; 4096 days (3996 established, 100 provisional).')
    from cabinet_party_sets import prepare_registry
    prepare_registry(daily, analytical)
    return daily, analytical


class Quantities:
    """Independent exact rational party sums, checked against district sums."""
    def __init__(self):
        self.rows = {(int(r['election_year']),r['party']):r for r in read(DECOMP/'raw/party_accounting_all_years.csv')}
        self.cache = {}

    def __call__(self, year, members):
        key = (int(year), joined(tokens(members)))
        if key in self.cache:
            return dict(self.cache[key])
        rows = [self.rows[key[0],p] for p in tokens(key[1])]
        assert rows, 'Empty cabinet is not expected in this release'
        V = int(rows[0]['V'])
        assert {int(r['V']) for r in rows} == {V}
        v, s = sum(int(r['v_i']) for r in rows), sum(int(r['s_i']) for r in rows)
        exact = lambda field: sum((Fraction(r[field].replace('//','/')) for r in rows),Fraction())
        q = Fraction(513*v,V)
        d, A, B = s-q, exact('A_i_exact'), exact('B_i_exact')
        assert exact('q_i_exact') == q and exact('d_i_exact') == d and A+B == d
        R = Fraction(s,1)/q if q else None
        assert not q or R == 1+d/q
        result = dict(votes=v,national_vote_total=V,vote_share=v/V,seats=s,seat_share=s/513,
            q_C=float(q),d_C=float(d),R_C=float(R) if R is not None else "",A_C=float(A),B_C=float(B),
            inversion_status=2*v<V and s>=257)
        self.cache[key] = result
        return dict(result)


def concrete_sensitivities(release, mapping, daily, quantify):
    """Evaluate each finite alternative; never guess parties for UNKNOWN people.

    Primary unions come from daily_coverage. For a local changed fact, subtract
    only its party witnesses and add the released alternative. Independent
    witnesses mask a removal. Joint service-date shifts move both holders.
    """
    bydate = {r['date']:r for r in daily}
    witnesses = read(release/'witnesses.csv')
    services = read(release/'services.csv')
    affiliations = read(release/'affiliations.csv')
    datespec = {r['uncertainty_id']:r for r in read(release/'date_sensitivity.csv')}
    rows, scenario_periods = [], []

    def other_union(t, excluded_person=None, excluded_service=None):
        return {w['party_id'] for w in witnesses if w['start_inclusive'] <= t < w['end_exclusive']
                and w['person_id'] != excluded_person and w['service_id'] != excluded_service}

    def evaluate(sid, candidate, start, end, primary_state, alternative_state, affected, alter,
                 kind, unbounded=False):
        scenario = sid+'/'+candidate
        changed_daily = [dict(r) for r in daily]
        local = []
        for t in dates(start,end):
            base = bydate[t]
            hist = alter(t)
            members = translate(hist,int(base['election_year']),mapping)
            primary = quantify(base['election_year'],base['election_party_set'])
            alternative = quantify(base['election_year'],members)
            rec = dict(sensitivity_id=sid,scenario_id=scenario,scenario_type=kind,
                candidate=candidate,start_inclusive=t,end_exclusive=tomorrow(t),days=1,
                affected_historical_party=affected,primary_state=primary_state,alternative_state=alternative_state,
                primary_historical_party_set=base['historical_party_ids'],alternative_historical_party_set=joined(hist),
                primary_election_party_set=base['election_party_set'],alternative_election_party_set=members,
                primary_provisional_day=truth(base['provisional_day']),
                primary_provisional_person_ids=base['provisional_person_ids'],
                comparison_scope='conditional on V5 no-additional-party assumptions' if truth(base['provisional_day']) else 'concrete released alternative',
                unbounded_personal_uncertainty=unbounded,
                inversion_classification_changes=primary['inversion_status'] != alternative['inversion_status'])
            for prefix, values in [('primary',primary),('alternative',alternative)]:
                rec.update({prefix+'_'+k:v for k,v in values.items()})
            # Recompress local outcomes without merging distinct primary sets.
            compare = {k:v for k,v in rec.items() if k not in ('start_inclusive','end_exclusive','days')}
            if local and compare == {k:v for k,v in local[-1].items() if k not in ('start_inclusive','end_exclusive','days')}:
                local[-1]['end_exclusive'],local[-1]['days'] = tomorrow(t),local[-1]['days']+1
            else:
                local.append(rec)
            i = (date.fromisoformat(t)-date.fromisoformat(START)).days
            changed_daily[i]['election_party_set'] = members
            changed_daily[i]['historical_party_ids'] = joined(hist)
        rows.extend(local)
        # Whole-calendar scenario recompression, with exact quantities.
        for p in compress(changed_daily):
            scenario_periods.append(dict(scenario_id=scenario,sensitivity_id=sid,
                scenario_period_id=p['analytical_period_id'],election_year=p['election_year'],
                start_inclusive=p['start_inclusive'],end_exclusive=p['end_exclusive'],days=p['days'],
                election_party_set=p['election_party_set'],**quantify(p['election_year'],p['election_party_set'])))

    for c in read(release/'sensitivity_constraints.csv'):
        assert c['event_type'] == 'affiliation_change'
        sid = c['uncertainty_id']
        for candidate in sorted(tokens(c['candidate_dates'])):
            if candidate == c['baseline_effective_date']:
                continue
            def alter(t, c=c, candidate=candidate):
                hist = other_union(t,excluded_person=c['person_id'])
                side = 'before' if t < candidate else 'after'
                active = any(s['person_id']==c['person_id'] and truth(s['included']) and
                             s['start_inclusive']<=t<s['end_exclusive'] for s in services)
                if active and c[side+'_state']=='PARTY':
                    hist.add(c[side+'_party_id'])
                return hist
            evaluate(sid,candidate,c['earliest_effective_date'],c['latest_effective_date'],
                'transition on '+c['baseline_effective_date'],'transition on '+candidate,
                joined(filter(None,[c['before_party_id'],c['after_party_id']])),
                alter,'joint_affiliation_date')
    for c in read(release/'service_sensitivity_constraints.csv'):
        sid = c['uncertainty_id']
        outgoing = [s for s in services if s['office_id']==c['office_id'] and s['person_id']==c['outgoing_person_id']
                    and s['end_exclusive']==c['baseline_effective_date'] and truth(s['included'])]
        incoming = [s for s in services if s['office_id']==c['office_id'] and s['person_id']==c['incoming_person_id']
                    and s['start_inclusive']==c['baseline_effective_date'] and truth(s['included'])]
        assert len(outgoing)==len(incoming)==1, sid
        for candidate in sorted(tokens(c['candidate_dates'])):
            if candidate == c['baseline_effective_date']:
                continue
            def alter(t,c=c,candidate=candidate,outgoing=outgoing,incoming=incoming):
                excluded = {outgoing[0]['service_id'],incoming[0]['service_id']}
                hist = {w['party_id'] for w in witnesses if w['start_inclusive']<=t<w['end_exclusive'] and w['service_id'] not in excluded}
                side = 'outgoing' if t<candidate else 'incoming'
                if c[side+'_state']=='PARTY':
                    hist.add(c[side+'_party_id'])
                # UNKNOWN contributes no party ONLY under the already flagged V5
                # primary assumption; this never establishes historical unaffiliation.
                if c[side+'_state']=='UNKNOWN':
                    assert c[side+'_person_id'] in tokens(bydate[t]['provisional_person_ids'])
                return hist
            evaluate(sid,candidate,c['earliest_effective_date'],c['latest_effective_date'],
                'joint succession on '+c['baseline_effective_date'],'joint succession on '+candidate,
                datespec[sid]['affected_party_ids'],alter,'joint_service_date',
                unbounded='UNKNOWN' in (c['outgoing_state'],c['incoming_state']))
    for c in read(release/'residual_person_uncertainty.csv'):
        if c['alternative_scope']=='linked_date_window':
            continue  # Already evaluated as one jointly shifted event, never free daily choices.
        primary = c['primary_party_id'] if c['primary_state']=='PARTY' else c['primary_state']
        for alt in json.loads(c['alternatives']):
            if alt==primary:
                continue
            assert alt != 'UNKNOWN', 'Unbounded alternatives must not be fabricated'
            def alter(t,c=c,alt=alt):
                hist = other_union(t,excluded_person=c['person_id'])
                if alt!='UNAFFILIATED':
                    hist.add(alt)
                return hist
            evaluate(c['uncertainty_id'],alt,c['start_inclusive'],c['end_exclusive'],primary,alt,
                joined(filter(None,[c['primary_party_id'],alt if alt!='UNAFFILIATED' else ''])),alter,'finite_affiliation')
    rows.sort(key=lambda r:(r['sensitivity_id'],r['candidate'],r['start_inclusive']))
    scenario_periods.sort(key=lambda r:(r['scenario_id'],r['start_inclusive']))
    write(OUT/'cabinet_sensitivity_results.csv',rows)
    write(OUT/'cabinet_sensitivity_periods.csv',scenario_periods)
    return rows


def before_after(periods, quantify, release):
    rows = []
    daily = read(OUT/'cabinet_analysis_daily.csv')
    historical = read(release/'historical_comparison.csv')
    for basis in ('manuscript_reported','preintegration_generated'):
        old = read(DATA/'cabinet_v5_comparison_baseline'/f'{basis}.csv')
        # Include uncovered old days as explicit unavailable, rather than noninversion.
        intervals = list(old)
        covered = {t for o in old for t in dates(o['period_start'],tomorrow(o['period_end']))}
        for t in dates(START,END):
            if t not in covered:
                intervals.append(dict(period='UNIDENTIFIED',period_start=t,period_end=t,period_days=1,
                    election_year=election(t),parties='',historical_parties='',coalition_inversion=''))
        for o in intervals:
            for p in periods:
                start, end = max(o['period_start'],p['start_inclusive']), min(tomorrow(o['period_end']),p['end_exclusive'])
                if start>=end:
                    continue
                oldset = joined(o['parties'].split(', '))
                oq = quantify(o['election_year'],oldset) if oldset else {}
                added, removed = tokens(p['election_party_set'])-tokens(oldset), tokens(oldset)-tokens(p['election_party_set'])
                reasons = []
                if not oldset:
                    reasons.append('other: previously unavailable composition now has V5 primary set')
                elif added or removed:
                    reasons.append('historical membership correction')
                if (o['period_start'],tomorrow(o['period_end'])) != (p['start_inclusive'],p['end_exclusive']):
                    reasons.append('date correction')
                if len(tokens(p['source_period_ids']))>1:
                    reasons.append('historical organizational transition collapsed after translation')
                if any(truth(d['provisional_day']) for d in daily if start<=d['date']<end):
                    reasons.append('provisional V5 assumption')
                details=[h for h in historical if h['start_inclusive']<end and h['end_exclusive']>start]
                r=dict(comparison_baseline=basis,overlap_start=start,overlap_end_exclusive=end,
                    overlap_days=len(dates(start,end)),old_period_id=o['period'],new_period_id=p['analytical_period_id'],
                    old_start=o['period_start'],old_end_exclusive=tomorrow(o['period_end']),
                    new_start=p['start_inclusive'],new_end_exclusive=p['end_exclusive'],
                    old_election_party_set=oldset,new_election_party_set=p['election_party_set'],
                    membership_added=joined(added),membership_removed=joined(removed),
                    old_duration=o['period_days'],new_duration=p['days'],
                    reason_for_analytical_change='; '.join(reasons) or 'unchanged',
                    historical_change_dimensions=joined(h['change_dimensions'] for h in details),
                    historical_decision_ids=joined(x for h in details for x in tokens(h['explanation_decision_ids'])))
                for k in ('vote_share','seats','q_C','d_C','A_C','B_C','R_C','inversion_status'):
                    r['old_'+k],r['new_'+k]=oq.get(k,''),p[k]
                rows.append(r)
    # Aggregate contiguous unavailable old days within the same new analytical period.
    merged=[]
    for r in sorted(rows,key=lambda x:(x['comparison_baseline'],x['overlap_start'])):
        if merged and r['old_period_id']=='UNIDENTIFIED' and merged[-1]['old_period_id']=='UNIDENTIFIED' and r['new_period_id']==merged[-1]['new_period_id'] and r['comparison_baseline']==merged[-1]['comparison_baseline'] and merged[-1]['overlap_end_exclusive']==r['overlap_start'] and r['reason_for_analytical_change']==merged[-1]['reason_for_analytical_change']:
            m=merged[-1];m['old_end_exclusive']=r['old_end_exclusive'];m['overlap_end_exclusive']=r['overlap_end_exclusive'];m['old_duration']=int(m['old_duration'])+1;m['overlap_days']+=1
        else:
            merged.append(r)
    write(OUT/'cabinet_analysis_before_after.csv',merged)


def analyze():
    pin, release, mapping = validate_inputs()
    daily, periods = read(OUT/'cabinet_analysis_daily.csv'),read(OUT/'cabinet_analysis_periods.csv')
    quantify = Quantities()
    pipeline = {(int(r['election_year']),r['period']):r for r in read(PAPER/'raw/cabinet_coalition_metrics.csv')}
    for p in periods:
        q=quantify(p['election_year'],p['election_party_set']); p.update(q)
        source=pipeline[int(p['election_year']),p['period']]
        for a,b in [('votes','votes'),('seats','seats'),('vote_share','vote_share'),('q_C','quota'),('d_C','seat_diff'),('R_C','representation_ratio')]:
            assert abs(float(q[a])-float(source[b]))<1e-10,(p['analytical_period_id'],a)
        assert truth(source['coalition_inversion'])==q['inversion_status']
    byid={p['analytical_period_id']:p for p in periods}
    for d in daily:
        d.update(quantify(d['election_year'],d['election_party_set']))
        assert all(d[k]==byid[d['analytical_period_id']][k] for k in quantify(d['election_year'],d['election_party_set']))
    write(OUT/'cabinet_analysis_daily.csv',daily)
    sensitivities=concrete_sensitivities(release,mapping,daily,quantify)
    inversions=[]
    for p in periods:
        touched=[s for s in sensitivities if s['start_inclusive']<p['end_exclusive'] and s['end_exclusive']>p['start_inclusive']]
        p['concrete_sensitivity_ids']=joined(s['sensitivity_id'] for s in touched)
        p['robust_to_all_recorded_concrete_sensitivities']='no' if any(s['inversion_classification_changes'] for s in touched) else 'yes'
        if p['inversion_status']:
            inversions.append(dict(p,notes=('Primary set includes a flagged no-additional-party assumption; unknown affiliations are unbounded. ' if int(p['provisional_days']) else '')+
                ('Recorded concrete alternatives change inversion status on some dates.' if p['robust_to_all_recorded_concrete_sensitivities']=='no' else 'No recorded concrete alternative changes inversion status on these dates.')))
    write(OUT/'cabinet_analysis_periods.csv',periods)
    write(OUT/'cabinet_inversions.csv',inversions,list(inversions[0]) if inversions else list(periods[0])+['notes'])
    before_after(periods,quantify,release)
    write(OUT/'cabinet_timeline_source.csv',periods)
    # Compatibility sensitivity filenames are regenerated, never left from paper-v1.
    write(PAPER/'raw/cabinet_date_sensitivity.csv',sensitivities)
    summaries=[]
    for sid in sorted({s['scenario_id'] for s in sensitivities}):
        ss=[s for s in sensitivities if s['scenario_id']==sid]
        summaries.append(dict(scenario_id=sid,sensitivity_id=ss[0]['sensitivity_id'],
            affected_days=sum(s['days'] for s in ss),changed_classification_days=sum(s['days'] for s in ss if s['inversion_classification_changes']),
            primary_inversion_days=sum(s['days'] for s in ss if s['primary_inversion_status']),
            alternative_inversion_days=sum(s['days'] for s in ss if s['alternative_inversion_status'])))
    write(PAPER/'raw/cabinet_date_sensitivity_summary.csv',summaries)
    print(f'V5 analysis: {len(periods)} periods, {len(inversions)} primary inversions; {len(summaries)} concrete scenarios.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage',choices=['prepare','analyze'])
    args=parser.parse_args()
    prepare() if args.stage=='prepare' else analyze()
