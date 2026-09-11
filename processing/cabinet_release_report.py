#!/usr/bin/env python3
"""Report the chronology implementation and frozen prose; never write manuscript text."""
from __future__ import annotations
import csv, hashlib, json, re, sys, subprocess
from datetime import date, timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REPORT=ROOT/'reports/cabinet_chronology_implementation'
PRE=REPORT/'preservation/files'
PAPER=ROOT/'processing/Processing/output/paper'
DECOMP=ROOT/'processing/Processing/output/decomposition'
MAIN=ROOT/'writing/submission_inversions_review/manuscript/main_rw_again.tex'
STANDALONE_FILES = ('party_components_all_years.csv', 'party_AB_by_year.csv',
    'inversion_party_components.csv', 'inversion_party_AB.csv', 'inversion_AB_summary.csv',
    'party_component_report.md', 'party_AB_report.md', 'party_AB_scatter.png', 'party_AB_scatter.pdf',
    'party_AB_validation.json')
sys.path.insert(0,str(ROOT/'processing/Processing/decomposition'))
from validate_prose_provenance import parse_blocks, same_value

def read(path):
    with Path(path).open(encoding='utf-8-sig',newline='') as f: return list(csv.DictReader(f))
def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(8*1024*1024),b''): h.update(block)
    return h.hexdigest()
def write_csv(path,rows,fields=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields or list(rows[0]),lineterminator='\n');w.writeheader();w.writerows(rows)
def cell(x): return str(x).replace('|','\\|').replace('\n',' ')
def relative(path): return str(path.relative_to(ROOT))
def truth(x): return str(x).lower()=='true'


def cabinet_artifact_details(path):
    """Identify the maintained producer, including cabinet fields in mixed files."""
    name = path.name.lower()
    rel = relative(path).lower()
    modules = 'processing/Processing/decomposition/'
    source = 'processing/Processing/output/paper/raw/cabinet_coalition_metrics.csv'
    if path.parent == ROOT and path.name in STANDALONE_FILES:
        dependent = name not in ('party_components_all_years.csv', 'party_ab_by_year.csv', 'party_ab_scatter.png', 'party_ab_scatter.pdf')
        return dependent, modules + 'party_AB_diagnostic.py', 'processing/Processing/output/decomposition/raw/party_district_accounting_all_years.csv' + ('; ' + source + '; processing/Processing/output/paper/raw/ideology_k_gap_coalitions_all_parties.csv' if dependent else '')
    if name in ('cabinet_calendar_status.csv', 'cabinet_unidentified_intervals.csv'):
        return True, 'processing/Processing/src/CabinetRelease.jl', 'cabinet_dataset/releases/' + PIN_VERSION + '/periods.csv; cabinet_dataset/releases/' + PIN_VERSION + '/membership.csv'
    if name in ('coalition_period_linkage.csv', 'party_mapping_coalitions.csv', 'preflight_checks.csv'):
        return True, 'processing/Processing/running/running.jl', 'cabinet_dataset/releases/' + PIN_VERSION + '/periods.csv; cabinet_dataset/releases/' + PIN_VERSION + '/membership.csv'
    if name.startswith('cabinet_inversion_'):
        return True, modules + 'IntermediateAccountingReport.jl', source + '; processing/Processing/output/decomposition/raw/inversion_case_registry.csv'
    if name == 'cabinet_coalition_metrics.csv':
        return True, 'processing/Processing/running/running.jl', 'cabinet_dataset/releases/' + PIN_VERSION + '/membership.csv; processing/Processing/output/paper/raw/party_seat_differentials_all_years.csv'
    if name == 'main_rw_again.pdf' or name == 'intermediate_accounting_report.pdf':
        return True, 'latexmk', source
    if 'cross_domain' in name:
        producer = 'writing/make_cross_domain_components.py' if path.suffix == '.pdf' else modules + 'cross_domain_components.py'
        return True, producer, source + '; processing/Processing/output/decomposition/raw/party_accounting_all_years.csv; processing/Processing/output/paper/raw/ideology_k_gap_minimal_majorities.csv'
    if name.startswith('cabinet_date_sensitivity'):
        return True, 'processing/cabinet_date_sensitivity.py', 'cabinet_dataset/releases/' + PIN_VERSION + '/sensitivity_constraints.csv; cabinet_dataset/releases/' + PIN_VERSION + '/service_sensitivity_constraints.csv; processing/Processing/data/cabinet_release_election_crosswalk.csv; processing/Processing/output/decomposition/raw/party_accounting_all_years.csv'
    if name in ('cabinet_period_source_spells.csv', 'cabinet_period_party_set_changes.csv'):
        return True, 'processing/Processing/psc_baseline_repair/build_cabinet_diagnostics.py', 'cabinet_dataset/releases/' + PIN_VERSION + '/witnesses.csv; cabinet_dataset/releases/' + PIN_VERSION + '/periods.csv'
    if 'cabinet_district_concentration' in name:
        return True, modules + 'CabinetDistrictTable.jl', 'processing/Processing/output/decomposition/raw/accounting_focal_state_contributions.csv'
    if 'coalition_party_component' in name:
        return True, modules + 'PartyComponentTable.jl', 'processing/Processing/output/decomposition/raw/coalition_party_contributions.csv'
    if any(token in name for token in ('coalition_party', 'accounting_focal', 'accounting_gross', 'accounting_all_inversion', 'state_weighting_anatomy', 'federation', 'accounting_integration')):
        producer = 'writing/make_coalition_figures.py' if path.suffix == '.pdf' else modules + 'AccountingIntegration.jl'
        return True, producer, source + '; processing/Processing/output/decomposition/raw/inversion_case_registry.csv'
    if any(token in name for token in ('cabinet_party_set', 'party_size_cabinet', 'party_size_diagnostic')) or name == 'party_accounting_all_years.csv':
        return True, modules + 'PartySizeDiagnostics.jl', 'processing/Processing/output/decomposition/raw/coalition_period_quantities.csv; processing/Processing/output/decomposition/raw/party_accounting_all_years.csv'
    if any(token in name for token in ('all_inversion', 'inversion_case_registry', 'table_case_', 'generated_interpretation', 'intermediate_accounting')):
        return True, modules + 'IntermediateAccountingReport.jl', source + '; processing/Processing/output/decomposition/raw/party_district_accounting_all_years.csv'
    if 'prose_analysis' in name:
        return True, modules + 'ProseSummaries.jl', source + '; processing/Processing/output/decomposition/raw/accounting_all_inversion_decomposition.csv'
    if 'coalition_period_quantities' in name or (name.startswith(('inversion_', 'table_inversion_')) and 'ideolog' not in name) or 'observed_inversion_decomposition' in name:
        producer = 'writing/make_coalition_figures.py' if path.suffix == '.pdf' else modules + 'CoalitionDecomposition.jl'
        return True, producer, source
    if any(token in name for token in ('cabinet', 'observed_coalition', 'observed_inversion')):
        producer = 'writing/make_coalition_figures.py' if path.suffix == '.pdf' else 'processing/Processing/running/running.jl'
        return True, producer, source
    if '/report/' in rel and name in ('party_size_diagnostics.tex',):
        return True, modules + 'PartySizeDiagnostics.jl', source
    return False, 'processing/Processing/running/running.jl' if path.is_relative_to(PAPER) else modules + 'run_decomposition.jl', 'frozen electoral/ideological inputs'


def build_artifact_manifest(pin, consumed):
    candidates = set()
    for base in (PAPER, DECOMP):
        manifest = base / 'artifact_manifest.csv'
        if manifest.exists():
            candidates.update((base / row['path']).resolve() for row in read(manifest) if (base / row['path']).is_file())
        # Some maintained late generators (notably cross-domain and sensitivity)
        # write after the Julia manifest. Inventory this bounded output closure.
        for path in base.rglob('*'):
            if path.is_file() and path.suffix in ('.csv', '.json', '.tex', '.pdf', '.png', '.gz'):
                if cabinet_artifact_details(path)[0]:
                    candidates.add(path.resolve())
    candidates.update((ROOT/name).resolve() for name in STANDALONE_FILES if (ROOT/name).is_file())
    # The documented default figure output directory remains an active consumer.
    figure_stems = ['party_vote_share_vs_seat_share', 'observed_coalition_timeline',
        'inversion_decomposition_components', 'accounting_state_weighting_anatomy',
        'ideological_interval_heatmap_2014', 'ideological_interval_heatmap_2018',
        'ideological_interval_heatmap_2022', 'ideological_interval_heatmap_legend',
        'minimal_connected_winning_inversions_3x1_diamond',
        'district_electoral_weight_by_magnitude', 'cross_domain_components']
    for stem in figure_stems:
        extensions = ('.pdf', '.png') if stem == 'minimal_connected_winning_inversions_3x1_diamond' else ('.pdf',)
        for extension in extensions:
            path = ROOT / 'writing/figures' / (stem + extension)
            if not path.is_file():raise AssertionError('Missing maintained default figure: '+str(path))
            candidates.add(path.resolve())
    candidates.update(consumed)
    candidates.add(MAIN.with_suffix('.pdf'))
    known_by_name = {}
    for path in candidates:
        if path.is_relative_to(PAPER) or path.is_relative_to(DECOMP):
            known_by_name.setdefault(path.name, []).append(path)
    rows = []
    for path in sorted(candidates):
        dependent, generator, source = cabinet_artifact_details(path)
        if path.is_relative_to(ROOT / "writing/figures"):
            generator = "writing/make_coalition_figures.py"
        if path in consumed and not (path.is_relative_to(PAPER) or path.is_relative_to(DECOMP)):
            matching = [p for p in known_by_name.get(path.name, []) if digest(p) == digest(path)]
            if matching:
                dependent, generator, source = cabinet_artifact_details(matching[0])
            elif path.suffix == '.pdf' and dependent:
                generator = 'writing/make_coalition_figures.py'
        associated = sorted(p for p in consumed if p == path or
            (p.stem == path.stem and p.suffix in ('.tex', '.pdf', '.png')))
        rows.append(dict(path=relative(path), cabinet_dependent=dependent, generator=generator,
            source_metrics=source, release_version=pin['data_version'] if dependent else '',
            release_metadata_sha256=pin['metadata_sha256'] if dependent else '', sha256=digest(path),
            latex_consumed=path in consumed, latex_path='; '.join(relative(p) for p in associated)))
    return rows


def ideological_slice(rows):
    return [r for r in rows if r.get('domain', '').startswith('k=') or
            r.get('domain') in ('ideological', 'ideology') or r.get('case_domain') == 'ideological']


def substantive_rows(rows, excluded=()):
    # Registry/focal ordinal positions shift when cabinet rows are inserted or
    # removed; ideological identity and every accounting quantity remain fixed.
    omitted = {'focal_order', 'registry_order', 'case_order'} | set(excluded)
    return sorted((json.dumps({k:v for k,v in row.items() if k not in omitted}, sort_keys=True, ensure_ascii=False)
                   for row in rows))


def mixed_invariance_checks():
    checks = []
    independent = {'party_accounting_all_years.csv', 'party_district_accounting_all_years.csv',
                   'district_accounting_all_years.csv', 'accounting_district_electoral_weight.csv',
                   'party_size_correlations.csv', 'party_size_groups.csv', 'party_fragmentation_summary.csv',
                   'table_accounting_minimal_ideological.csv'}
    cabinet_fields = {'ever_in_cabinet', 'cabinet_observation_count', 'cabinet_source_period_count', 'cabinet_days',
        'identified_cabinet_days', 'calendar_cabinet_days', 'unidentified_cabinet_days', 'cabinet_participation_status'}
    for base in (PAPER, DECOMP):
        oldbase = PRE / base.relative_to(ROOT)
        protected = {ROOT / p.relative_to(PRE) for p in oldbase.rglob('*.csv')}
        for path in sorted(protected | set(base.rglob('*.csv'))):
            oldpath = PRE / path.relative_to(ROOT)
            if not oldpath.exists():
                continue
            if path.name in independent:
                before, after = read(oldpath), read(path) if path.exists() else []
                excluded = cabinet_fields if path.name == 'party_accounting_all_years.csv' else ()
                passed = substantive_rows(before, excluded) == substantive_rows(after, excluded)
                checks.append(dict(path=relative(path), check='frozen party/district/ideological accounting values unchanged',
                    passed=passed, before_sha256=digest(oldpath), after_sha256=digest(path) if path.exists() else 'MISSING'))
            elif cabinet_artifact_details(path)[0] and path.name != 'prose_analysis_summaries.csv':
                before, after = ideological_slice(read(oldpath)), ideological_slice(read(path)) if path.exists() else []
                if before:
                    checks.append(dict(path=relative(path), check='mixed-output ideological identities and numerical values unchanged (cabinet-dependent ordinal positions excluded)',
                        passed=substantive_rows(before) == substantive_rows(after), before_sha256=digest(oldpath), after_sha256=digest(path) if path.exists() else 'MISSING'))
    return checks

def main():
    REPORT.mkdir(parents=True,exist_ok=True)
    pin=json.loads((ROOT/'processing/Processing/data/cabinet_release_pin.json').read_text())
    global PIN_VERSION
    PIN_VERSION=pin['data_version']
    release=ROOT/pin['release_path'];meta=json.loads((release/'metadata.json').read_text())
    metrics=read(PAPER/'raw/cabinet_coalition_metrics.csv')
    calendar=read(PAPER/'raw/cabinet_calendar_status.csv')
    old=read(PRE/'processing/Processing/output/paper/raw/cabinet_coalition_metrics.csv')
    frozen=digest(MAIN)==digest(PRE/MAIN.relative_to(ROOT))
    if not frozen: raise AssertionError('Authoritative manuscript source changed despite prose freeze')
    # Enumerate every stale provenance field, rather than aborting at the first.
    prose=[]; cache={}
    for block in parse_blocks(MAIN.read_text()):
        for ref in block.rows:
            path=ROOT/ref.source
            records=cache.setdefault(ref.source,read(path) if path.exists() else [])
            matches=[(i,r) for i,r in enumerate(records,1) if all(r.get(k)==v for k,v in ref.key.items())]
            key='; '.join(f'{k}={v}' for k,v in ref.key.items())
            if len(matches)!=1:
                prose.append(dict(file=relative(MAIN),line=block.line,tex_label_or_block=block.identifier,
                    existing_claim=block.paragraph,status='unsupported',new_value=f'Old semantic key resolves to {len(matches)} current rows',
                    source_output=ref.source,source_row=key,reason='The old case or aggregate is no longer identified by this key. Reassess the whole paragraph against the current release.'))
            else:
                number,row=matches[0]
                for field,recorded in ref.fields.items():
                    actual=row.get(field,'<field absent>')
                    if not same_value(recorded,actual):
                        prose.append(dict(file=relative(MAIN),line=block.line,tex_label_or_block=block.identifier,
                            existing_claim=f'{block.paragraph} [recorded {field}={recorded}]',status='stale quantity',new_value=f'{field}={actual}',
                            source_output=ref.source,source_row=f'data row {number}; {key}',reason='Computed source value changed; source prose and provenance comments were intentionally preserved.'))
    lines=MAIN.read_text().splitlines()
    def prose_paragraph(line_number):
        start=line_number-1;end=start+1
        while start>0 and lines[start-1].strip() and not lines[start-1].lstrip().startswith('%'):start-=1
        while end<len(lines) and lines[end].strip() and not lines[end].lstrip().startswith('%'):end+=1
        return start+1, '\n'.join(lines[start:end])
    # Methods and interpretive claims not captured by numerical provenance.
    manual=[('Wikipedia cabinet-member lists','methods description changed','Pinned reviewed officeholding and contemporaneous person affiliations now determine history; Atlas is a reconciled compilation.','cabinet_dataset/README.md'),
        ('Periods 2021.3 and 2022.1','unsupported','Historical-to-reporting linkage now comes from release source IDs and mapping-equivalent coalescing, not these old fixed IDs.','processing/Processing/output/paper/diagnostics/cabinet_coalitions_before_coalescing.csv'),
        ('Four of the twenty-three','stale quantity',f'{len(metrics)} identified reporting periods, {sum(truth(r["coalition_inversion"]) for r in metrics)} identified inversion periods; {sum(int(r["days"]) for r in calendar if not truth(r["identified"]))} unidentified calendar days.','processing/Processing/output/paper/raw/observed_cabinet_duration_summary.csv'),
        ('In every distinct cabinet composition','unsupported','Scope is identified complete sets only. Check current component signs and missing intervals before making a universal chronology claim.','processing/Processing/output/decomposition/raw/cabinet_party_set_accounting.csv'),
        ('under all four administrations','unsupported','Check administration coverage of current identified inversions; unidentified intervals cannot establish absence or presence.','processing/Processing/output/paper/raw/cabinet_coalition_metrics.csv')]
    for needle,status,value,source in manual:
        for i,line in enumerate(lines,1):
            if needle in line:
                prose.append(dict(file=relative(MAIN),line=i,tex_label_or_block='methods/interpretation',existing_claim=prose_paragraph(i)[1],status=status,new_value=value,source_output=source,source_row='current release/registry',reason='This prose was deliberately frozen; its method, scope or historical referent changed.'))
    current_inversion_years=sorted({r['election_year'] for r in metrics if truth(r['coalition_inversion'])})
    scope_claims=[
      ('following all three elections', 'unsupported', f'Identified inversions occur only for election linkage(s) {current_inversion_years}; other linkages contain unidentified days, so their full-chronology inversion status is unresolved.', 'processing/Processing/output/paper/raw/observed_cabinet_duration_summary.csv'),
      ('Each office', 'methods description changed', 'Office eligibility, effective service, personal affiliation and organizational identity are separate reviewed inputs.', 'cabinet_dataset/CODEBOOK.md'),
      ('each office, incumbent', 'methods description changed', 'The production source is a pinned multi-source adjudicated release; service and affiliation assertions carry separate provenance.', 'cabinet_dataset/CODEBOOK.md'),
      ('A cabinet period lasts', 'methods description changed', 'Historical periods include explicit non-identification and administration boundaries. Paper reporting coalescing is a separate election-mapping operation.', 'processing/Processing/output/paper/raw/cabinet_calendar_status.csv'),
      ('Panel A includes all observed cabinet periods', 'unsupported', f'Panel A can display only {len(metrics)} identified complete reporting sets; unidentified intervals have no component coordinates.', 'processing/Processing/output/decomposition/raw/cross_domain_components.csv'),
      ('Every cabinet', 'unsupported', 'Universal cabinet claims require restriction to identified complete compositions; the full calendar includes 1,880 unidentified days.', 'processing/Processing/output/paper/raw/cabinet_calendar_status.csv'),
      ('Every distinct cabinet', 'unsupported', 'Universal cabinet claims require restriction to identified complete compositions.', 'processing/Processing/output/decomposition/raw/cabinet_party_set_accounting.csv'),
      ('Parties that enter cabinet are larger', 'unsupported', 'Ever/never cabinet participation must distinguish known membership from unknown possible participation; compare regenerated party-size summary status.', 'processing/Processing/output/decomposition/tables/report/party_size_cabinet_summary.csv'),
      ('the three underrepresented', 'stale quantity', f'Current identified underrepresented reporting periods: {sum(float(r["seat_diff"])<0 for r in metrics)}; see each regenerated row for election linkage.', 'processing/Processing/output/paper/raw/cabinet_coalition_metrics.csv'),
      ('PSC-inclusive', 'unsupported', 'The old PSC-inclusive inversion and duration refer to superseded membership/service assumptions. Current identified 2018-linked rows have no inversion; unidentified intervals remain unresolved.', 'processing/Processing/output/paper/raw/observed_cabinet_coalitions_2018.csv'),
      ('unlike PSC', 'unsupported', 'The old PSC departure comparison is not established by the current complete-set baseline.', 'processing/Processing/output/paper/raw/observed_cabinet_coalitions_2018.csv'),
      ('Unlike PSC', 'unsupported', 'The old PSC departure comparison is not established by the current complete-set baseline.', 'processing/Processing/output/paper/raw/observed_cabinet_coalitions_2018.csv'),
      ('All data used in the analysis were checked by the author', 'unsupported', 'The new adjudicated release and residual gaps require author review; pipeline execution does not establish this existing authorship assertion.', 'cabinet_dataset/IMPLEMENTATION_REPORT.md'),
    ]
    seen=set()
    for needle,status,value,source in scope_claims:
        for i,line in enumerate(lines,1):
            if needle in line and not line.lstrip().startswith('%'):
                start,paragraph=prose_paragraph(i)
                if (start,needle) in seen:continue
                seen.add((start,needle))
                prose.append(dict(file=relative(MAIN),line=start,tex_label_or_block='uncaptured methods/scope claim',existing_claim=paragraph,status=status,new_value=value,source_output=source,source_row='current identified rows plus explicit unavailable calendar',reason='Methods, historical identity or full-calendar scope is not completely represented by numerical provenance fields.'))
    write_csv(REPORT/'stale_prose_claims.csv',prose,['file','line','tex_label_or_block','existing_claim','status','new_value','source_output','source_row','reason'])
    text=['# Stale prose report','', 'The manuscript source is byte-identical to its pre-task snapshot. This report supplies no replacement prose. The compiled PDF is an intermediate revision with regenerated artifacts beside intentionally frozen narrative.','',f'Release `{pin["data_version"]}`; cutoff 2026-03-19 inclusive. Full calendar: 4,096 days. Identified reporting periods: {len(metrics)}. Unidentified days: {sum(int(r["days"]) for r in calendar if not truth(r["identified"]))}.','', '| Current location | Existing claim | Status | New computed value / evidence | Source output / row | Reason |','|---|---|---|---|---|---|']
    for r in prose:
        text.append('| '+' | '.join(cell(x) for x in [f'{r["file"]}:{r["line"]} / {r["tex_label_or_block"]}',r['existing_claim'],r['status'],r['new_value'],f'{r["source_output"]} / {r["source_row"]}',r['reason']])+' |')
    (REPORT/'STALE_PROSE_REPORT.md').write_text('\n'.join(text)+'\n')
    # Align old/new intervals, preserving all old intervals and explicit unknown new sets.
    category_groups={
      'R01':'affiliation witness; effective service date', 'R02':'in-tenure affiliation change; effective service date',
      'R03':'effective service date', 'R04':'effective service date', 'R05':'missing office/party witness',
      'R06':'missing office/party witness; succession', 'R07':'missing office/party witness; succession',
      'R08':'missing office/party witness; succession', 'R09':'in-tenure affiliation change',
      'R10':'in-tenure affiliation change; date convention; service interruption', 'R11':'affiliation departure',
      'R12':'affiliation witness', 'R13':'affiliation change; partial non-identification',
      'R14':'contemporaneous affiliation; dated merger; personal party change', 'R15':'affiliation departure; effective date adjudication',
      'R16':'in-tenure affiliation change; date convention', 'R17':'affiliation departure; date convention',
      'R18':'unaffiliated-to-party change; effective service date', 'R19':'in-tenure affiliation change',
      'R20':'missing qualifying office; affiliation assertion adjudication', 'R21':'in-tenure affiliation change',
      'R22':'effective service date', 'R23':'in-tenure affiliation change', 'R24':'in-tenure affiliation change; effective service date',
      'R25':'effective service date; contemporaneous affiliation', 'R26':'dated merger continuity; conflicting affiliation assertion',
      'R27':'affiliation expulsion; service departure', 'R28':'personal affiliation versus portfolio sponsorship',
      'R29':'missing qualifying offices; partial non-identification; cutoff', 'R30':'office scope; actual service interruption',
    }
    decision_rows={r['decision_id']:r for r in read(release/'decisions.csv')}
    cases=read(ROOT/'cabinet_chronology_implementation/cabinet_chronology_handoff/evidence/reconciliation/atlas_audit_reconciliation.csv')
    categorized=[]
    for c in cases:
        d=decision_rows[c['case_id']]
        categorized.append(dict(case_id=c['case_id'],person_or_issue=c['person_or_issue'],change_dimensions=category_groups[c['case_id']],disposition=d['disposition'],evidence_ids=d['evidence_ids'],rationale=d['rationale']))
    write_csv(REPORT/'case_change_dimensions.csv',categorized)
    comparison=[]
    for oldrow in old:
        a=date.fromisoformat(oldrow['period_start']);b=date.fromisoformat(oldrow['period_end'])+timedelta(days=1)
        for new in calendar:
            start=max(a,date.fromisoformat(new['start_inclusive']));end=min(b,date.fromisoformat(new['end_exclusive']))
            if end<=start: continue
            matches=[r for r in metrics if new['period_id'] in json.loads(r['source_periods'])]
            n=matches[0] if matches else {}
            comparison.append(dict(start_inclusive=start,end_exclusive=end,days=(end-start).days,election_year=oldrow['election_year'],
                old_period=oldrow['period'],new_historical_period=new['period_id'],new_reporting_period=n.get('period','unidentified'),
                old_election_parties=oldrow['parties'],new_election_parties=n.get('parties','UNIDENTIFIED'),
                old_seats=oldrow['seats'],new_seats=n.get('seats',''),old_vote_share=oldrow['vote_share'],new_vote_share=n.get('vote_share',''),
                old_inversion=oldrow['coalition_inversion'],new_inversion=n.get('coalition_inversion','unidentified'),
                composition_status=new['composition_status'],decision_ids=new['decision_ids'],
                decision_change_dimensions='; '.join(sorted({category_groups[x] for x in new['decision_ids'].split(';') if x in category_groups})),
                bounded_affiliation_ids=new['bounded_affiliation_ids'],bounded_service_ids=new['bounded_service_ids'],
                consumer_coalescing='multiple historical periods' if n and len(json.loads(n['source_periods']))>1 else 'none'))
    write_csv(REPORT/'cabinet_before_after.csv',comparison)
    summary=['# Cabinet before/after comparison','', '| Election | Old periods | New identified reporting periods | Old inversion periods/days | New identified inversion periods/days | Unidentified days |','|---|---:|---:|---|---|---:|']
    for y in ['2014','2018','2022']:
        o=[r for r in old if r['election_year']==y];n=[r for r in metrics if r['election_year']==y];c=[r for r in calendar if r['election_year']==y]
        inv=lambda rows:[r for r in rows if truth(r['coalition_inversion'])]
        summary.append(f'| {y} | {len(o)} | {len(n)} | {len(inv(o))} / {sum(int(r["period_days"]) for r in inv(o))} | {len(inv(n))} / {sum(int(r["period_days"]) for r in inv(n))} | {sum(int(r["days"]) for r in c if not truth(r["identified"]))} |')
    summary+=['','Historical changes are separately documented in `cabinet_dataset/inputs/decisions.csv` and the reconciliation-case dispositions. They include missing office witnesses and real service interruptions (office/scope), in-tenure personal changes (affiliation), effective-entry/departure corrections and bounded conventions (dates), dated mergers with stable rename identities (organizations), and mapping-equivalent reporting coalescing (consumer only). `case_change_dimensions.csv` separately lists all 30 dispositions and the factual, affiliation, date and scope dimensions adjudicated. `cabinet_before_after.csv` aligns intervals, links those dimensions, records bounded conventions and consumer coalescing, and retains decision IDs; it does not infer the cause of a history change from electoral results.','', 'Counts and inversion days refer to identified complete sets. They are not estimates renormalized over known days, and unidentified days are not coded as noninversions.']
    (REPORT/'BEFORE_AFTER.md').write_text('\n'.join(summary)+'\n')
    # Source/asset hash provenance, including paths actually recorded by LaTeX.
    consumed=set()
    fls=MAIN.with_suffix('.fls')
    if fls.exists():
        for line in fls.read_text(errors='replace').splitlines():
            if line.startswith('INPUT '):
                p=Path(line[6:]);p=(MAIN.parent/p).resolve() if not p.is_absolute() else p.resolve()
                if p.is_file() and p.is_relative_to(ROOT): consumed.add(p)
    artifacts=build_artifact_manifest(pin,consumed)
    write_csv(REPORT/'artifact_manifest.csv',sorted(artifacts,key=lambda r:r['path']))
    # Cabinet-independent numerical registries should be byte-exact under the pinned runtime.
    invariants=[]
    for oldpath in sorted((PRE/PAPER.relative_to(ROOT)).rglob('*.csv')):
        p=ROOT/oldpath.relative_to(PRE)
        name=p.name
        if not (name.startswith(('ideology_','ideological_','party_seat_differentials')) or name in ('table_03_ideology_k_gap_summary.csv','table_03_ideology_exact_connected_summary.csv')): continue
        oldpath=PRE/p.relative_to(ROOT)
        if not oldpath.exists(): continue
        equal=p.exists() and read(oldpath)==read(p)
        invariants.append(dict(path=relative(p),check='cabinet-independent rows and values unchanged',passed=equal,before_sha256=digest(oldpath),after_sha256=digest(p) if p.exists() else 'MISSING'))
    invariants.extend(mixed_invariance_checks())
    standalone_run = subprocess.run([sys.executable, str(REPORT/'validate_standalone_diagnostic_invariance.py')], capture_output=True, text=True)
    if not standalone_run.stdout.strip():
        raise RuntimeError('Standalone diagnostic invariance validator failed: ' + standalone_run.stderr)
    standalone = json.loads(standalone_run.stdout)
    (REPORT/'standalone_diagnostic_invariance.json').write_text(standalone_run.stdout)
    invariants.extend(dict(path=r['file'],check=r['scope']+' unchanged in original all-party standalone sensitivity',passed=r['passed'],before_sha256=r['before_sha256'],after_sha256=r['after_sha256']) for r in standalone['results'])
    for pair in standalone['aliases']:
        directory = PRE if pair['version'] == 'before' else ROOT
        invariants.append(dict(path=pair['first']+'; '+pair['second'],check=pair['version']+' standalone aliases byte-identical',passed=pair['byte_identical'],before_sha256=digest(directory/pair['first']),after_sha256=digest(directory/pair['second'])))
    for record in json.loads((REPORT/'preservation/frozen_inputs_before.json').read_text()):
        p=ROOT/record['path']
        if record['path'].startswith('processing/Processing/data/') and p.name=='cabinet_to_election_party_crosswalk.csv': pass
        current=digest(p)
        invariants.append(dict(path=record['path'],check='frozen input unchanged',passed=current==record['sha256'],before_sha256=record['sha256'],after_sha256=current))
    invariants.append(dict(path=relative(MAIN),check='entire manuscript source byte unchanged',passed=frozen,before_sha256=digest(PRE/MAIN.relative_to(ROOT)),after_sha256=digest(MAIN)))
    write_csv(REPORT/'invariance_checks.csv',invariants)
    failures=[r for r in invariants if not r['passed']]
    print(f'Reported {len(prose)} stale claims/fields, {len(artifacts)} artifact records, {len(invariants)} invariant checks; {len(failures)} failures.')
    if failures: raise AssertionError('Invariance differences require review: '+', '.join(r['path'] for r in failures))
if __name__=='__main__':main()
