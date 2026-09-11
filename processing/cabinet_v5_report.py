#!/usr/bin/env python3
"""Finish the analytical handoff, frozen-prose change map, and upload bundle."""
from __future__ import annotations
import re
import shutil
import subprocess
import sys
import zipfile
from collections import defaultdict
from cabinet_v5 import *
from cabinet_v5_validation import check_invariance, quantitative, BEFORE

MAIN=ROOT/'writing/submission_inversions_review/manuscript/main_rw_again.tex'
PDF=MAIN.with_suffix('.pdf')
sys.path.insert(0,str(ROOT/'processing/Processing/decomposition'))
from validate_prose_provenance import parse_blocks, same_value


def stats(periods):
    return dict(periods=len(periods),inversions=sum(truth(r['inversion_status']) for r in periods),
        A_positive=sum(float(r['A_C'])>0 for r in periods),B_positive=sum(float(r['B_C'])>0 for r in periods),
        R_above_one=sum(float(r['R_C'])>1 for r in periods),R_below_one=sum(float(r['R_C'])<1 for r in periods),
        statuses=dict(Counter(r['historical_status'] for r in periods)),
        inversion_days={str(y):sum(int(r['days']) for r in periods if int(r['election_year'])==y and truth(r['inversion_status'])) for y in (2014,2018,2022)})


def robustness(inversions,periods,sensitivities):
    lines=['# Cabinet V5 inversion robustness','',
        'Primary classifications use V5 released daily sets. End dates below are exclusive. '
        'Robustness concerns recorded concrete alternatives, not invented affiliations for the nine unresolved officeholders.','']
    for r in inversions:
        lines += [f"## {r['analytical_period_id']} / {r['period']} / {r['administration']}",
            f"- Dates: [{r['start_inclusive']}, {r['end_exclusive']}); {r['days']} days.",
            f"- Election {r['election_year']}; parties: {r['election_party_set']}.",
            '- '+ '; '.join(f'{k}={r[k]}' for k in ('vote_share','seats','q_C','d_C','R_C','A_C','B_C'))+'.',
            f"- Evidence-established days: {r['established_days']}; provisional days: {r['provisional_days']}.",
            f"- Recorded concrete sensitivities touching the interval: {r['concrete_sensitivity_ids'] or 'none'}.",
            f"- Robust to every recorded concrete alternative: {r['robust_to_all_recorded_concrete_sensitivities']}.",
            f"- {r['notes']}",'']
    for basis in ('manuscript_reported','preintegration_generated'):
        old=read(DATA/'cabinet_v5_comparison_baseline'/f'{basis}.csv')
        lines += [f'## Comparison: {basis}',f"Old analytical periods: {len(old)}; old inversion periods: {sum(truth(r['coalition_inversion']) for r in old)}.",'']
        oldsets=set()
        for o in old:
            if not truth(o['coalition_inversion']):continue
            key=(o['election_year'],joined(o['parties'].split(', ')));oldsets.add(key)
            same=[n for n in inversions if (n['election_year'],n['election_party_set'])==key]
            if same:
                lines.append(f"- Old {o['period']} ({o['period_start']} through {o['period_end']}, {o['period_days']} days): configuration survives as "+
                    '; '.join(f"{n['analytical_period_id']} [{n['start_inclusive']}, {n['end_exclusive']}) / {n['days']} days" for n in same)+'.')
            else:lines.append(f"- Old {o['period']} ({o['period_days']} days): inversion configuration disappears from the V5 primary chronology.")
        new=[n for n in inversions if (n['election_year'],n['election_party_set']) not in oldsets]
        lines.append('- New inversion configurations: '+(', '.join(n['analytical_period_id'] for n in new) if new else 'none')+'.')
        lines.append('')
    flips=[r for r in sensitivities if truth(r['inversion_classification_changes'])]
    lines += ['## Sensitivity and provisional-day totals',
        f"- {len({r['sensitivity_id'] for r in sensitivities})} concrete sensitivity records; {len({r['scenario_id'] for r in sensitivities})} nonbaseline scenarios; {len(flips)} compressed comparisons change inversion classification.",
        f"- Primary inversion days: {sum(int(r['days']) for r in inversions)}; provisional inversion days: {sum(int(r['provisional_days']) for r in inversions)}.",
        '- The 100 provisional days remain conditional on V5 no-additional-party assumptions. Absence of an inversion on those dates is not a bound on every historically unknown affiliation.',
        '- MRE 2021 service-date alternatives retain the outgoing UNKNOWN state. Quantified comparison is explicitly conditional on the existing V5 primary assumptions; no arbitrary alternative party is assigned.',
        '- Sachsida has no released alternative in V5 and no sensitivity is recreated.',
        '- Full values and dates: cabinet_sensitivity_results.csv; whole-calendar recompressions: cabinet_sensitivity_periods.csv.']
    (OUT/'CABINET_INVERSION_ROBUSTNESS.md').write_text('\n'.join(lines)+'\n')


def source_audit():
    """Inventory all repository TeX matches without rewriting any source."""
    paths=subprocess.check_output(['rg','--files','--hidden','--no-ignore','-g','*.tex','-g','!audit/cabinet_v5_before/**','-g','!.git/**'],cwd=ROOT,text=True).splitlines()
    pattern=re.compile(r'cabinet|Dilma|Temer|Bolsonaro|Lula|twenty-three|four inversions|PSC-inclusive|\b(?:102|238|255)\b',re.I)
    rows=[]
    for rel in paths:
        for n,line in enumerate((ROOT/rel).read_text(errors='replace').splitlines(),1):
            if pattern.search(line):
                role='active manuscript' if ROOT/rel==MAIN else 'generated asset' if Path(rel).name.startswith('table_') else 'other draft, report or archived source'
                rows.append(dict(file=rel,line=n,role=role,text=line))
    write(OUT/'manuscript_source_search.csv',rows)
    codepaths=subprocess.check_output(['rg','--files','processing','writing','-g','*.py','-g','*.jl','-g','*.sh'],cwd=ROOT,text=True).splitlines()
    dependencies=[]
    codepattern=re.compile(r'cabinet|observed.coalition|coalition_period_quantities|cross_domain_components|inversion_case_registry',re.I)
    for rel in codepaths:
        matches=[(n,line) for n,line in enumerate((ROOT/rel).read_text(errors='replace').splitlines(),1) if codepattern.search(line)]
        if not matches:continue
        role='test' if '/tests/' in rel or '/test/' in rel or Path(rel).name.startswith('test_') else 'historical audit or exploratory code' if '/exploratory/' in rel or Path(rel).name in ('build_downstream_audit.py','build_post_psc_manifest.py') else 'analysis, figure, validation or packaging code'
        for n,line in matches:dependencies.append(dict(file=rel,line=n,role=role,reference=line))
    write(OUT/'cabinet_code_dependency_search.csv',dependencies)


def modification_map(periods,inversions):
    text=MAIN.read_text();lines=text.splitlines();s=stats(periods)
    sections={};section='Abstract'
    for i,line in enumerate(lines,1):
        match=re.search(r'\\(?:sub)?section\*?\{([^}]+)\}',line)
        if match:section=match.group(1)
        sections[i]=section
    items=[]
    def add(needle,new,reason,source,kind='interpretive',group='C',priority='BLOCKING',title=None):
        # Find prose only, never comments, then collect the containing paragraph.
        found=[i for i,l in enumerate(lines) if needle in l and not l.lstrip().startswith('%')]
        assert found, 'Unlocated change-map anchor: '+needle
        for i in found:
            a,b=i,i+1
            while a and lines[a-1].strip() and not lines[a-1].lstrip().startswith('%'):a-=1
            while b<len(lines) and lines[b].strip() and not lines[b].lstrip().startswith('%'):b+=1
            items.append(dict(section=sections[i+1],line=a+1,anchor=title or needle,
                current=' '.join(l.strip() for l in lines[a:b]),reason=reason,new=new,source=source,kind=kind,group=group,priority=priority))
    periods_file='generated/cabinet_v5/cabinet_analysis_periods.csv'
    inv_file='generated/cabinet_v5/cabinet_inversions.csv'
    comparison='generated/cabinet_v5/cabinet_analysis_before_after.csv (comparison_baseline=manuscript_reported)'
    sens='generated/cabinet_v5/cabinet_sensitivity_results.csv'
    bridge='processing/Processing/output/paper/tables/table_appendix_cabinet_interval_bridge.csv'
    summary=f"{s['periods']} analytical periods; {s['inversions']} inversions, Dilma II/2014 (5 days) and Lula III/2022 (255 days); zero primary inversions linked to 2018 or under Temer."
    coverage='4,096 primary days: 3,996 evidence-established and 100 provisional. Both primary inversions use only evidence-established days. Nine unresolved officeholders retain UNKNOWN affiliations; the separate primary assumption contributes no additional party.'
    signs=f"A_C>0 in {s['A_positive']}/{s['periods']} periods; B_C>0 in {s['B_positive']}; R_C>1 in {s['R_above_one']}; R_C<1 in {s['R_below_one']}."
    add('following all three elections',summary,'The cabinet and ideological domains no longer have the same election coverage.',inv_file,'factual','A')
    add('The empirical pattern spans both',summary,'The introduction result preview conflates cabinet and ideological coverage.',inv_file,'factual','A')
    add('Wikipedia cabinet-member lists','Describe the standalone officeholder-level released chronology, separate service and contemporaneous affiliation records, scope/closing-state rules, dated witnesses, evidence and decisions; identify the single V5 release pin.',
        'Wikipedia extraction alone no longer describes the analytical source.', 'generated/cabinet_v5/provenance.json; V5 CODEBOOK.md','methodological','B')
    add('A cabinet period lasts','State that daily historical sets are mapped to the corresponding election-year identities BEFORE maximal consecutive recompression. Historical label or merger events alone do not create analytical periods.',
        'The described period object and operation order are stale.',periods_file+'; historical_analytical_linkage.csv','methodological','B')
    add('2026. Periods are first delimited', 'Retain election windows; distinguish 55 historical periods from 53 analytical periods; document the audited stable identities and merger expansion. Replace obsolete example identifiers only after consulting historical_analytical_linkage.csv.',
        'Daily mapping precedes analytical period boundaries; the old 2021.3/2022.1 example refers to superseded identifiers.',periods_file,'methodological','B')
    add('reports the cabinet sequence used',coverage+' Include concrete date/affiliation sensitivities, their results, and limits of unbounded UNKNOWN affiliations.',
        'The data section currently omits V5 provisional coverage and sensitivity handling.',inv_file+'; '+sens+'; provenance.json','limitation/provenance','B')
    add('Four of the twenty-three',summary,'Both the denominator and case identities changed.',inv_file,'numerical','A')
    add('The first inversion emerges', 'The same Dilma party vector survives as 2016.3 / CV5-005, April 14-18 inclusive (5 days). Numerical coalition quantities survive. Released transitions record REPUBLICANOS leaving March 18, PP leaving April 14, and PSD leaving April 19; distinguish these events when revisiting the start/end explanation.',
        'The old period ID and two-day duration are stale; do not infer a party exit from the old endpoints.',comparison+'; V5 transitions.csv','factual','A')
    add('The later Temer inversion begins', 'No Temer primary analytical period is an inversion. Over the old interval, initial V5 membership adds PR, PRB and PTB and removes PSB, giving 312 seats and 57.8279279843202% of votes. Subsequent sets have 302, 268 and 247 seats; the 268-seat interval still has 50.03050576961592% of votes. None inverts.',
        'This entire example and its threshold comparison rely on a removed historical configuration.',comparison,'interpretive','C')
    add('The cabinet sequence linked to the 2018', 'No V5 primary 2018-linked cabinet period inverts. During August 4, 2021-March 23, 2022, the V5 set excludes PATRIOTA and PSDB relative to the old example: 222 seats and 39.79336317736909% of votes. Subsequent March transitions produce 187 or 158 seats. The former 257-seat, 238-day episode is absent; revisit the proposed NOVO/PP/PSC transition and overrepresentation example.',
        'The concrete cabinet example no longer exists, even though its abstract threshold distinction remains mathematical.',comparison,'interpretive; transition','C')
    add('The initial Lula III cabinet provides', 'CV5-041/2023.1 retains the same party set, quantities and 255-day duration. Its comparison to a Bolsonaro inversion is unsupported; use V5 entry/service dates for the endpoint.',
        'The Lula numbers survive but the converse-transition argument references a removed case.',inv_file+'; '+comparison,'transition','C')
    add('the pattern extends to the entire',signs+' Ever/never cabinet mean vote shares: 2014 5.720562356882122%/0.8347979203981275%; 2018 5.412186779334364%/1.8351252882662546%; 2022 5.567791806230482%/1.6593249162617105%. Participation uses primary sets and includes provisional dates.',
        'Counts and selection-dependent party-size summaries must be evaluated against the enlarged chronology.',periods_file+'; processing/Processing/output/decomposition/tables/report/party_size_cabinet_summary.csv','numerical; limitation/provenance','A')
    add('In every distinct cabinet composition','The claim fails in CV5-030/2020.9, December 9, 2020-February 11, 2021: DEM;PSC;PSD;PSL. Large-party positive A=4.2634970812796755; all negative A=-5.49437225087713; balance=-1.2308751695974545. It holds in 33 of 34 distinct sets. Total A remains positive because smaller parties also contribute positive A.',
        'The enlarged chronology adds a direct counterexample to the universal large-party-dominance claim; the exception itself is evidence-established.',
        'processing/Processing/output/decomposition/raw/cabinet_party_set_accounting.csv','interpretive','C','IMPORTANT')
    add('The four inverted cabinet periods', 'Only Dilma CV5-005 and Lula CV5-041 remain. Dilma q+A=254.47093010683732<257, with B closing the shortfall; Lula q+A=263.991908723467 and B=-0.9919087234670415 reduces the result to 263 seats. No Temer or Bolsonaro inversion can support the comparison.',
        'The four-case comparison and old period labels are stale.',inv_file,'interpretive','C')
    add('than inversion. Figure',signs+' Report administration/election membership of the underrepresented periods from the canonical CSV.',
        'Figure 2 now contains the full 53-period primary chronology and displays provisional days.',periods_file,'numerical','A')
    add('Measured over time, inversions occupy', 'Inversion days by election: 2014=5/1461; 2018=0/1461; 2022=255/1174. Total=260/4096. Disclose 100 provisional days in the full denominator; none is a primary inversion day.',
        'The old 102 and 238 day totals are obsolete; the 255 day total survives.',inv_file+'; processing/Processing/output/paper/raw/observed_cabinet_duration_summary.csv','numerical','A')
    add('threshold under exact connectedness, alongside', 'The 2018 PT--PSDB ideological inversion remains unchanged; there is no accompanying primary cabinet inversion.',
        'An unchanged ideological paragraph contains an indirect reference to the removed cabinet case.',inv_file,'interpretive','C')
    add('In the 2018 cabinet inversion, for example', signs+' PP and PR party A_i values remain unchanged, but they cannot be described as contributions to a V5 cabinet inversion in 2018.',
        'Party-level constants survive while their cabinet-case attribution does not.',inv_file+'; processing/Processing/output/decomposition/raw/party_accounting_all_years.csv','interpretive','C')
    add('component: \\(B_C\\) is positive in only',signs+' B reinforces the Dilma inversion and offsets the Lula inversion. Remove/reconsider comparisons invoking Temer or 2018 cabinet inversions; ideological results are unchanged.',
        'The sign count and two cabinet examples are obsolete.',inv_file+'; '+periods_file,'numerical; interpretive','A')
    add('Panel A includes all observed cabinet', 'Panel A uses all 53 primary analytical periods, including mixed/provisional periods; Panel B values are unchanged. State the primary-set scope and connect the V5 provenance flag to interpretation.',
        'The panel selection now includes a complete primary calendar and explicit uncertainty.', 'generated/cabinet_v5/figure5_panel_a_source.csv; noncabinet_invariance.csv','limitation/provenance','B','IMPORTANT')
    add('period lies to the right of',signs+' Check the updated underrepresented-period labels and compare inversion A_C/q_C with the full regenerated panel.',
        'The positive-A and three-underrepresented-period statements survive, but the primary-set scope and plotted configuration identifiers must be reconciled with the new panel.',periods_file,'numerical','A')
    add('In 2018, the 238-day', 'The 2018 ideological case survives, while no V5 primary cabinet inversion occurs in that election. Reconsider the claimed same-election dual-domain evidence.',
        'The discussion relies on a removed cabinet episode.',inv_file,'interpretive','C')
    add('timeline shows that relative advantage is common',signs+' Only two primary analytical periods invert.',
        'The discussion repeats the 20/23 and four-inversion totals.',periods_file,'numerical','A')
    add('result. The within-district component is positive', 'A is positive in both cabinet inversions. B reinforces Dilma and offsets Lula; use the new Dilma identifier 2016.3. Unchanged ideological cases retain their values.',
        'The four-case count, cabinet label and cabinet/ideological partition must be updated.',inv_file,'numerical; interpretive','A')
    add('addition, the vote share',coverage+' Recorded concrete sensitivities do not replace unbounded affiliation uncertainty.',
        'The limitations require explicit primary-assumption and historical-provenance scope.',inv_file+'; '+sens,'limitation/provenance','B','IMPORTANT')
    add('The empirical comparison is not uniform',summary+' The ideological exact-connected and one-gap results remain unchanged.',
        'The conclusion retains two 2014 cases and a 238-day 2018 cabinet inversion.',inv_file,'factual','A')
    add('cabinet coalitions are overrepresented, but only four', 'Two cabinet periods satisfy inversion; retain the mathematical distinction between R, d, q and majority status.',
        'The conclusion repeats the removed cases.',inv_file,'numerical','A')
    add('\\textbf{Data availability.}', 'Identify the released V5 chronology and compact generated/cabinet_v5 analytical outputs; ensure the cited repository actually contains the updated assets before publication.',
        'The existing generic cabinet-input description does not identify the new released source; this task does not publish or push anything.', 'generated/cabinet_v5/README.md; provenance.json','limitation/provenance','B','IMPORTANT')
    add('\\textbf{AI assistance.}', 'Author must review the description of historical research, extraction and downstream assistance and personally verify the author-checking assertion; the pipeline cannot attest to author actions.',
        'Wikipedia-only extraction no longer describes the full data workflow.', 'CABINET_V5_INTEGRATION_REPORT.md; V5 COMPLETION_REPORT.md','factual; limitation/provenance','B','IMPORTANT')
    add('four cabinet inversions and all twelve', 'Contribution tables now select two cabinet inversions; ideological rows remain unchanged. Check the unchanged ideological-count wording independently against its existing source.',
        'The cabinet row count is stale.',inv_file+'; processing/Processing/output/decomposition/tables/table_coalition_party_contributions.csv','numerical','A')
    add('contributions to \\(A_C\\) in the four distinct', 'Two distinct cabinet inversion vectors and regenerated district concentration rows.',
        'The appendix count no longer matches the regenerated table.',inv_file+'; processing/Processing/output/decomposition/tables/table_cabinet_district_concentration.csv','numerical','A')
    add("majority. Unlike PSC's departure", 'The one-gap PSDB exclusion remains unchanged, but its comparison with a PSC departure ending an observed cabinet inversion is unsupported.',
        'This indirect cabinet dependency occurs in an ideological appendix paragraph.',comparison,'interpretive','C')
    add('table keeps two operations separate', 'Cabinet periods are now defined only after daily election-year translation and recompression. Parliamentary closure still filters represented cabinet parties without changing cabinet votes/seats.',
        'The bridge appendix describes the wrong order of operations.',bridge+'; '+periods_file,'methodological','B')
    add('The first result is that no observed cabinet', 'All 53 periods still have nonzero closure gaps; the range is now 4-16 represented parties, not 7-16. The scope includes 8 mixed and 2 fully provisional periods.',
        'The universal nonconnectedness claim and seven-to-sixteen range rely on obsolete membership.',bridge,'numerical; interpretive','A')
    add('All connected closures in Table', '48 of 53 closures are vote-and-seat majorities; five are neither: 2022.1-2022.4 have 39.197948917097875% and 203 seats; 2022.5 has 48.80269913179969% and 251 seats. Dilma CV5-005/2016.3 has PCdoB-PR closure, 77.46357740119768%, 408 seats, 12 gaps.',
        'The universal claim, period ID, and example membership must be checked against new periods.',bridge,'interpretive','C')
    add('The same logic applies to the more durable', 'The old Temer inversion and its eight-party set are absent as a primary inversion; reassess the full ideological-location argument and nearest-inversion comparison.',
        'The paragraph explains a historical cabinet inversion that disappears.',comparison+'; '+bridge,'interpretive','C')
    add('The Dilma coalitions have a different', 'Re-evaluate descriptive left anchors, member entry/exit, and span changes using the V5 bridge and daily memberships.',
        'Qualitative cabinet membership and transition claims can be stale without a hardcoded statistic.',bridge,'interpretive','C','IMPORTANT')
    add('2016.3 to 2016.4 alone increases', 'The actual May 12, 2016 transition is 2016.4 -> 2016.5. Unweighted means 4.95 -> 6.718181818181819 (delta 1.7681818181818185); seat-weighted 5.2699551569506715 -> 6.924913294797687 (delta 1.6549581378470153). The bridge within-administration delta columns are blank at this administration boundary; compare the two rows explicitly.',
        'Reused period labels refer to different dates and memberships.',bridge,'numerical; transition','A')
    add('The Bolsonaro-period cabinets are more', 'Update cabinet membership, closure and Jaccard examples from V5; distinguish the unchanged 2018 ideological inversion from the absent primary cabinet inversion.',
        'The old PSC-inclusive cabinet and overlap interpretation no longer describe the chronology.',bridge+'; '+comparison,'interpretive','C')
    add('The 2023.1 Lula cabinet returns', 'Lula CV5-041/2023.1 survives: PSOL-UNIÃO closure has 20 parties, 11 gaps, 79.83088706012424% and 407 seats; nearest minimal inversion MDB-UNIÃO has Jaccard 0.1875. Preserve the surviving values while reconciling the new provenance and full-calendar context.',
        'The cabinet vector survives, but the paragraph must be checked with the regenerated comparison table and new chronological scope.',bridge,'interpretive','C','IMPORTANT')
    add('2018, the PSC-inclusive cabinet period', 'The 2018 primary cabinet domain has no inversion; retain separately supported ideological results and reassess the final domain synthesis.',
        'The appendix synthesis repeats the removed 257-seat cabinet episode.',inv_file,'interpretive','C')
    # Audit every numerical provenance block, including embedded ordinary prose
    # that a keyword search could miss. Do not auto-rekey obsolete period labels.
    for block in parse_blocks(text):
        stale=[];sources=[]
        for ref in block.rows:
            path=ROOT/ref.source
            rows=read(path) if path.exists() else []
            matches=[r for r in rows if all(str(r.get(k,''))==v for k,v in ref.key.items())]
            key='; '.join(f'{k}={v}' for k,v in ref.key.items())
            if len(matches)!=1:
                stale.append(f'{key}: old semantic key resolves to {len(matches)} rows; reassess membership/date identity before selecting a new row.')
            else:
                for field,old in ref.fields.items():
                    new=matches[0].get(field,'<absent>')
                    if not same_value(old,new):stale.append(f'{key}; {field}: recorded {old}; current keyed value {new}. Reused period IDs may denote a different cabinet.')
            sources.append(ref.source)
        if stale:
            items.append(dict(section=sections[block.line],line=block.line,anchor='Provenance block '+block.identifier,
                current=block.paragraph,reason='One or more frozen numerical claims or semantic keys disagree with regenerated outputs.',
                new=' | '.join(stale),source='; '.join(sorted(set(sources))),kind='numerical; factual',group='A',priority='BLOCKING'))
    write(OUT/'manuscript_required_changes.csv',items)
    intro=['#+title: Cabinet V5 Manuscript Modifications','#+startup: overview','',
        '* Executive summary','** New empirical cabinet result',summary,signs,
        '** What remains unchanged','Election profiles, party ideology orders, k=0/k=1 enumeration, ideological heatmaps, minimal-connected quantities and Figure 5 Panel B are numerically unchanged; see noncabinet_invariance.csv.',
        '** Historical uncertainty relevant to interpretation',coverage,
        'Concrete alternative robustness is distinct from unbounded unknown affiliations. No replacement prose is supplied. Current manuscript text and provenance comments were preserved byte-for-byte.',
        '** Revision classes','- A: empirical changes caused by chronology, including obsolete numerical provenance keys.',
        '- B: methodological description and limitation/provenance requirements.',
        '- C: interpretations and transitions whose cabinet example or scope must be reconsidered.',
        '* Required modifications by manuscript section']
    for section in dict.fromkeys(x['section'] for x in items):
        intro.append('** '+section)
        for r in [x for x in items if x['section']==section]:
            intro += ['*** TODO '+r['anchor'],f":PROPERTIES:\n:CLASS: {r['group']}\n:PRIORITY: {r['priority']}\n:END:",
                f"- Manuscript section: {r['section']}",f"- Location: [[file:{MAIN.relative_to(ROOT)}::{r['line']}][line {r['line']}]]; {r['anchor']}",
                '- Current claim/value: '+r['current'],'- Why it is stale / requires review: '+r['reason'],
                '- New result or methodological fact that must be reflected: '+r['new'],
                '- Source output: '+r['source'],'- Change type: '+r['kind'],'- Priority: '+r['priority'],'']
    intro += ['* Global consistency checks',
        '- [ ] Distinguish the manuscript-reported 23-period/four-inversion baseline from the preintegration generated 33-period/two-inversion paper-v1 baseline.',
        '- [ ] Synchronize abstract, introduction, results, cross-domain comparisons, discussion, conclusion, appendix and all surviving case IDs; never globally substitute numbers in ideological claims.',
        '- [ ] Preserve 5/1461, 0/1461 and 255/1174 primary inversion-day totals; do not call 100 provisional dates historically established.',
        '- [ ] Review captions and table notes for the 53-period primary analytical object and provenance; source linkage is generated, not a dump of raw historical periods.',
        '- [ ] Refresh numerical provenance comments together with eventual prose edits, using semantic membership/date identity instead of reusing old period labels.',
        '- [ ] Add V5 source citations and primary/sensitivity methodology; do not restart historical reconstruction or assign hypothetical parties to unresolved people.',
        '- [ ] The source search covers all repository .tex files. Only main_rw_again.tex is the active manuscript; duplicate drafts/archives are inventoried in manuscript_source_search.csv and were not rewritten.',
        '- [ ] Verify author statements, data availability and anonymous-repository contents during the later revision/publication session; this integration does not commit or publish.',
        '- [ ] Recompile after the later prose revision and rerun the normal strict provenance audit (without --freeze-prose).']
    (ROOT/'CABINET_V5_MANUSCRIPT_MODIFICATIONS.org').write_text('\n'.join(intro)+'\n')
    return items


def inventory():
    import cabinet_release_report as legacy
    legacy.PIN_VERSION=json.loads((DATA/'cabinet_release_pin.json').read_text())['data_version']
    records=[]
    before_hashes=json.loads((DATA/'cabinet_v5_comparison_baseline/preservation_signatures.json').read_text())['files']
    candidates=[]
    for base in (PAPER,DECOMP,MAIN.parent,ROOT/'writing/figures'):
        candidates.extend(p for p in base.rglob('*') if p.is_file() and p.suffix in ('.csv','.tex','.pdf','.png','.json','.gz'))
    candidates += [ROOT/name for name in legacy.STANDALONE_FILES if (ROOT/name).exists()]
    for path in sorted(set(candidates)):
        dependent,producer,source=legacy.cabinet_artifact_details(path)
        if not dependent:continue
        if path.name in ('table_observed_inversion_decomposition.tex','table_02_cabinet_inversion_tabular.tex','table_appendix_cabinet_composition.tex'):
            producer='processing/cabinet_v5_assets.py'
            source='generated/cabinet_v5/cabinet_inversions.csv' if 'inversion' in path.name else 'generated/cabinet_v5/cabinet_analysis_periods.csv; generated/cabinet_v5/historical_analytical_linkage.csv'
        elif path.name.startswith('cabinet_date_sensitivity'):
            producer='processing/cabinet_v5.py analyze'
        before_hash=before_hashes.get(str(path.relative_to(ROOT)), '')
        after_hash=digest(path)
        records.append(dict(path=str(path.relative_to(ROOT)),producer=producer,
            canonical_cabinet_input='generated/cabinet_v5/cabinet_analysis_periods.csv',other_inputs=source,
            before_sha256=before_hash,after_sha256=after_hash,
            changed=before_hash!=after_hash,status='regenerated'))
    write(OUT/'cabinet_dependency_inventory.csv',records)
    return records


def package(pin,periods,inversions,sensitivities,items,inventory_rows):
    for directory in ('tables','figures','figure_data','v5_provenance'):(OUT/directory).mkdir(exist_ok=True)
    cabinet_tables=[r for r in inventory_rows if r['path'].startswith('processing/Processing/output/paper/latex/') and r['path'].endswith('.tex')]
    for r in cabinet_tables:shutil.copy2(ROOT/r['path'],OUT/'tables'/Path(r['path']).name)
    figure_names=['observed_coalition_timeline.pdf','cross_domain_components.pdf','inversion_decomposition_components.pdf','accounting_state_weighting_anatomy.pdf']
    for name in figure_names:shutil.copy2(MAIN.parent/name,OUT/'figures'/name)
    for name in ('cross_domain_components','cross_domain_components_seat_winning','cross_domain_components_all_parties'):
        rows=read(PAPER/'figure_data'/f'{name}.csv')
        write(OUT/'figure_data'/f'{name}.csv',rows)
        if name=='cross_domain_components':
            byperiod={p['period']:p for p in periods}
            panel=[]
            for r in rows:
                if r['domain']!='cabinet':continue
                p=byperiod[r['display_label']]
                panel.append(dict(r,analytical_period_id=p['analytical_period_id'],established_days=p['established_days'],
                    provisional_days=p['provisional_days'],historical_status=p['historical_status'],sensitivity_ids=p['sensitivity_ids']))
            write(OUT/'figure5_panel_a_source.csv',panel)
    shutil.copy2(DATA/'cabinet_release_election_crosswalk.csv',OUT/'v5_provenance/election_crosswalk.csv')
    for name in ('party_accounting_all_years.csv','coalition_party_contributions.csv'):
        shutil.copy2(DECOMP/'raw'/name,OUT/name)
    release=ROOT/pin['release_path']
    for name in ('metadata.json','CODEBOOK.md','COMPLETION_REPORT.md','primary_assumptions.csv','date_sensitivity.csv','service_sensitivity_constraints.csv','sensitivity_constraints.csv'):
        shutil.copy2(release/name,OUT/'v5_provenance'/name)
    s=stats(periods)
    changedtables=sorted({Path(r['path']).name for r in inventory_rows if truth(r['changed']) and r['path'].endswith('.tex')})
    (OUT/'README.md').write_text(f'''# Cabinet V5 analytical handoff

This is downstream electoral analysis, not a historical evidence archive.

**Transformation:** V5 historical daily party set -> audited election-year party mapping -> maximal consecutive analytical recompression -> electoral quantities.

The source is `{pin['release_path']}`. Historical identities and all historical UNKNOWN records remain unchanged. The 2014, 2018 and 2022 elections apply respectively to 2015-2018, 2019-2022 and 2023-2026-03-19. The audited crosswalk is unchanged. DEM/PSL -> UNIÃO maps to DEM+PSL for 2018; renames retain election-year identities. No historical reconstruction runs in the paper pipeline.

There are **{s['periods']} analytical periods** and **{s['inversions']} primary inversions**. All 4,096 dates have primary sets: 3,996 established and 100 provisional. The 100 days rely on separate no-additional-party assumptions for nine historically unresolved people, not evidence of non-affiliation. Both inversions are entirely evidence-established. Period status is established, mixed or provisional; daily historical_status retains V5's established/unidentified meaning while primary_set_status records primary_adjudicated/primary_provisional.

- `cabinet_analysis_daily.csv`: exactly one row per date, historical display labels/stable IDs, election parties, provenance and all quantities.
- `cabinet_analysis_periods.csv`: authoritative maximal periods, stable CV5 IDs plus compatibility year.sequence labels, full quantities and evidence counts.
- `cabinet_inversions.csv`: primary inversion registry and recorded-concrete-sensitivity robustness.
- `cabinet_sensitivity_results.csv`: each nonbaseline concrete alternative, recompressed over its affected window. Blank affected-party fields mark composition-neutral or unbounded records, not invented parties.
- `cabinet_sensitivity_periods.csv`: complete recompressed calendar for every scenario. Cases are changed one at a time. All comparisons change one released fact at a time and hold all other facts at their primary values. Unknown affiliations remain unbounded.
- `cabinet_analysis_before_after.csv`: interval-overlap comparison for BOTH the 23-period manuscript narrative and the 33-period preintegration generated baseline. Do not add repeated old/new durations across overlap rows. Old unavailable days have blank quantities/status, never false inversion flags. Changed memberships are diagnosed using V5 historical comparison decisions; date-boundary differences are recorded separately. No new party mapping is introduced.
- `historical_analytical_linkage.csv`: exact historical starts, exclusive ends and overlaps underlying each analytical period.
- `cabinet_timeline_source.csv`, `figure5_panel_a_source.csv`, `figure_data/`, `tables/`, `figures/`: reviewable figure data and generated assets.
- `CABINET_INVERSION_ROBUSTNESS.md`, `quantitative_validation.csv`, `noncabinet_invariance.csv`, `cabinet_dependency_inventory.csv`: outcome and verification records.
- `cabinet_code_dependency_search.csv`: repository-wide code references, separated from historical audits and tests; maintained asset producers are in the dependency inventory.
- `manuscript_required_changes.csv` and root `CABINET_V5_MANUSCRIPT_MODIFICATIONS.org`: editing checklist, with no replacement prose. The PDF intentionally retains stale prose.
- `v5_provenance/`: compact release metadata/assumptions/constraints, without raw evidence snapshots.

Shares are proportions, not percentages; A, B, q and d use seats. R is dimensionless. An inversion requires vote share < 0.5 and seats >=257. Dates are ISO; end_exclusive is half-open. Semicolons separate parties/IDs; historical_party_sets is a JSON array. Repeated periods with the same set at nonconsecutive dates remain separate.

Reproduce from the repository root:

```bash
{COMMAND}
```

`processing/Processing/data/cabinet_release_pin.json` is the single release selector. Reproduction validates hashes, runs the existing Julia election/decomposition pipelines, independently checks exact district/party closure, regenerates all figures/tables, compiles both PDFs and creates the reports/ZIP. Manuscript prose is never changed. The checked-in comparison inputs/signatures are under `processing/Processing/data/cabinet_v5_comparison_baseline/`.
''')
    warning_lines=[l for l in MAIN.with_suffix('.log').read_text(errors='replace').splitlines() if re.search(r'Warning|Overfull|Underfull|undefined',l)]
    (OUT/'latex_build_warnings.txt').write_text('\n'.join(warning_lines)+'\n')
    changes=[r for r in sensitivities if truth(r['inversion_classification_changes'])]
    report=['# Cabinet V5 integration report','',
        f"1. Source release: `{pin['release_path']}`; metadata SHA-256 `{pin['metadata_sha256']}`. Release files unchanged.",
        '2. Transformation: released daily contemporaneous sets -> unchanged audited election-year crosswalk -> maximal analytical recompression -> existing electoral/decomposition pipeline plus independent exact validation.',
        f"3. Old periods: manuscript-reported baseline 23; preintegration paper-v1 generated baseline 33 identified periods (with unavailable calendar intervals). Both are preserved and compared.",
        f"4. New periods: {s['periods']} (from 55 historical periods); historical status counts {s['statuses']}.",
        '5. Manuscript old inversions: Dilma 2016.2 (2 days); Temer 2017.1 (100 days); Bolsonaro 2021.3/2022.1 (238 days); Lula 2023.1 (255 days). Preintegration generated inversions: Lula 2023.1 (123 days) and 2023.3 (7 days).',
        '6. New primary inversions: '+ '; '.join(f"{r['analytical_period_id']} / {r['period']} / {r['administration']}, [{r['start_inclusive']},{r['end_exclusive']}), {r['days']} days" for r in inversions)+'.',
        '7. Manuscript comparison: Dilma configuration survives, 2 -> 5 days; Lula configuration and 255 days survive; Temer and Bolsonaro configurations disappear. Against paper-v1 generated outputs, Dilma reappears and the two Lula episodes are covered by one continuous 255-day period. Inversion days by election: '+str(s['inversion_days'])+'.',
        '8. Provisional treatment: 100 daily no-additional-party assumptions, historical UNKNOWN preserved; 3,996 evidence-established days. No primary inversion contains a provisional day.',
        f"9. Sensitivities: {len({r['sensitivity_id'] for r in sensitivities})} concrete records / {len({r['scenario_id'] for r in sensitivities})} nonbaseline scenarios; {len(changes)} compressed comparisons change inversion classification. Nine unbounded affiliations are not supplied hypothetical parties. Sachsida alternative is absent.",
        '10. Changed generated table assets: '+', '.join(changedtables)+'. Full dependency inventory includes CSVs, compatibility copies, report tables, contribution rows and bridge tables.',
        '11. Regenerated cabinet figures: '+', '.join(figure_names)+'. Figure 5 Panel B numerical values are unchanged.',
        '12. Appendix: full analytical chronology with established/mixed/provisional status and day counts; historical linkage table; party d/A/B contributions; district concentration; cabinet-to-ideology bridges for both universes.',
        f"13. Non-cabinet validation: {len(read(OUT/'noncabinet_invariance.csv'))} protected products/slices unchanged. Includes party/district election profiles, both ideological universes, k=0/k=1, ideological contributions and Figure 5 Panel B. Cabinet participation annotations in party-accounting files change by design; electoral fields do not.",
        '14. Manuscript compilation: successful through latexmk in the canonical pipeline; ordinary manuscript prose and its provenance comments match the preintegration snapshot. Stale numerical/source references are recorded for later editing; technical warnings are in latex_build_warnings.txt.',
        f'15. PDF: `{PDF.relative_to(ROOT)}`.',
        '16. Analytical handoff: `generated/cabinet_v5/`.',
        '17. Upload ZIP: `handoff/cabinet_v5_analysis_handoff.zip`.',
        '18. Manuscript change map: `CABINET_V5_MANUSCRIPT_MODIFICATIONS.org`; '+str(len(items))+' dependency/provenance checklist entries; no replacement prose.',
        f'19. Exact command: `{COMMAND}`.',
        '',f"Other cabinet-wide results: {s['A_positive']}/{s['periods']} A_C positive; {s['B_positive']} B_C positive; {s['R_above_one']} R_C>1; {s['R_below_one']} R_C<1.",
        '', 'Preservation: `audit/cabinet_v5_before/` retains the pre-task PDF, generated products, source files, working-tree diff and hash manifest. Existing unrelated changes were preserved. No commit or push was made.']
    (ROOT/'CABINET_V5_INTEGRATION_REPORT.md').write_text('\n'.join(report)+'\n')
    for name in ('CABINET_V5_INTEGRATION_REPORT.md','CABINET_V5_MANUSCRIPT_MODIFICATIONS.org'):shutil.copy2(ROOT/name,OUT/name)
    manifest={str(p.relative_to(OUT)):dict(sha256=digest(p),bytes=p.stat().st_size)
        for p in OUT.rglob('*') if p.is_file() and p.name!='bundle_manifest.json'}
    manifest['main_rw_again.pdf']=dict(sha256=digest(PDF),bytes=PDF.stat().st_size)
    (OUT/'bundle_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    (ROOT/'handoff').mkdir(exist_ok=True)
    with zipfile.ZipFile(ROOT/'handoff/cabinet_v5_analysis_handoff.zip','w',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        for path in sorted(OUT.rglob('*')):
            if path.is_file():z.write(path,str(path.relative_to(OUT)))
        z.write(PDF,'main_rw_again.pdf')
    print('V5 reports, Org checklist, analytical handoff and ZIP complete.')


def main():
    check_invariance();quantitative()
    pin,release,_=validate_inputs()
    before_hashes=json.loads((DATA/'cabinet_v5_comparison_baseline/preservation_signatures.json').read_text())['files']
    assert digest(MAIN)==before_hashes[str(MAIN.relative_to(ROOT))], 'Manuscript prose changed'
    assert PDF.exists() and PDF.stat().st_size>10000
    periods=read(OUT/'cabinet_analysis_periods.csv');inversions=read(OUT/'cabinet_inversions.csv');sensitivities=read(OUT/'cabinet_sensitivity_results.csv')
    daily=read(OUT/'cabinet_analysis_daily.csv')
    coverage=[]
    for administration in dict.fromkeys(r['administration'] for r in daily):
        rr=[r for r in daily if r['administration']==administration]
        coverage.append(dict(administration=administration,days=len(rr),
            established_days=sum(not truth(r['provisional_day']) for r in rr),
            provisional_days=sum(truth(r['provisional_day']) for r in rr),
            inversion_days=sum(truth(r['inversion_status']) for r in rr)))
    write(OUT/'coverage_by_administration.csv',coverage)
    robustness(inversions,periods,sensitivities)
    source_audit()
    items=modification_map(periods,inversions)
    inventory_rows=inventory()
    package(pin,periods,inversions,sensitivities,items,inventory_rows)


if __name__=='__main__':main()
