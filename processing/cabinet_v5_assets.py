#!/usr/bin/env python3
"""CSV-only V5 manuscript tables; ordinary manuscript text is never written."""
from cabinet_v5 import OUT, PAPER, DECOMP, ROOT, read, truth, tokens, joined, write
from datetime import date, timedelta
import json

MANUSCRIPT=ROOT/'writing/submission_inversions_review/manuscript'


def tex(value):
    return str(value).replace('_',r'\_').replace('&',r'\&').replace('%',r'\%')


def save(name, text, decomposition=False):
    for directory in [OUT/'tables',PAPER/'latex',MANUSCRIPT]+([DECOMP/'latex'] if decomposition else []):
        directory.mkdir(parents=True,exist_ok=True)
        (directory/name).write_text(text+'\n')


def inversion_table(rows, components=True):
    lines=[r'\begin{tabularx}{\textwidth}{@{}llrrrrrr>{\raggedright\arraybackslash}X@{}}',
        r'\toprule',
        r'Election & Period & Days & Vote \% & Seats & \(d_C\) & \(A_C\) & \(B_C\) & Parties \\' if components else
        r'Election & Period & Days & Vote \% & Seats & \(q_C\) & \(d_C\) & \(R_C\) & Parties \\',r'\midrule']
    for r in rows:
        d,A=round(1000*float(r['d_C'])),round(1000*float(r['A_C']))
        values=[f'{d/1000:.3f}',f'{A/1000:.3f}',f'{(d-A)/1000:.3f}'] if components else [f"{float(r[k]):.3f}" for k in ('q_C','d_C','R_C')]
        lines.append(' & '.join([r['election_year'],tex(r['period']),r['days'],f"{100*float(r['vote_share']):.2f}",r['seats'],*values,tex(r['election_party_set'].replace(';',', '))])+r' \\')
    lines.extend([r'\bottomrule',r'\end{tabularx}'])
    lines.append(r'\par\smallskip{\footnotesize V5 primary analytical periods. '+
        '; '.join(f"{tex(r['period'])}: {r['established_days']} established and {r['provisional_days']} provisional days" for r in rows)+'.}')
    return '\n'.join(lines)


def chronology(rows):
    heading=r'Election & Period & Dates (inclusive) & Days & Vote \% & Seats & \(d_C\) & Evidence status & Prov. days & Election-year parties \\'
    lines=[r'\begin{landscape}',r'\scriptsize',r'\setlength{\tabcolsep}{2.5pt}',r'\renewcommand{\arraystretch}{1.06}',
        r'\begin{longtable}{@{}llp{3.4cm}rrrrlrp{8.2cm}@{}}',
        r'\caption{V5 cabinet chronology after daily election-year translation and analytical recompression}\label{tab:full-cabinet-composition}\\',
        r'\toprule',heading,r'\midrule',r'\endfirsthead',r'\toprule',heading,r'\midrule',r'\endhead']
    for r in rows:
        end=(date.fromisoformat(r['end_exclusive'])-timedelta(days=1)).isoformat()
        lines.append(' & '.join([r['election_year'],tex(r['period']),r['start_inclusive']+'--'+end,r['days'],
            f"{100*float(r['vote_share']):.2f}",r['seats'],f"{float(r['d_C']):.2f}",r['historical_status'],r['provisional_days'],
            tex(r['election_party_set'].replace(';',', '))])+r' \\')
    lines += [r'\bottomrule',r'\end{longtable}',
        r'\noindent\footnotesize Notes: One row per maximal consecutive election-year party set. Dates above are inclusive; CSV end dates are exclusive. '+
        r'The 4,096 days include 3,996 evidence-established days and 100 provisional days. Mixed periods combine both; provisional assumptions add no party '+
        r'and do not establish historical non-affiliation. The link table below identifies the underlying historical periods.']
    lines += [r'\begin{longtable}{@{}llp{15cm}@{}}',r'\caption*{Analytical-to-historical period linkage}\\',
        r'\toprule',r'Period & Analytical ID & V5 source period IDs (each ID embeds its historical start date) \\',r'\midrule',r'\endhead']
    for r in rows:
        lines.append(' & '.join([tex(r['period']),r['analytical_period_id'],tex(r['source_period_ids'].replace(';','; '))])+r' \\')
    lines += [r'\bottomrule',r'\end{longtable}',r'\noindent\footnotesize Exact source starts, exclusive ends, and overlap dates are in '+
        r'\texttt{generated/cabinet\_v5/historical\_analytical\_linkage.csv}.',r'\end{landscape}']
    return '\n'.join(lines)


def main():
    inversions=read(OUT/'cabinet_inversions.csv')
    periods=read(OUT/'cabinet_analysis_periods.csv')
    save('table_observed_inversion_decomposition.tex',inversion_table(inversions),True)
    save('table_02_cabinet_inversion_tabular.tex',inversion_table(inversions,False))
    save('table_appendix_cabinet_composition.tex',chronology(periods))
    # Preserve the same CSV authority as the generated appendix, including its provenance.
    old = read(PAPER/'tables/table_appendix_cabinet_composition.csv')
    byperiod = {p['period']:p for p in periods}
    for row in old:
        row.update({k:v for k,v in byperiod[row['period']].items() if k not in row})
    write(PAPER/'tables/table_appendix_cabinet_composition.csv',old)
    for manifest_path in (PAPER/'artifact_manifest.csv',):
        manifest=read(manifest_path)
        for row in manifest:
            if row['path']=='tables/table_appendix_cabinet_composition.csv':
                row['rows'],row['columns']=len(old),len(old[0])
        write(manifest_path,manifest)
    print('V5 CSV-driven cabinet tables and complete analytical chronology generated.')


if __name__=='__main__':
    main()
