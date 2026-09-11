#!/usr/bin/env python3
"""CSV-only manuscript cabinet tables from the shared distinct-set registry."""
from cabinet_v5 import OUT, PAPER, DECOMP, ROOT, read, truth, write
from cabinet_party_sets import OUT as SETS

MANUSCRIPT=ROOT/'writing/submission_inversions_review/manuscript'


def tex(value):
    return str(value).replace('_',r'\_').replace('&',r'\&').replace('%',r'\%')


def save(name, text, decomposition=False):
    for directory in [SETS/'tables',OUT/'tables',PAPER/'latex',MANUSCRIPT]+([DECOMP/'latex'] if decomposition else []):
        directory.mkdir(parents=True,exist_ok=True)
        (directory/name).write_text(text+'\n')


def inversion_table(rows, components=True):
    lines=[r'\begin{tabularx}{\textwidth}{@{}llrrrrrr>{\raggedright\arraybackslash}X@{}}',r'\toprule',
        r'Election & Set & Days & Vote \% & Seats & \(d_C\) & \(A_C\) & \(B_C\) & Parties \\' if components else
        r'Election & Set & Days & Vote \% & Seats & \(q_C\) & \(d_C\) & \(R_C\) & Parties \\',r'\midrule']
    for r in rows:
        d,A=round(1000*float(r['d_C'])),round(1000*float(r['A_C']))
        values=[f'{d/1000:.3f}',f'{A/1000:.3f}',f'{(d-A)/1000:.3f}'] if components else [f"{float(r[k]):.3f}" for k in ('q_C','d_C','R_C')]
        lines.append(' & '.join([r['election_year'],r['display_label'],r['total_observed_days'],f"{100*float(r['vote_share']):.2f}",r['seats'],*values,tex(r['canonical_membership'].replace(';',', '))])+r' \\')
    lines.extend([r'\bottomrule',r'\end{tabularx}'])
    lines.append(r'\par\smallskip{\footnotesize Days sum actual appearances of each set; all inversion days are evidence-established.}')
    return '\n'.join(lines)


def composition(rows):
    heading=r'Set & Election-year parties & Occ. & Days & Prov. & First observed & Last observed \\'
    lines=[r'\begin{landscape}',r'\fontsize{9}{10.5}\selectfont',r'\setlength{\tabcolsep}{4pt}',r'\renewcommand{\arraystretch}{1.0}',
        r'\begin{longtable}{@{}lp{12.2cm}rrrll@{}}',
        r'\caption{Distinct election-year cabinet party sets}\label{tab:full-cabinet-composition}\\',
        r'\toprule',heading,r'\midrule',r'\endfirsthead',r'\toprule',heading,r'\midrule',r'\endhead']
    for r in rows:
        lines.append(' & '.join([r['display_label'],tex(r['canonical_membership'].replace(';',', ')),r['occurrence_count'],r['total_observed_days'],r['provisional_days'],r['first_observed'],r['last_observed']])+r' \\')
    lines += [r'\bottomrule',r'\end{longtable}',
        r'\noindent\footnotesize Labels begin with the election year (14, 18, or 22) and order sets by first appearance within that election. '+
        r'Occ. counts separate appearances; Days sums their actual covered dates. First and last observed dates do not imply continuous observation. '+
        r'Prov. counts provisional days under the unchanged no-additional-party assumptions. '+
        r'Full intervals, historical periods, administrations, and daily evidence links are provided in the replication CSVs.',r'\end{landscape}']
    return '\n'.join(lines)


def main():
    rows=read(SETS/'cabinet_party_sets.csv');inversions=[r for r in rows if truth(r['coalition_inversion'])]
    save('table_observed_inversion_decomposition.tex',inversion_table(inversions),True)
    save('table_02_cabinet_inversion_tabular.tex',inversion_table(inversions,False))
    save('table_appendix_cabinet_composition.tex',composition(rows))
    write(SETS/'inverted_sets.csv',inversions)
    write(PAPER/'tables/table_appendix_cabinet_composition.csv',rows)
    for manifest_path in (PAPER/'artifact_manifest.csv',):
        manifest=read(manifest_path)
        for name,count,columns in [('table_02_cabinet_inversion_tabular.tex',len(inversions),9),('table_appendix_cabinet_composition.tex',len(rows),7)]:
            rel='latex/'+name
            if not any(r['path']==rel for r in manifest):
                row={k:'' for k in manifest[0]};row.update(path=rel,artifact_type='latex',description='Generated from the shared distinct cabinet party-set registry.',rows=count,columns=columns);manifest.append(row)
        for r in manifest:
            if r['path']=='tables/table_appendix_cabinet_composition.csv':r['rows'],r['columns']=len(rows),len(rows[0])
        write(manifest_path,manifest)
    print('Cabinet tables: one row per distinct set, with cumulative actual days.')

if __name__=='__main__':main()
