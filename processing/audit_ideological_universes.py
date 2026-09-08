#!/usr/bin/env python3
"""Independent, serialized-output audit of parliamentary and all-party domains."""
from __future__ import annotations

import re
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / 'processing/Processing/output/paper'
YEARS = (2014, 2018, 2022)
UNIVERSES = ('seat_winning', 'all_parties')
EXPECTED = {
    (2014, 'seat_winning'): (28, (8, 4), (87, 46)),
    (2018, 'seat_winning'): (30, (8, 1), (108, 42)),
    (2022, 'seat_winning'): (23, (4, 2), (39, 12)),
    (2014, 'all_parties'): (32, (8, 4), (88, 43)),
    (2018, 'all_parties'): (35, (8, 0), (118, 24)),
    (2022, 'all_parties'): (32, (4, 2), (53, 17)),
}


def read(relative):
    return pd.read_csv(PAPER / relative, keep_default_na=False)


def close(a, b):
    assert np.allclose(a, b, atol=1e-9, rtol=0), 'Accounting identity failed'


def main():
    party = read('raw/party_seat_differentials_all_years.csv')
    coalitions = read('raw/ideology_k_gap_accounting_both_universes.csv')
    assert set(coalitions.ideological_universe) == set(UNIVERSES)
    reports = []
    for year in YEARS:
        p = party.loc[party.election_year == year].set_index('party')
        V = int(p.votes.sum())
        assert p.seats.sum() == 513
        close(p.quota.sum(), 513)
        close(p.seat_diff.sum(), 0)
        original = read(f'raw/ideology_order_{year}_all_parties.csv').sort_values('ordinal_position')
        assert set(original.party) == set(p.index)
        positive = [name for name in original.party if p.loc[name, 'seats'] > 0]
        primary = read(f'raw/ideology_order_{year}.csv').sort_values('ordinal_position')
        assert primary.party.tolist() == positive
        assert primary.ordinal_position.tolist() == list(range(1, len(positive) + 1))
        assert primary.original_ordinal_position.tolist() == original.loc[original.party.isin(positive), 'original_ordinal_position'].tolist()
        for universe in UNIVERSES:
            names = positive if universe == 'seat_winning' else original.party.tolist()
            n = len(names)
            assert n == EXPECTED[year, universe][0]
            position = {name: i for i, name in enumerate(names)}
            for k in (0, 1):
                domain = coalitions.loc[(coalitions.election == year) & (coalitions.ideological_universe == universe) & (coalitions.k == k)]
                assert len(domain) == n * (n + 1) // 2 + (n * (n-1) * (n-2) // 6 if k else 0)
                assert domain.coalition_id.is_unique
                assert set(domain.national_vote_total) == {V} == set(domain.V)
                assert set(domain.total_seats) == {513} == set(domain.S)
                masks, member_sums = [], []
                for row in domain.itertuples():
                    members = row.coalition_id.split('|')
                    assert members == [x.strip() for x in row.parties.split(',')]
                    indices = [position[name] for name in members]
                    assert indices == sorted(indices)
                    gaps = set(range(indices[0], indices[-1]+1)) - set(indices)
                    assert len(gaps) == row.gap_count <= k
                    assert row.left_index == indices[0]+1 and row.right_index == indices[-1]+1
                    if universe == 'seat_winning':
                        assert (p.loc[members, 'seats'] > 0).all()
                        assert all(p.loc[names[i], 'seats'] > 0 for i in gaps)
                    votes = int(p.loc[members, 'votes'].sum())
                    seats = int(p.loc[members, 'seats'].sum())
                    assert row.votes == votes and row.seats == seats
                    close(row.vote_share, votes/V)
                    close(row.q_C, 513*votes/V)
                    close(row.d_C, seats-row.q_C)
                    close(row.R_C, seats/row.q_C)
                    assert row.inversion == (2*votes < V and seats >= 257)
                    assert row.seat_majority == (seats >= 257)
                    masks.append(sum(1 << i for i in indices))
                    member_sums.append(float(p.loc[members, 'seat_diff'].sum()))
                close(domain.d_C, member_sums)
                close(domain.A_C+domain.B_C, domain.d_C)
                close(domain.A_over_q+domain.B_over_q, domain.R_C-1)
                winners = [mask for mask, winning in zip(masks, domain.seat_majority) if winning]
                for mask, winning, minimal in zip(masks, domain.seat_majority, domain.minimal_seat_majority):
                    # Proper subsets are searched within this serialized D_k,
                    # independently of the Julia enumeration implementation.
                    expected = winning and not any(other != mask and other & mask == other for other in winners)
                    assert minimal == expected
                actual = (int(domain.minimal_seat_majority.sum()), int((domain.minimal_seat_majority & domain.inversion).sum()))
                assert actual == EXPECTED[year, universe][k+1]
                reports.append(dict(election=year, ideological_universe=universe, k=k, parties=n, V=V, admissible=len(domain), minimal_winners=actual[0], minimal_inversions=actual[1], all_checks_passed=True))
        # Identical members have identical national accounting even when adjacency differs.
        a = coalitions.loc[(coalitions.election == year) & (coalitions.ideological_universe == 'seat_winning')]
        b = coalitions.loc[(coalitions.election == year) & (coalitions.ideological_universe == 'all_parties')]
        shared = a.merge(b, on=['election','k','coalition_id'], suffixes=('_primary','_robustness'))
        for column in ('votes','seats','vote_share','q_C','d_C','R_C','A_C','B_C'):
            close(shared[column+'_primary'], shared[column+'_robustness'])
    # Check all complete party contribution vectors after CSV/gzip round trip.
    member_path = PAPER / 'raw/ideology_k_gap_party_contributions_both_universes.csv.gz'
    if not member_path.exists():
        member_path = member_path.with_suffix('')
    members = pd.read_csv(member_path)
    keys = ['election','ideological_universe','k','coalition_id']
    summed = members.groupby(keys)[['q_i','d_i','A_i','B_i']].sum().reset_index()
    merged = coalitions.merge(summed, on=keys, validate='one_to_one')
    assert len(merged) == len(coalitions)
    for coalition, component in [('q_C','q_i'),('d_C','d_i'),('A_C','A_i'),('B_C','B_i')]:
        close(merged[coalition], merged[component])
    assert (members.loc[members.ideological_universe == 'seat_winning', 'seats'] > 0).all()
    # Audit every primary cabinet closure/gap/overlap from its represented members.
    bridge = read('tables/table_appendix_cabinet_interval_bridge.csv')
    split = lambda value: set(x.strip() for x in str(value).split(',') if x.strip())
    for row in bridge.itertuples():
        order = read(f'raw/ideology_order_{row.election_year}.csv').party.tolist()
        cabinet = split(row.cabinet_parties) & set(order)
        indices = [order.index(name) for name in cabinet]
        closure = set(order[min(indices):max(indices)+1]) if indices else set()
        assert closure == split(row.closure_parties)
        assert closure - cabinet == split(row.closure_gap_parties)
        assert len(closure - cabinet) == row.closure_gap_n
        for prefix in ('closest_mcw', 'closest_mci'):
            near = split(getattr(row, prefix+'_parties'))
            if near:
                close(getattr(row, prefix+'_jaccard'), len(cabinet & near)/len(cabinet | near))
    output = PAPER / 'diagnostics/ideological_universe_reproduction_audit.json'
    output.write_text(json.dumps(dict(domains=reports, member_rows=len(members), bridge_rows=len(bridge), assertions='passed', vote_denominator='all_valid_federal_deputy_votes'), indent=2)+'\n')
    summary = read('tables/ideological_universe_comparison.csv')
    text = ["# Ideological universe refactor: regenerated results", "",
            "Generated by `processing/audit_ideological_universes.py` from the production outputs.", "",
            "The primary universe is `seat_winning`; robustness is `all_parties`. Both use all valid federal-deputy votes in V. The ideological order is filtered without changing positions or relative ordering; each k-domain and its minimality are recomputed independently.", "",
            "| Election | Universe | Parties | k | Minimal winners | Minimal inversions | Strongest inversion | Vote % | Seats |", "|---|---|---:|---:|---:|---:|---|---:|---:|"]
    for r in summary.itertuples():
        votes = f"{float(r.strongest_inversion_vote_share_pct):.2f}" if r.strongest_inversion_vote_share_pct != '' else '--'
        seats = str(int(float(r.strongest_inversion_seats))) if r.strongest_inversion_seats != '' else '--'
        text.append(f"| {r.election} | {r.ideological_universe} | {r.ideological_party_count} | {r.k} | {r.minimal_seat_majority_coalitions} | {r.minimal_inversions} | {r.strongest_inversion_coalition} | {votes} | {seats} |")
    text += ["", "## Substantive changes", ""]
    for universe in UNIVERSES:
        exact = coalitions.loc[(coalitions.ideological_universe == universe) & (coalitions.k == 0)]
        minima = exact.loc[exact.minimal_seat_majority]
        inv = minima.loc[minima.inversion]
        one = coalitions.loc[(coalitions.ideological_universe == universe) & (coalitions.k == 1) & coalitions.minimal_seat_majority]
        retained = len(minima.merge(one, on=['election','coalition_id']))
        signs = int((inv.A_C > 0).sum())
        text.append(f"- **{universe}**: {len(minima)} exact-connected minimal winners; {len(inv)} minimal inversions; {int(exact.inversion.sum())} inversions in the full exact-connected domain. {signs} of {len(inv)} minimal inversions have positive A_C. {retained} exact-connected minima remain minimal under k=1.")
        neg = one.loc[one.inversion & (one.A_C < 0)]
        text.append(f"  The k=1 minimal inversions include {len(neg)} cases with negative A_C.")
    text += ["", "The parliamentary specification introduces the 2018 PT–PSDB exact-connected inversion. Restoring zero-seat parties removes it. The strongest 2014 PTB–PR and 2022 PP–PL endpoint regions survive; their primary k=0 member sets already coincide with the strongest primary k=1 cases. The 2022 MDB–UNIÃO within-district component changes sign between universes. These differences are retained in the manuscript sensitivity appendix.", "",
             f"Primary cabinet gaps range from {bridge.closure_gap_n.min()} to {bridge.closure_gap_n.max()}; all {len(bridge)} closures and nearest-interval overlaps were recomputed using represented cabinet members. Observed cabinet membership and electoral accounting use their existing definitions.", "",
             "## Verification and outputs", "",
             f"All structural and arithmetic assertions passed for {len(coalitions):,} coalition rows and {len(members):,} member rows. Checks cover filtered order, original ranks, zero-seat exclusion, represented-party gaps, unchanged V and S, no vote renormalization, strict inversions, exhaustive domain-relative minimality, A+B=d, member contribution sums, and primary closure/Jaccard calculations.", "",
             "The full production command is `processing/rebuild_manuscript.sh --clean`. Main summary, full compositions, primary and all-party decompositions, contributions, closures, accounting macros, figures, PDFs, and submission archives are generated programmatically. Source input and output manifests are stored under `output/decomposition/audit/` and `output/paper/artifact_manifest.csv`.", "",
             "## Manuscript generated assets", ""]
    manuscript = (ROOT/'writing/submission_inversions_review/manuscript/main_rw_again.tex').read_text()
    assets = sorted(set(re.findall(r"\\input\{([^{}]+)\}", manuscript) + re.findall(r"\\includegraphics(?:\[[^]]*\])?\{([^{}]+)\}", manuscript)))
    text += [f"- `{asset}`" for asset in assets]
    (PAPER/'ideological_universe_refactor_report.md').write_text('\n'.join(text)+'\n')
    print(json.dumps(dict(domains=len(reports), coalitions=len(coalitions), member_rows=len(members), bridge_rows=len(bridge), assertions='passed')))


if __name__ == '__main__':
    main()
