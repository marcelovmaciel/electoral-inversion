#+TITLE: Audit TODO for test_running.jl

* Overview / audit goal
This checklist is based on the logic in =test_running.jl= and the shared =Processing= code it calls. It is meant for a human researcher reading the script, running it, and deciding which parts of the analysis are reliable, ambiguous, or likely mistaken. It does not assume any specific runtime output in advance.

- [ ] Treat this file as a guide to audit the script's logic, not as proof that any printed result is correct.
- [ ] Keep =test_running.jl= open while using this checklist so you can compare each output block to the code that generated it.
- [ ] Keep notes year by year, because 2014, 2018, and 2022 do not use identical vote or ideology inputs.
- [ ] Separate data-hygiene problems from substantive findings; many apparent inversions could be artifacts of label translation.

* Run the script and capture output
The script prints large tables directly to stdout and mutates global =Processing= paths at startup. Capture a clean run so you can tie any failure to the exact stage where it happens.

- [x] Run the script from a fresh Julia session and capture both stdout and stderr, for example: =julia test_running.jl 2>&1 | tee out/test_running_stdout.txt=.
- [x] Note whether the first failure happens in =PART 1. LOAD DATA= or in =PART 2. ANALYZE SEAT DIFFERENTIALS=.
- [x] If the script stops, record the exact year and subsection that failed before changing any data or code.
- [x] Save the printed lists of parties, coalition periods, and ideology source names; they are the quickest sanity checks later.

  Captured output: =processing/Processing/exploratory/audit/out/test_running_stdout.txt=.
  Finding: =processing/Processing/exploratory/audit/findings/run_capture.md=.
  Result: script completed with status =0=, so there was no first failing stage, year, or subsection to record.

* Validate data loading assumptions
The script constructs several paths by walking upward from =@__DIR__=. If any directory layout changed, the analysis can fail or silently read the wrong files.

- [x] Confirm the printed =repo_root=, =data_root=, =coalition_json_path=, and =classification_root_dir= point to the intended repository copy.
- [x] Confirm these election files exist and are the intended versions: =data/raw/electionsBR/2014/party_mun_zone.csv=, =2014/candidate.csv=, =2018/party_mun_zone.csv=, =2018/candidate.csv=, =2022/party_mun_zone.csv=, =2022/candidate.csv=.
- [x] Confirm the coalition input exists at =scraping/output/partidos_por_periodo.json= and that its period keys look like =YYYY.k=.
- [x] Confirm ideology source files exist under =scrape_classification/output/classificacao_2023/= and =scrape_classification/output/classificacao_2025/=.
- [x] Confirm the shared alias table exists at =Processing/data/party_aliases.csv=.
- [x] Confirm the cabinet-to-election crosswalk exists at =Processing/data/cabinet_to_election_party_crosswalk.csv=, because observed coalition analysis depends on it.
- [x] Check whether the hard-coded =expected_total_seats = 513= is appropriate for every run you intend to treat as valid.

  Finding: =processing/Processing/exploratory/audit/findings/data_loading_paths.md=.
  Result: required local paths and files were present for the captured 2014, 2018, and 2022 run; coalition period keys matched =YYYY.k=; captured totals reported 513 seats in all three years. Caveat: confirming the election files are the authoritative raw versions requires provenance or checksum evidence beyond file existence.

* Validate party canonicalization and alias mapping
The script canonicalizes party labels in votes, seats, coalitions, and ideology data before joining anything. If those mappings are wrong, every downstream table can look coherent while being substantively off.

- [x] Review =Processing.normalize_party= and verify that removing accents, punctuation, and spacing does not collapse distinct party labels that should remain separate.
- [x] Check whether every raw party label seen in vote and seat files maps through =canonical_party_for_election(..., strict = false)= without falling back to accidental equivalence.
- [x] Audit the local electoral overrides in =test_running.jl=: 2014 =PATRIOTA -> PEN= and the 2022 =AGIR -> AGIR= passthrough.
- [x] Decide whether any local override is doing substantive work that should instead live in =party_aliases.csv=.
- [x] Compare the printed party lists from votes and seats for each year before trusting the merged summary.
- [x] Flag any party label that only appears after normalization and is not obvious from the raw files.

  Finding: =processing/Processing/exploratory/audit/findings/party_identity.md=.
  Result: normalization did not collapse distinct observed vote/seat raw labels, and no elected-seat party lacked votes after canonicalization. Problem found: local overrides for 2014 =PATRIOTA -> PEN= and 2022 =AGIR -> AGIR= do substantive work that is not represented in the shared alias table, so other scripts using only =party_aliases.csv= may disagree.

* Validate vote column detection
Vote totals are not taken from one fixed schema. The script selects columns dynamically, and that choice can change the meaning of =valid_total= across years.

- [x] Confirm 2014 uses =QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA= through =ARC.vote_kwargs_for_year(2014)=.
- [x] Confirm 2018 and 2022 rely on automatic detection and note which columns were actually chosen.
- [x] If both component columns and a total-vote column exist, manually compare them to see whether the component sum matches the intended valid-vote concept.
- [x] Check whether any fallback to =QT_VOTOS=, =TOTAL_VOTOS=, or =QT_VOTOS_VALIDOS= would mix valid votes with a broader total.
- [x] Verify that the script filters to =DS_CARGO == "DEPUTADO FEDERAL"= before aggregating votes nationally.
- [x] Check whether party totals look plausible after canonicalization, especially for renamed parties.

  Finding: =processing/Processing/exploratory/audit/findings/vote_columns.md=.
  Result: problem found. 2014 correctly uses forced component columns. 2018 and 2022 use automatic detection, but =first_in_df= returns the first matching dataframe column rather than the first preferred candidate, so =QT_VOTOS_LEGENDA_VALIDOS= is selected instead of the apparently preferred =QT_TOTAL_VOTOS_LEG_VALIDOS=. This excludes converted nominal-to-legend votes: 206,687 in 2018 and 123,458 in 2022. No broad fallback column was used in the captured run.

* Validate seat loading and winner-status logic
Seat counts come from exact winner-status matching in =candidate.csv=. A small mismatch in status labels can propagate into wrong seat totals and false inversion signals.

- [x] Confirm the raw candidate file uses one of these exact winner labels for elected candidates: =ELEITO=, =ELEITO POR QP=, =ELEITO POR MEDIA=, =ELEITO POR MÉDIA=.
- [x] Check for winner-status variants that should count as elected but would be missed by exact matching.
- [x] Verify that party canonicalization in =candidate.csv= uses the same year-specific identity logic as in the vote file.
- [x] Confirm the national seat total really sums to 513 after the winner filter.
- [x] Check whether any party has seats but zero votes, or votes but zero seats, and whether that reflects data reality or a join problem.
- [x] Inspect a few parties manually to confirm the winner count matches the official seat count.

  Finding: =processing/Processing/exploratory/audit/findings/seat_loading.md=.
  Result: confirmed with a provenance caveat. Exact winner-status matching covers the observed elected labels in 2014, 2018, and 2022; national winner totals and local =seats.csv= vacancy totals are 513 in all three years; no elected-seat party lacks votes after canonicalization. This validates local =candidate.csv= / =seats.csv= consistency, not an external official party-seat table.

* Validate coalition period linkage to election years
The script links election years to post-election coalition periods by mandate. If that linkage is wrong, the observed coalition analysis is answering the wrong question even if the arithmetic is correct.

- [x] Confirm the script's linkage is the intended one: 2014 election -> 2015, 2016, 2017, 2018 periods; 2018 election -> 2019, 2020, 2021, 2022 periods; 2022 election -> 2023, 2024, 2025 periods.
- [x] Decide whether including the mandate-end year in full is theoretically justified for each election.
- [x] Check whether =coalitions_by_year= is returning periods by direct =YYYY.= prefix instead of by date-window overlap, and decide whether that is acceptable.
- [x] Compare the printed coalition period keys to =period_start= and =period_end= dates from =coalition_period_windows=.
- [x] Confirm the raw parties in =partidos_por_periodo.json= are sensible before canonicalization.
- [x] Flag any period whose dates or label suggest it belongs to a different electoral mandate.

  Finding: =processing/Processing/exploratory/audit/findings/coalition_period_linkage.md=.
  Result: problem found. The high-level election-to-mandate linkage is implemented as intended and matches the shared runner mapping. However, =coalitions_by_year= returns direct =YYYY.= period keys before considering date-window overlap, so it should not be read as consistently returning every period active during a calendar year. Including mandate-end years in full remains a substantive research choice that should be documented explicitly.

* Validate coalition translation into election-space parties
Observed coalition tables are built in two steps: first canonicalize cabinet-year party labels, then translate them into election-year party identities. This is one of the highest-risk parts of the script.

- [x] Confirm =Processing.canonicalize_parties(raw_parties; year = coalition_year, strict = true)= resolves every coalition member without ambiguity.
- [x] Audit =cabinet_to_election_party_crosswalk.csv= for cases where one cabinet party expands into multiple election-year parties.
- [x] For several periods, manually verify that the final =election_space_parties= all exist in =summary_df.SG_PARTIDO=.
- [x] Check whether same-label passthrough in =cabinet_parties_in_election_space= is ever incorrectly mixing cabinet-year and election-year party identities.
- [x] Verify that translated coalitions do not silently gain or lose parties relative to the raw coalition record.
- [x] Flag any coalition where translation choices materially change the vote total, seat total, or inversion status.

  Diagnostic: =processing/Processing/exploratory/audit/check_coalition_translation.jl=.
  Output: =processing/Processing/exploratory/audit/out/coalition_translation_periods.csv= and =processing/Processing/exploratory/audit/out/coalition_translation_party_map.csv=.
  Result: no problem found in the translation mechanics. Strict coalition canonicalization succeeded for all 22 mandate-period rows, no raw coalition members collapsed during canonicalization, no translated party was missing from the relevant =summary_df.SG_PARTIDO=, and same-label passthrough showed no election-year canonical drift. The current crosswalk has seven one-to-one mappings and no one-to-many expansions. Translation is substantively important, though: explicit crosswalk mappings changed votes or seats in 19 of 22 periods, and changed =candidate_inversion= from false to true in two 2014 periods: =2016.2=, where =MDB|PL= translates to =PMDB|PR= and adds 16,427,468 votes plus 99 seats, and =2017.1=, where =MDB= translates to =PMDB= and adds 10,791,949 votes plus 65 seats. These are crosswalk-sensitive observed-coalition findings, not silent translation failures.

* Validate party summary construction
The central party summary is the base table for both observed coalitions and ideology sweeps. If it is wrong, all later tables inherit the error.

- [x] Recompute a few party rows manually to confirm =vote_share = votes / total_votes=, =quota = vote_share * total_seats=, and =seat_diff = seats - quota=.
- [x] Confirm =Processing.party_summary= uses an outer join on =SG_PARTIDO= and replaces missing votes or seats with zero.
- [x] Decide whether treating missing votes or missing seats as zero is substantively correct for this analysis.
- [x] Compare the highest positive and negative =seat_diff= parties against raw vote and seat rankings to catch obvious anomalies.
- [x] Check that the total seats and total votes used later in coalition summaries are exactly those from =summary_df=.

  Diagnostic: =processing/Processing/exploratory/audit/check_party_summary.jl=.
  Output: =processing/Processing/exploratory/audit/out/party_summary_checks.csv=, =processing/Processing/exploratory/audit/out/party_summary_recomputed.csv=, =processing/Processing/exploratory/audit/out/party_summary_missing_inputs.csv=, and =processing/Processing/exploratory/audit/out/party_summary_extremes.csv=.
  Result: no problem found. =Processing.party_summary= uses an outer join on =SG_PARTIDO=, coalesces missing votes and seats to zero, and computes =vote_share=, =quota=, and =seat_diff= with the expected formulas. Manual recomputation matched all rows exactly within floating precision, and summary totals matched input totals: 2014 =97,355,354= votes and =513= seats, 2018 =98,264,190= votes and =513= seats, and 2022 =109,413,508= votes and =513= seats. No party had seats while missing votes. Vote-only zero-seat parties were retained, which is substantively appropriate because they belong in the vote denominator with zero seats. Highest and lowest =seat_diff= rows looked plausible against vote and seat ranks, and both observed coalition summaries and ideology sweeps take their vote/seat totals from =summary_df= or tables joined back to it.

* Validate observed coalition seat differential analysis
The observed coalition table is the script's direct comparison between cabinet coalitions and the election-year party summary. This is where translation mistakes can become substantive-looking findings.

- [x] For a few coalition periods in each year, manually sum member-party votes and seats from =summary_df= and compare them to =observed_table=.
- [x] Confirm =vote_majority= means vote share strictly greater than 0.5, and =seat_majority= means seats strictly greater than half of total seats.
- [x] Decide whether strict > 50% thresholds are the intended theoretical rule or whether ties need explicit treatment.
- [x] Confirm =candidate_inversion= is defined only as =seat_majority && !vote_majority=.
- [x] Check whether large coalition =seat_diff= values are robust to alternative party translation choices.
- [x] Compare adjacent coalition periods to see whether inversion findings are stable or just artifacts of relabeling across periods.

  Diagnostic: =processing/Processing/exploratory/audit/check_observed_coalition_analysis.jl=.
  Output: =processing/Processing/exploratory/audit/out/observed_coalition_checks.csv=, =processing/Processing/exploratory/audit/out/observed_coalition_recomputed.csv=, =processing/Processing/exploratory/audit/out/observed_coalition_manual_checks.csv=, =processing/Processing/exploratory/audit/out/observed_coalition_translation_sensitivity.csv=, =processing/Processing/exploratory/audit/out/observed_coalition_adjacent_changes.csv=, and =processing/Processing/exploratory/audit/out/observed_coalition_sensitivity_note.md=.
  Result: no problem found. All 22 observed coalition rows matched manual recomputation from =summary_df=, and the definitions are exactly =vote_share > 0.5=, =coalition_seats > total_seats / 2=, and =candidate_inversion = seat_majority && !vote_majority=. With =513= seats, seat majority starts at =257=; no exact threshold ties were observed. The observed inversion rows are =2014/2016.2=, =2014/2017.1=, and =2022/2023.1=. Documentation warnings remain important. First, cabinet labels are intentionally translated into the election-year party-label space before summing votes and seats; for example, 2014 cabinet labels such as =MDB= and =PL= must be counted as the 2014 election labels =PMDB= and =PR=. Same-label-only matching is a negative control showing the wrong result without historical translation, not a valid alternative specification. Second, adjacent-period inversion changes are substantive results of changing cabinet membership, not robustness failures; the documented transitions are =2016.1 -> 2016.2=, =2016.2 -> 2016.3=, =2016.4 -> 2017.1=, =2017.1 -> 2018.1=, and =2023.1 -> 2023.2=.

* Validate ideology mapping and ideology ordering
The ideology stage uses source-year classifications and then translates them into election-year party identities. This can drop parties or force historically questionable equivalences.

- [x] Confirm source selection: 2014 and 2018 use classification 2023, while 2022 uses classification 2025.
- [x] Review the ideology raw overrides for 2022, especially =CDD -> CIDADANIA= and =PROGRE -> PP=.
- [x] Review the ideology label overrides for 2014: =AVANTE -> PT DO B=, =DC -> PSDC=, =MDB -> PMDB=, =PATRIOTA -> PEN=, =PODE -> PTN=.
- [x] Review the 2014 ideology drop list =NOVO=, =PMB=, =REDE= and decide whether those parties are being dropped for good methodological reasons.
- [x] Check whether any ideology rows are silently skipped because =canonical_party(..., strict = false)= returns =__UNKNOWN__= and no raw override exists.
- [x] Confirm the script raises an error if translated ideology rows duplicate the same =SG_PARTIDO=.
- [x] Confirm the script raises an error if any party in =summary_df= has no ideology position after translation.
- [x] Flag any party that disappears only because of ideology translation, not because it is absent from the election summary.

  Finding: =processing/Processing/exploratory/audit/findings/ideology_mapping.md=
  Result: mixed / mostly fine with caveats. =build_ideology_order= explicitly uses classification 2023 for 2014/2018 and classification 2025 for 2022, follows the audited raw -> normalized -> canonical -> raw override -> label override -> drop -> summary join pipeline, rejects duplicate translated parties, and errors when any =summary_df= party lacks ideology coverage. The current files have no silent unknown skips, no inner-join-only disappearances, and complete coverage of all three election summaries. The caveats are methodological and reproducibility-related: 2022 =CDD -> CIDADANIA=, =PROGRE -> PP=, and =AGIR -> AGIR= compensate for missing/source-specific alias coverage, while the 2014 label overrides retroactively assign successor-party ideology positions to predecessor election labels. The 2014 =NOVO=, =PMB=, and =REDE= drops are harmless for the current summary because those parties are absent from the 2014 election summary.

* Validate ideology sweep logic for minimal majority coalitions
The sweep table is not a search over all coalitions. It only checks contiguous left-to-right blocks in ideology order and stops at the first seat majority from each starting party.

- [x] Confirm that the sweep operates on ideology-ordered parties joined to =summary_df=, not on observed coalition periods.
- [x] For several starting parties, manually trace the accumulation to verify the script stops at the first seat majority.
- [x] Decide whether the note =first seat majority= matches the intended theoretical concept of a minimal majority coalition.
- [x] Check whether the contiguous-block assumption is substantively defensible for the cases you want to interpret.
- [x] Inspect all rows with =reached_seat_majority = false= and determine whether the right-tail coalition really never reaches 257 seats.
- [x] Compare sweep-generated inversions to observed coalition periods before treating them as politically meaningful.

  Finding: =processing/Processing/exploratory/audit/findings/ideology_sweep.md=
  Result: mixed / mechanically correct with interpretive caveats. =build_ideology_sweep_table= operates on =ideology_df= joined to =summary_df= and sorted by ideology ordinal position; it does not use observed coalition periods. Accumulation traces confirm that rows stop at the first point where seats exceed half of 513, and every =reached_seat_majority = false= row has a right-tail total below 257 seats. The caveat is interpretive: =first seat majority= is only minimal with respect to extending the right endpoint from a fixed start party in a contiguous ideology block. It is not a search over all coalitions or even all possible ideologically compact alternatives. Sweep inversions differ from observed-coalition inversions: 2014 has five sweep inversion start parties versus observed periods =2016.2= and =2017.1=, 2018 has none, and 2022 has six sweep inversion start parties versus observed period =2023.1=.

* Validate interpretation of candidate inversions
In this script, =candidate_inversion= is a summary label for an aggregate coalition condition. It should not be read as direct evidence about candidate-level or district-level inversions.

- [x] Treat =candidate_inversion= as a coalition-level majority test, not as an individual-candidate phenomenon.
- [x] Keep separate the three concepts the script prints: positive =seat_diff=, seat majority, and seat-majority-without-vote-majority.
- [x] Check whether any inversion case disappears once alias, crosswalk, or ideology problems are corrected.
- [x] Note whether inversion appears in observed coalitions, ideology sweeps, or both; those are not interchangeable findings.
- [x] Ask whether national aggregation is the right level for interpreting proportional representation inversions in the first place.

  Finding: =processing/Processing/exploratory/audit/findings/candidate_inversion_interpretation.md=
  Result: mixed / correct label mechanics with strong interpretation caveats. In both observed-coalition and ideology-sweep paths, =candidate_inversion= is exactly =seat_majority && !vote_majority=, so it is a national aggregate coalition/block majority condition rather than an individual-candidate or district-level phenomenon. Positive =seat_diff=, seat majority, and seat-majority-without-vote-majority are distinct: for example, 2018 ideology sweeps have 19 positive-seat-diff rows and 20 seat-majority rows but no inversions. No final inversion disappears after the audited corrections; the corrected vote-column handling adds the 2022 ideology-sweep =MDB -> UNIÃO= inversion, and the required cabinet-to-election crosswalk creates the final 2014 observed inversions that a same-label negative control would miss. Final observed-coalition inversions are 2014 =2016.2=, 2014 =2017.1=, and 2022 =2023.1=; final ideology-sweep inversions are 2014 starts =PSB=, =PV=, =PTB=, =PT DO B=, =SOLIDARIEDADE= and 2022 starts =MDB=, =PROS=, =PRTB=, =AGIR=, =PTB=, =PP=. The national aggregation level remains a methodological question outside what the script itself can settle.

* Questions for interpretation
These questions are worth answering before treating the output as a substantive claim about representation, coalition politics, or ideology.

- [X] Ask whether post-election cabinet coalitions are the right object for diagnosing election-year disproportionality.
  - well, this is the contribution of the paper. Not an overall take about disproportionality, but whether the ruling coalition reflects, mathematically, the votes etc. 
- [X] Ask whether a national seat-differential framework hides state-level mechanisms that actually generate the observed pattern.
  - this is a reasonable take. State level mechanisms is precisely what is creating that. I have no doubt. But that can be the next step in the analysis, not a fix for now. 
- [ ] Ask whether 2025 ideology placements are an acceptable proxy for the 2022 electoral party system.
  - reasonable. We can use the 2023 classification for previous years, and the new classification for post bolsonaro ? 
- [X] Ask whether parties with nonzero votes but zero seats should enter coalition and sweep logic exactly as the script currently counts them.
  - yes, they should. I believe it is ok as it is. 
- [X] Ask whether the script's majority logic should be identical across party, observed coalition, and ideology sweep analyses.
  - It should. That is the whole point. 

* Refactor candidates / next code changes
Several risks visible in the script come from local overrides and repeated logic. Even if the current run succeeds, those are prime targets for cleanup.

- [x] Move durable identity fixes out of =test_running.jl= and into shared alias or crosswalk data where possible.
- [x] Add a preflight check that all required files exist before any heavy analysis begins.
- [x] Add diagnostics that print raw -> normalized -> canonical party mappings for votes, seats, coalitions, and ideology inputs.
- [x] Add a consistency check comparing component votes against total votes when both are available.
- [x] Make the cabinet-to-election translation step report expansions and passthroughs explicitly.
- [x] Consider making =expected_total_seats= configurable or validated from metadata instead of hard-coding 513.
- [x] Reduce repeated year-specific print blocks by iterating through years and emitting the same diagnostics systematically.

  Finding: =processing/Processing/exploratory/audit/findings/refactor_candidates.md=.
  Output: =processing/Processing/exploratory/audit/out/preflight_checks.csv=, =party_mapping_votes.csv=, =party_mapping_seats.csv=, =party_mapping_coalitions.csv=, =party_mapping_ideology.csv=, =vote_column_consistency.csv=, =cabinet_translation_report.csv=, and =test_running_stdout_after_refactor.txt=.
  Result: completed. Durable identity mappings for 2014 =PATRIOTA -> PEN= and 2022 =AGIR -> AGIR=, =CDD -> CIDADANIA=, and =PROGRE -> PP= now live in =party_aliases.csv=. The script now preflights required paths, emits raw -> normalized -> canonical diagnostics for all four party-input domains, reports vote component choices and component consistency, explicitly classifies cabinet-to-election mappings as crosswalk rename/passthrough, label passthrough, or expansion, validates expected seats from shared metadata with an =EXPECTED_TOTAL_SEATS= override, and iterates through audited years instead of using repeated year-specific print blocks. The remaining local 2014 ideology label overrides are methodological predecessor/successor translations from a 2023 ideology source into 2014 election space, not general aliases. The main post-refactor run completed successfully.

* Likely failure modes to investigate first
These are the most plausible substantive problems visible from the script design. Investigate them before spending time on fine-grained interpretation of any inversion result.

- [x] Alias and label mismatches across election files, coalition JSON, ideology sources, and the cabinet-to-election crosswalk.
- [x] Parties dropped from ideology ordering because translation fails, because =ideology_drop_labels= removes them, or because the source classification year is a poor historical fit.
- [x] Manual overrides in =test_running.jl= masking upstream data-hygiene problems that should be fixed in shared normalization code.
- [x] Coalition-year versus election-year translation mistakes, especially where a cabinet party expands into multiple election parties.
- [x] Vote-column detection selecting a total-vote field that is not actually the same concept as valid legislative votes.
- [x] Winner-status exact matching missing legitimate elected records and distorting party seat totals.
- [x] The hard-coded =expected_total_seats = 513= hiding edge cases when filtering or winner logic is incomplete.
- [x] The ideology sweep detecting artifacts of ordering and the =first seat majority= stopping rule rather than politically meaningful coalitions.

  Finding: =processing/Processing/exploratory/audit/findings/likely_failure_modes.md=.
  Result: closed for the current refactored run. All vote, seat, coalition, and ideology input mappings now resolve through shared aliases; durable local overrides were moved into =party_aliases.csv=; coalition translation reports no unmapped rows and no current one-to-many expansions; vote-column selection now uses the intended valid-vote component fields; winner-status and seat-total checks still validate 513 seats in all three years; and the hard-coded seat expectation in =test_running.jl= was replaced by shared metadata with an explicit environment override. Remaining caveats are methodological rather than hidden code-path failures: 2014 ideology predecessor/successor translation, 2022 use of the 2025 ideology source, and the ideology sweep's constrained contiguous-block / first-seat-majority design.
