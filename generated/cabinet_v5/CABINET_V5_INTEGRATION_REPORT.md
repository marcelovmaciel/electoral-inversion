# Cabinet V5 integration report

1. Source release: `cabinet_dataset/releases/2026-03-19-history-v5-candidate`; metadata SHA-256 `f8762399c7b3ef29928d8b1c55a4873575c06a6529f698af31ff87e8eedea1b3`. Release files unchanged.
2. Transformation: released daily contemporaneous sets -> unchanged audited election-year crosswalk -> maximal analytical recompression -> existing electoral/decomposition pipeline plus independent exact validation.
3. Old periods: manuscript-reported baseline 23; preintegration paper-v1 generated baseline 33 identified periods (with unavailable calendar intervals). Both are preserved and compared.
4. New periods: 53 (from 55 historical periods); historical status counts {'established': 43, 'mixed': 8, 'provisional': 2}.
5. Manuscript old inversions: Dilma 2016.2 (2 days); Temer 2017.1 (100 days); Bolsonaro 2021.3/2022.1 (238 days); Lula 2023.1 (255 days). Preintegration generated inversions: Lula 2023.1 (123 days) and 2023.3 (7 days).
6. New primary inversions: CV5-005 / 2016.3 / dilma_2, [2016-04-14,2016-04-19), 5 days; CV5-041 / 2023.1 / lula_3, [2023-01-01,2023-09-13), 255 days.
7. Manuscript comparison: Dilma configuration survives, 2 -> 5 days; Lula configuration and 255 days survive; Temer and Bolsonaro configurations disappear. Against paper-v1 generated outputs, Dilma reappears and the two Lula episodes are covered by one continuous 255-day period. Inversion days by election: {'2014': 5, '2018': 0, '2022': 255}.
8. Provisional treatment: 100 daily no-additional-party assumptions, historical UNKNOWN preserved; 3,996 evidence-established days. No primary inversion contains a provisional day.
9. Sensitivities: 23 concrete records / 52 nonbaseline scenarios; 0 compressed comparisons change inversion classification. Nine unbounded affiliations are not supplied hypothetical parties. Sachsida alternative is absent.
10. Changed generated table assets: generated_interpretation.tex, party_size_diagnostics.tex, table_02_cabinet_inversion_tabular.tex, table_accounting_focal_cases.tex, table_accounting_gross_components.tex, table_all_inversion_decomposition.tex, table_appendix_cabinet_composition.tex, table_appendix_cabinet_interval_bridge.tex, table_cabinet_district_concentration.tex, table_case_component_extremes.tex, table_case_district_vectors.tex, table_case_party_district_extremes.tex, table_case_party_vectors.tex, table_coalition_party_component_extremes.tex, table_coalition_party_contributions.tex, table_inversion_case_registry.tex, table_inversion_party_contribution_extremes.tex, table_observed_inversion_decomposition.tex. Full dependency inventory includes CSVs, compatibility copies, report tables, contribution rows and bridge tables.
11. Regenerated cabinet figures: observed_coalition_timeline.pdf, cross_domain_components.pdf, inversion_decomposition_components.pdf, accounting_state_weighting_anatomy.pdf. Figure 5 Panel B numerical values are unchanged.
12. Appendix: full analytical chronology with established/mixed/provisional status and day counts; historical linkage table; party d/A/B contributions; district concentration; cabinet-to-ideology bridges for both universes.
13. Non-cabinet validation: 144 protected products/slices unchanged. Includes party/district election profiles, both ideological universes, k=0/k=1, ideological contributions and Figure 5 Panel B. Cabinet participation annotations in party-accounting files change by design; electoral fields do not.
14. Manuscript compilation: successful through latexmk in the canonical pipeline; ordinary manuscript prose and its provenance comments match the preintegration snapshot. Stale numerical/source references are recorded for later editing; technical warnings are in latex_build_warnings.txt.
15. PDF: `writing/submission_inversions_review/manuscript/main_rw_again.pdf`.
16. Analytical handoff: `generated/cabinet_v5/`.
17. Upload ZIP: `handoff/cabinet_v5_analysis_handoff.zip`.
18. Manuscript change map: `CABINET_V5_MANUSCRIPT_MODIFICATIONS.org`; 64 dependency/provenance checklist entries; no replacement prose.
19. Exact command: `JULIA_BIN=processing/julia_paper_runtime.sh processing/rebuild_manuscript.sh --freeze-prose`.

Other cabinet-wide results: 53/53 A_C positive; 19 B_C positive; 50 R_C>1; 3 R_C<1.

Preservation: `audit/cabinet_v5_before/` retains the pre-task PDF, generated products, source files, working-tree diff and hash manifest. Existing unrelated changes were preserved. No commit or push was made.
