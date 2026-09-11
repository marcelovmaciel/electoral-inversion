# Cabinet history v5 completion report

FULL PRIMARY COVERAGE — FLAGGED PROVISIONAL ASSUMPTIONS; HISTORICALLY INCOMPLETE CANDIDATE

V4 began with **3,996 historically established days and 100 unidentified days**. V5 supplies primary cabinet party sets for **4,096/4,096 days**: **3,996 historically established** and **100 explicitly provisional**. **0 primary dates remain unfilled.** Historical affiliation evidence remains unresolved on 100 dates.

Version: `2026-03-19-history-v5-candidate`. Calendar: `[2015-01-01,2026-03-20)`. The duplicate `sdf.zip` matched all 47 v4 release files; initial working inputs and code matched the original package; the baseline rebuilt identically and passed 24 tests. V4 and its package remain unchanged.

The user subsequently authorized full primary coverage using flagged assumptions wherever targeted historical research still left a gap. [primary_assumptions.csv](primary_assumptions.csv) implements that instruction separately from the historical records: each residual officeholder is provisionally assumed to contribute no additional party (assumed UNAFFILIATED). **The underlying affiliation remains UNKNOWN.** The choice does not claim evidence of non-affiliation, does not bound the actual alternatives, and does not infer membership from employment or a partial-list absence. Every represented party still has an actual source-backed officeholder witness.

| Administration | Days | V4 established | V5 established | V5 provisional | Primary unfilled |
|---|---:|---:|---:|---:|---:|
| dilma_2 | 497 | 497 | 497 | 0 | 0 |
| temer | 964 | 886 | 886 | 78 | 0 |
| bolsonaro | 1461 | 1439 | 1439 | 22 | 0 |
| lula_3 | 1174 | 1174 | 1174 | 0 | 0 |

**Newly resolved by historical evidence: 0 v4 days. Filled provisionally: 100. Changed primary sets on previously established days: 0. Historical regressions: 0.** [v4_daily_comparison.csv](v4_daily_comparison.csv) and [v4_interval_resolutions.csv](v4_interval_resolutions.csv) account for the entire 100-day starting hole.

| V4 interval (half-open) | Dependencies | Days | Evidence-resolved | Provisional | Primary unfilled |
|---|---|---:|---:|---:|---:|
| [2016-11-25, 2017-02-03) | Ivani dos Santos | 70 | 0 | 70 | 0 |
| [2017-08-02, 2017-08-03) | Ivani dos Santos | 1 | 0 | 1 | 0 |
| [2017-10-20, 2017-10-26) | Ivani dos Santos | 6 | 0 | 6 | 0 |
| [2018-12-31, 2019-01-01) | Carlos Henrique Menezes Sobral | 1 | 0 | 1 | 0 |
| [2020-02-14, 2020-02-18) | Marcos Paulo Cardoso Coelho da Silva | 4 | 0 | 4 | 0 |
| [2020-04-24, 2020-04-28) | Luiz Pontel de Souza | 4 | 0 | 4 | 0 |
| [2020-04-28, 2020-04-29) | Luiz Pontel de Souza; Renato de Lima França | 1 | 0 | 1 | 0 |
| [2021-03-30, 2021-03-31) | Jonathas Assunção Salvador Nery de Castro; Otávio Brandelli; Sérgio José Pereira | 1 | 0 | 1 | 0 |
| [2021-03-31, 2021-04-01) | Sérgio José Pereira | 1 | 0 | 1 | 0 |
| [2022-12-21, 2022-12-30) | Maria Estella Dantas Antonichelli | 9 | 0 | 9 | 0 |
| [2022-12-30, 2023-01-01) | Maria Estella Dantas Antonichelli; Jonathas Assunção Salvador Nery de Castro | 2 | 0 | 2 | 0 |

Targeted implementation outcomes:

- **Ivani dos Santos: unresolved; new channels documented.** Added dated SGIP3 governance checks (531 returned entries), registry-scope documentation and a compatible first-person biography; retained all three UNKNOWN spells and all service dates. Residual: 77 days still require a person-specific historical affiliation/non-affiliation observation. Evidence: `V5W1E001;V5W1E002;V5W1E003`; decisions: `V5W1D001`.
- **Carlos Henrique Menezes Sobral, December 31 2018: service confirmed; historical affiliation unresolved.** Original January 1 DOU and vacancy succession rule support the existing December 31 service. Added historical governing-body and exact-identity/name registry checks; retained the UNKNOWN affiliation and all service dates. Residual: The 2023 MDB label is not backprojected; primary day is now separately filled by a flagged assumption. Evidence: `V5W2E001;V5W2E002;V5W2E003`; decisions: `V5W2D001;V5W2D002`.
- **Marcos Paulo, Luiz Pontel and Renato de Lima França (2020): three personal affiliations remain unresolved.** Added original personnel/association documents and new scoped historical-registry checks; preserved all three UNKNOWN spells, service dates and actual vacancy classifications. Residual: The new records do not state personal party membership/non-membership. Partial-list absence and the identity-unconfirmed Renato PT namesake cannot resolve the four Casa Civil days or five Justice days (including the AGU overlap). Evidence: `V5W3E001;V5W3E002;V5W3E003`; decisions: `V5W3D001;V5W3D002;V5W3D003`.
- **Jonathas, Otávio Brandelli and Sérgio José Pereira, March 2021: historical affiliations unresolved; original personnel sources added.** Added original CMB/Senate dossiers, the qualified Terracap discovery and scoped registry checks; retained exact vacancy dates and MRE outgoing UNKNOWN sensitivity. Combined both independent Jonathas research trails. Residual: The two calendar days require provisional primary assumptions. Generic eligibility and unspecified negative TSE certificates do not establish non-affiliation. Evidence: `V5W4E001;V5W4E002;V5W4E003`; decisions: `V5W4D001;V5W4D002;V5W4D003`.
- **Helder Melillo Lopes Cunha Silva: registry reason resolved; historical effect still bounded by competing interpretations.** Official TSE client identifies departure reason 2 as a request by the voter and distinguishes entered departure date from registration/effectuation. Raw fields, NOVO primary and UNAFFILIATED alternative retained. Residual: The historical court-notice/effective-termination date is not established; two days remain set-sensitive. Evidence: `V5W5E001`; decisions: `V5W5D001`.
- **Jonathas Assunção Salvador Nery de Castro, December 2022: unresolved; original issuer disclosure inspected.** Added the original Petrobras 2022 reference filing and retained UNKNOWN; corporate independence and PEP disclosures are not a personal party declaration. Residual: December 30-31 affiliation remains unknown; this evidence does not establish March 2021 status. Evidence: `V5W5E004`; decisions: `V5W5D004`.
- **Maria Estella Dantas Antonichelli: service capacity corrected; affiliation unresolved.** Corrected titular to vacancy_acting for [2022-12-21,2023-01-01), using the official roll, substitute rule and official acts; source dates unchanged. Reviewed original board eligibility dossiers without treating eligibility as non-affiliation. Residual: All 11 days remain blocked by personal affiliation. Evidence: `V5W5E002;V5W5E003`; decisions: `V5W5D002;V5W5D003`.
- **Damares Alves: departure window narrowed; entry membership corroborated.** BBC September 3, 2020 personal PP observation narrows departure from August 27-September 19 (24 candidates) to September 4-19 (16 candidates); unchanged primary September 19. This reduces set-sensitive days from 23 to 15. Official registry corroborates the existing March 28, 2022 Republicanos entry. Residual: Exact PP departure day remains unresolved; current positive report takes precedence over weaker retrospective years. Evidence: `V5W6E03;V5W6E07;V5W6E09;V5W6E10`; decisions: `V5W6D02`.
- **Rogério Marinho: departure window narrowed.** Narrowed June 2020 PSDB departure from February 12-June 18 (128 candidate dates) to June 16-18 (3 candidates); unchanged primary June 18. The admissible window can change sets on 2 days, previously 127. Residual: Exact exit day still lacks a definitive registry event; competing older reports are retained and adjudicated. Evidence: `V5W6E04;V5W6E05;V5W6E06;V5W6E08;V5W6E09;V5W6E10`; decisions: `V5W6D03`.
- **Adolfo Sachsida: formal successor membership supported; alternative removed.** Official TSE historical May 27, 2013 entry matches the exact TCU identity and MME minister roll, with regular status and no recorded departure/pending fields. Retained UNIAO throughout [2022-05-11,2023-01-01) and removed the 235-day UNAFFILIATED sensitivity; conflicting source assertions remain archived. Residual: Historical registry continuity is an adjudication from a dated record, not a contemporaneous certificate for every day. No specific competing affiliation trajectory remains supported. Evidence: `V5W6E01;V5W6E02`; decisions: `V5W6D01`.
- **Ana Carla Machado Lopes PSDB affiliation: prior historical observation retained; no stronger affiliation evidence found.** Checked 27 historical PSDB governing bodies with 1,131 officer entries. Officer-list absence is not a full membership negative. Retained the 2016 observation and stated continuity over the 14 primary replacement days. Residual: The PSDB adjudication is not upgraded to a contemporary certificate; no contrary personal event or better exact-person observation was established. Evidence: `V5W7E005`; decisions: `V5W7D003`.
- **Sabino/Ana Carla, December 2023 return: date bound retained; retrospective report added.** A further December 6 retrospective confirms the December 4 return report; original parliamentary dispatch review did not resolve the December 5 vote/December 6 licence conflict. Retained December 4-6 bound and December 6 primary. Residual: Two dates remain set-sensitive; no primary service date was changed. Evidence: `V5W7E004`; decisions: `V5W7D001`.
- **Sabino/Ana Carla, November 2025 return: actual service and notification semantics strengthened.** Confirmed Ana Carla actual November 14 Orla activity and Sabino actual ministerial exercise by November 17 morning. Original Chamber dispatch explicitly times parliamentary notification at November 17 19:36, not ministerial assumption. Retained November 14-17 bound and November 17 primary. Residual: November 14 activity does not exclude a later same-day handoff; three dates remain set-sensitive. Evidence: `V5W7E001;V5W7E002;V5W7E003`; decisions: `V5W7D002`.
- **All residual primary gaps: 100 days filled with explicit provisional assumptions.** Added 12 separate assumption records across nine officeholders (105 overlapping person-days, 100 unique calendar days), choosing no additional party contribution. All historical UNKNOWN rows are preserved; all 4,096 dates now have primary sets with actual witnesses for every represented party. Residual: All 100 filled days remain historically uncertain and their unconstrained actual affiliations can change the sets. Historical completeness is not asserted. Evidence: not applicable (explicit modelling convention); decisions: `V5ROOTD002`.
- **Scope of date-bounded personal alternatives: derived sensitivity reporting corrected.** Residual-person output now respects the linked joint date constraint and actual service instead of treating date alternatives as independent choices throughout whole spells. Added alternative_scope and date_constraint_id. The same rule applies to all three existing bounded affiliation transitions. Residual: Independent membership alternatives retain their full evidenced scope. This reporting correction changes no primary party set, service rule or raw source alternative. Evidence: `AFF-024;AFF-013;AFF-028`; decisions: `R16;R17;BOL-A01`.

**Historical blockers retained: 9 people.** Maria Estella Dantas Antonichelli; Carlos Henrique Menezes Sobral; Ivani dos Santos; Jonathas Assunção Salvador Nery de Castro; Luiz Pontel de Souza; Marcos Paulo Cardoso Coelho da Silva; Otávio Brandelli; Renato de Lima França; Sérgio José Pereira.

[completion_blockers.csv](completion_blockers.csv) retains the exact missing facts, source trail and provisional-fill identifiers. [completion_research_register.csv](completion_research_register.csv) preserves the old and new research. The provisional assignments complete primary exports; they do not establish the unknown formal affiliations. Their unconstrained historical alternatives can change cabinet party sets. No global officeholding rule was changed.

Other residual set-changing sensitivities:

| Record | Effect | Potentially changing days | Parties |
|---|---|---:|---|
| transition-person-20d6d1c35408 | potential_set_change | 2 | PSDB |
| transition-person-87ad75f8d2f9 | potential_set_change | 15 | PP |
| mre-2021-effective-entry | unbounded_personal_alternative | 1 |  |
| sabino-2023-return | potential_set_change | 2 | PSDB |
| sabino-2025-nov-return | potential_set_change | 3 | PSDB;UNIAO |
| Rogério Marinho [2020-06-16, 2020-06-18) | primary_adjudication_with_set_changing_alternative | 2 | PSDB |
| Helder Melillo Lopes Cunha Silva [2022-12-30, 2023-01-01) | primary_adjudication_with_set_changing_alternative | 2 | NOVO |
| Damares Alves [2020-09-04, 2020-09-19) | primary_adjudication_with_set_changing_alternative | 15 | PP |

Sensitivity counts can overlap and must not be added. [residual_person_uncertainty.csv](residual_person_uncertainty.csv) and [date_sensitivity.csv](date_sensitivity.csv) preserve evidence-supported alternatives. Date-bounded alternatives are evaluated only within their linked date windows. Independent membership alternatives retain their own scope. A provisional no-additional-party choice is not evidence that the unknown alternatives are composition-neutral.

V5 has 55 maximal primary calendar segments and 50 membership-change boundaries. Evidence-quality changes do not split equal primary sets; atomic/daily rows preserve exact provisional dates, and periods record provisional_days. Transitions touching provisional dates carry assumption identifiers. The older [historical_comparison.csv](historical_comparison.csv), [unidentified_interval_resolutions.csv](unidentified_interval_resolutions.csv), and [previous_dependency_resolutions.csv](previous_dependency_resolutions.csv) retain their original comparison bases and do not count imputation as historical resolution.

Validation and reproduction:

- Verified v4 starting snapshot: **PASS**. 47 ZIP files matched v4; baseline rebuild byte-identical; 24 original tests passed.
- Full primary calendar: **PASS**. 4,096/4,096 dates; zero unfilled primary days; 100 dates carry explicit provisional assumptions.
- Historical completeness: **FAIL**. 100 dates across nine people remain historically unresolved; --require-complete intentionally fails.
- Test suite: **PASS**. 31 tests passed, including all original guarantees plus assumption and sensitivity regressions.
- Independent unions and witnesses: **PASS**. All 4,096 daily unions and 73,858 actual witness-days independently verified, including transition differences.
- Scope preservation: **PASS**. 924 pre-existing files outside cabinet_dataset remain byte-identical; no electoral/manuscript/consumer changes.
- Original v4 release and package: **PASS**. All original 47 release files and the v4 package retain their initial hashes.
- Clean v5 package reproduction: **PASS**. Clean extracted v5 package reproduced all 53 release files byte-for-byte; package manifest hashes and all 31 tests passed.
- Preserved v4 reproduction from v5 package: **PASS**. The same extracted package reproduced all 47 original v4 release files byte-for-byte; its preserved historical validator passed structural/provenance checks.

From the repository or extracted package root (Python 3.10+, standard library, offline):

```bash
python3 cabinet_dataset/build.py --output /tmp/cabinet-history-v5-rebuild
python3 -m unittest discover -s cabinet_dataset/tests -v
python3 cabinet_dataset/validate.py --release /tmp/cabinet-history-v5-rebuild --require-primary-coverage
python3 cabinet_dataset/validate.py --release /tmp/cabinet-history-v5-rebuild --require-complete
```

The historical `--require-complete` check remains an honest failure (100 unresolved historical dates). The separate `--require-primary-coverage` check tests the requested complete primary export, including its explicit provisional assumptions. No historical validator condition was weakened.

Release: `cabinet_dataset/releases/2026-03-19-history-v5-candidate/`. Package: `cabinet_dataset/packages/2026-03-19-history-v5-candidate.tar.gz`. The package retains inputs, source snapshots, worker handoffs, merge records and preserved v4 reproduction inputs. It rebuilds offline. Electoral calculations, the manuscript, consumer pin, commits and pushes remain outside this work.
