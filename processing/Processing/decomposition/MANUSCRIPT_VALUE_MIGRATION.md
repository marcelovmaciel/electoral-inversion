# Manuscript-value migration audit

Audited against `main_rw_again.tex` and repository state before this refactor.
Commit `f27cf5c3775ab4ccf03d8ef4573b173d67efce80` introduced
`AccountingIntegration.jl`, its `accounting_numeric_macros(integration)` writer,
and the generated prose interface. `write_accounting_integration_outputs`
serialized that file; `run_decomposition.jl` copied it to the paper and review
manuscript trees. Packaging followed the manuscript's `\input` list.
The file was generated, not a manually maintained source of numerical results.
The seat-winning ideological universe and all-valid-vote denominator from that
commit are preserved.

Of 118 legacy definitions, the manuscript referenced these 20 distinct macros.
There were no referenced aggregate, cabinet, party/state diagnostic, or other
legacy macros. The unused definitions are deliberately retired. Fixed-case
selection now asserts complete membership. Dynamic k=1 claims that formerly
reused k=0 values have separate k-specific selections, labels, and numbers.

| Legacy macro | Classification | Replacement |
|---|---|---|
| `\AcctIdeologicalTwentyEighteenCaseOneBetween` | Fixed substantive case: PT--PSDB | `\IdeologySeatWinningTwentyEighteenKZeroPTPSDBBetween` |
| `\AcctIdeologicalTwentyEighteenCaseOneDifferential` | Fixed substantive case: PT--PSDB | `\IdeologySeatWinningTwentyEighteenKZeroPTPSDBDifferential` |
| `\AcctIdeologicalTwentyEighteenCaseOneQuota` | Fixed substantive case: PT--PSDB | `\IdeologySeatWinningTwentyEighteenKZeroPTPSDBQuota` |
| `\AcctIdeologicalTwentyEighteenCaseOneVotePct` | Fixed substantive case: PT--PSDB | `\IdeologySeatWinningTwentyEighteenKZeroPTPSDBVotePct` |
| `\AcctIdeologicalTwentyEighteenCaseOneWithin` | Fixed substantive case: PT--PSDB | `\IdeologySeatWinningTwentyEighteenKZeroPTPSDBWithin` |
| `\AcctIdeologicalTwentyFourteenCaseOneDifferential` | Dynamic selection: strongest 2014 minimal inversion | `\IdeologySeatWinningTwentyFourteenKZeroStrongestDifferential` |
| `\AcctIdeologicalTwentyFourteenCaseOneQuota` | Dynamic selection: strongest 2014 minimal inversion | `\IdeologySeatWinningTwentyFourteenKZeroStrongestQuota` |
| `\AcctIdeologicalTwentyFourteenCaseOneVotePct` | Dynamic selection: strongest 2014 minimal inversion | `\IdeologySeatWinningTwentyFourteenKZeroStrongestVotePct` |
| `\AcctIdeologicalTwentyTwentyTwoCaseOneBetween` | Fixed substantive case: MDB--UNIÃO | `\IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniaoBetween` |
| `\AcctIdeologicalTwentyTwentyTwoCaseOneDifferential` | Fixed substantive case: MDB--UNIÃO | `\IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniaoDifferential` |
| `\AcctIdeologicalTwentyTwentyTwoCaseOneQuota` | Fixed substantive case: MDB--UNIÃO | `\IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniaoQuota` |
| `\AcctIdeologicalTwentyTwentyTwoCaseOneRequired` | Fixed substantive case: MDB--UNIÃO | `\IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniaoRequired` |
| `\AcctIdeologicalTwentyTwentyTwoCaseOneVotePct` | Fixed substantive case: MDB--UNIÃO | `\IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniaoVotePct` |
| `\AcctIdeologicalTwentyTwentyTwoCaseOneWithin` | Fixed substantive case: MDB--UNIÃO | `\IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniaoWithin` |
| `\AcctIdeologicalTwentyTwentyTwoCaseTwoBetween` | Fixed substantive case: PP--PL | `\IdeologySeatWinningTwentyTwentyTwoKZeroPPPLBetween` |
| `\AcctIdeologicalTwentyTwentyTwoCaseTwoDifferential` | Fixed substantive case: PP--PL | `\IdeologySeatWinningTwentyTwentyTwoKZeroPPPLDifferential` |
| `\AcctIdeologicalTwentyTwentyTwoCaseTwoQuota` | Fixed substantive case: PP--PL | `\IdeologySeatWinningTwentyTwentyTwoKZeroPPPLQuota` |
| `\AcctIdeologicalTwentyTwentyTwoCaseTwoRequired` | Fixed substantive case: PP--PL | `\IdeologySeatWinningTwentyTwentyTwoKZeroPPPLRequired` |
| `\AcctIdeologicalTwentyTwentyTwoCaseTwoVotePct` | Fixed substantive case: PP--PL | `\IdeologySeatWinningTwentyTwentyTwoKZeroPPPLVotePct` |
| `\AcctIdeologicalTwentyTwentyTwoCaseTwoWithin` | Fixed substantive case: PP--PL | `\IdeologySeatWinningTwentyTwentyTwoKZeroPPPLWithin` |

The 2014 k=0 selection uses minimum vote share, then member count and canonical
membership ID. This is the existing focal rule (minimum `v_C` within an election)
and summary rule (maximum `257-q_C`) stated directly. Dynamic fields within a
group are extracted from one selected row, and associated full-span and omitted
party values follow that same winner.

Production empirical guards removed from AccountingIntegration:

- `BASELINE_CASE_EXPECTATIONS`: four historical cabinet decompositions.
- `BASELINE_PARTY_EXPECTATIONS`: ten historical case/party decompositions.
- Literal expectations of four cabinet vectors and 33 cabinet member rows.

These regressions now live in `test_accounting_integration.jl`. The unused
`AcctUniqueCabinetVectorCount` literal `4` disappeared with the legacy writer;
the new cabinet inversion count derives from the current cabinet accounting
objects. Selected-party (17) and district-panel (81) dimension checks now derive
from the selected-party specification and analyzed election panel respectively.
Mathematical and structural invariants remain in production.

The specification additionally covers literal numerical prose: party
statistics, cabinet periods and transitions, duration and sign counts,
ideological domain summaries, both-universe robustness comparisons,
party-size diagnostics, district summaries, and cabinet/interval comparisons.
The cabinet district concentration table is an independently generated table,
not a collection of scalar registry entries.

`ManuscriptValues.jl` holds the public specification;
`ManuscriptValueSupport.jl` implements selection, formatting, assertions, and
serialization. Neither reads an existing TeX value file. Both serialized outputs
come from the same registry. The CSV stores exact rationals where the source
supplies them, alongside unrounded raw and declared-format display values.

## Semantic macro groups

Each prefix below plus its explicitly listed field suffixes in `ManuscriptValues.jl`
is a semantic macro group. The CSV gives a row for every complete macro.

| Prefix | Manuscript claim |
|---|---|
| `IdeologySeatWinningTwentyFourteenKZeroStrongest` | 2014 seat_winning k=0 strongest minimal inversion |
| `IdeologySeatWinningTwentyEighteenKZeroPTPSDB` | 2018 seat_winning k=0 PTPSDB substantive interval |
| `IdeologySeatWinningTwentyTwentyTwoKZeroMDBUniao` | 2022 seat_winning k=0 MDBUniao substantive interval |
| `IdeologySeatWinningTwentyTwentyTwoKZeroPPPL` | 2022 seat_winning k=0 PPPL substantive interval |
| `IdeologySeatWinningTwentyFourteenKOneStrongest` | 2014 seat_winning k=1 strongest minimal inversion |
| `IdeologySeatWinningTwentyTwentyTwoKOneStrongest` | 2022 seat_winning k=1 strongest minimal inversion |
| `IdeologySeatWinningTwentyEighteenKOneStrongest` | 2018 seat_winning k=1 strongest minimal inversion |
| `IdeologyAllPartiesTwentyTwentyTwoKZeroMDBUniao` | 2022 all_parties k=0 MDBUniao substantive interval |
| `PartyTwentyFourteenPMDB` | 2014 PMDB party accounting |
| `PartyTwentyFourteenPSD` | 2014 PSD party accounting |
| `PartyTwentyFourteenPTB` | 2014 PTB party accounting |
| `PartyTwentyFourteenPSDB` | 2014 PSDB party accounting |
| `PartyTwentyFourteenPSOL` | 2014 PSOL party accounting |
| `PartyTwentyEighteenPP` | 2018 PP party accounting |
| `PartyTwentyEighteenPR` | 2018 PR party accounting |
| `PartyTwentyEighteenMDB` | 2018 MDB party accounting |
| `PartyTwentyEighteenPSL` | 2018 PSL party accounting |
| `PartyTwentyEighteenNOVO` | 2018 NOVO party accounting |
| `PartyTwentyTwentyTwoPL` | 2022 PL party accounting |
| `PartyTwentyTwentyTwoUniao` | 2022 UNIÃO party accounting |
| `PartyTwentyTwentyTwoPT` | 2022 PT party accounting |
| `PartyTwentyTwentyTwoPP` | 2022 PP party accounting |
| `PartyTwentyTwentyTwoPV` | 2022 PV party accounting |
| `IdeologySeatWinningTwentyTwentyTwoKZeroPTPP` | 2022 seat_winning k=0 PTPP substantive interval |
| `IdeologySeatWinningTwentyTwentyTwoKZeroPSBPSC` | 2022 seat_winning k=0 PSBPSC substantive interval |
| `CabinetTwentyFourteenDilma` | Dilma inverted cabinet cabinet/2014/2016.2 |
| `CabinetTwentyFourteenTemer` | Temer inverted cabinet cabinet/2014/2017.1 |
| `CabinetTwentyEighteenBolsonaro` | Bolsonaro inverted cabinet cabinet/2018/2021.3/2022.1 |
| `CabinetTwentyTwentyTwoLula` | Lula inverted cabinet cabinet/2022/2023.1 |
| `CabinetTwentyEighteenBolsonaroAfterPSC` | BolsonaroAfterPSC transition period |
| `CabinetTwentyTwentyTwoLulaExpanded` | LulaExpanded transition period |
| `IdeologySeatWinningTwentyFourteenKZeroSummary` | 2014 seat_winning k=0 domain summary |
| `IdeologySeatWinningTwentyEighteenKZeroSummary` | 2018 seat_winning k=0 domain summary |
| `IdeologySeatWinningTwentyTwentyTwoKZeroSummary` | 2022 seat_winning k=0 domain summary |
| `IdeologyAllPartiesTwentyFourteenKZeroSummary` | 2014 all_parties k=0 domain summary |
| `IdeologyAllPartiesTwentyEighteenKZeroSummary` | 2018 all_parties k=0 domain summary |
| `IdeologyAllPartiesTwentyTwentyTwoKZeroSummary` | 2022 all_parties k=0 domain summary |
| `IdeologySeatWinningTwentyFourteenKOneSummary` | 2014 seat_winning k=1 domain summary |
| `IdeologySeatWinningTwentyEighteenKOneSummary` | 2018 seat_winning k=1 domain summary |
| `IdeologySeatWinningTwentyTwentyTwoKOneSummary` | 2022 seat_winning k=1 domain summary |
| `IdeologySeatWinningKZeroTotals` | seat_winning k=0 totals across elections |
| `IdeologyAllPartiesKZeroTotals` | all_parties k=0 totals across elections |
| `IdeologyAllPartiesTwentyFourteenKOneSummary` | 2014 all_parties k=1 domain summary |
| `IdeologyAllPartiesTwentyEighteenKOneSummary` | 2018 all_parties k=1 domain summary |
| `IdeologyAllPartiesTwentyTwentyTwoKOneSummary` | 2022 all_parties k=1 domain summary |
| `IdeologySeatWinningKOneSurvivingConnected` | Connected minimal majorities still minimal under k=1 |
| `IdeologySeatWinningKZeroAllInversions` | All connected inversions, including nonminimal cases |
| `IdeologySeatWinningKOneNegativeWithin` | Minimal one-gap inversions with negative within-district component |
| `IdeologySeatWinningTwentyFourteenKOneNegativeWithin` | Minimal one-gap inversions with negative within-district component |
| `IdeologySeatWinningTwentyTwentyTwoKOneNegativeWithin` | Minimal one-gap inversions with negative within-district component |
| `IdeologyAllPartiesKZeroPositiveWithin` | All-party connected minimal inversions with positive within component |
| `IdeologySeatWinningTwentyFourteenKOnePT` | PT membership among 2014 primary one-gap minimal inversions |
| `IdeologySeatWinningTwentyTwentyTwoKOneMDBUniao` | Gapped minimal inversions spanning MDB--UNIÃO |
| `CabinetPeriods` | All observed cabinet periods |
| `CabinetInversions` | Observed inverted cabinet periods and distinct accounting vectors |
| `CabinetComponents` | District component signs across all cabinet periods |
| `CabinetTwentyFourteenDuration` | 2014 mandate coverage and inversion duration |
| `CabinetTwentyEighteenDuration` | 2018 mandate coverage and inversion duration |
| `CabinetTwentyTwentyTwoDuration` | 2022 mandate coverage and inversion duration |
| `PartyTwentyFourteenEverCabinet` | 2014 mean vote share by cabinet participation |
| `PartyTwentyFourteenNeverCabinet` | 2014 mean vote share by cabinet participation |
| `PartyTwentyFourteenLarge` | 2014 parties at the descriptive 5 percent benchmark |
| `PartyTwentyEighteenEverCabinet` | 2018 mean vote share by cabinet participation |
| `PartyTwentyEighteenNeverCabinet` | 2018 mean vote share by cabinet participation |
| `PartyTwentyEighteenLarge` | 2018 parties at the descriptive 5 percent benchmark |
| `PartyTwentyTwentyTwoEverCabinet` | 2022 mean vote share by cabinet participation |
| `PartyTwentyTwentyTwoNeverCabinet` | 2022 mean vote share by cabinet participation |
| `PartyTwentyTwentyTwoLarge` | 2022 parties at the descriptive 5 percent benchmark |
| `IdeologyAllPartiesTwentyEighteenKOneStrongest` | 2018 all_parties k=1 strongest minimal inversion |
| `CabinetTwentyFourteenDilmaBridge` | 2016.2 cabinet parliamentary closure and closest intervals |
| `CabinetTwentyFourteenTemerBridge` | 2017.1 cabinet parliamentary closure and closest intervals |
| `CabinetTwentyEighteenBolsonaroBridge` | 2021.3/2022.1 cabinet parliamentary closure and closest intervals |
| `CabinetTwentyTwentyTwoLulaBridge` | 2023.1 cabinet parliamentary closure and closest intervals |
| `CabinetTwentyFourteenTemerTransitionBridge` | 2016.4 cabinet parliamentary closure and closest intervals |
| `CabinetBridge` | Range of gaps in observed parliamentary cabinet closures |
| `DistrictMagnitude` | District magnitude distribution (2014 apportionment, unchanged across analyzed elections) |
| `DistrictEightSeat` | Districts at the statutory eight-seat floor (2014 apportionment) |
| `IdeologySeatWinningTwentyEighteenStrongestFullSpan` | Full k=0 span of the strongest 2018 seat-winning k=1 minimal inversion |
| `PartyTwentyEighteenStrongestOmission` | Omitted party in the strongest 2018 seat-winning k=1 minimal inversion |
| `IdeologySeatWinningTwentyFourteenKZeroOther` | 2014 connected inversions excluding the strongest |
| `IdeologySeatWinningTwentyFourteenKZeroNegativeBetween` | 2014 minimal connected inversions with negative between-district component |
| `CabinetDistrictConcentration` | Range of within-district concentration across inverted cabinet vectors |
| `PartyTwentyFourteenFragmentation` | 2014 effective party numbers: inverse sum of squared production vote/seat shares |
| `PartyTwentyEighteenFragmentation` | 2018 effective party numbers: inverse sum of squared production vote/seat shares |
| `PartyTwentyTwentyTwoFragmentation` | 2022 effective party numbers: inverse sum of squared production vote/seat shares |

## Validation of this migration

The pre-refactor PDF, generated tables, machine-readable outputs, and prose
macro values were saved outside the repository before editing. Comparisons use
those actual outputs, not regression constants as inputs.

- The final clean production build completed analysis, registry generation,
  synchronization, figures/tables, both PDF compilations, and submission archives.
- All 234 registry values are generated from 85 explicitly named selection groups.
  Every expanded manuscript prose value and table cell matches the saved source.
- All 35 final manuscript PDF pages render identically to the saved PDF at 75 dpi;
  extracted layout-preserving text is also identical.
- The initial regenerated analysis matched all 241 existing data/table assets
  byte-for-byte. The final clean run used Julia 1.12.7 with generic CPU code via
  the existing `JULIA_BIN` override. Its 231 byte-identical assets and ten remaining
  assets are semantically equivalent: differing numeric cells are roundoff of at
  most 7.105427357601002e-15, and the internal closure audit table differs only in
  signs on displayed zero residuals. No identities, membership, counts, exact
  accounting quantities, or manuscript displays changed. No output was edited
  to force agreement. The four legacy analysis-tree macro files were retired.
- The complete decomposition/accounting/manuscript test suite passed 1,260
  assertions, including 80 manuscript provenance and drift assertions.
- Ideological-domain tests passed 969 assertions; figure/table/audit/packaging
  tests passed all 18 tests. The full Processing suite passed 1,135 assertions
  with one pre-existing skipped/broken test.
- The independent ideological audit passed for 12 domains, 31,238 coalitions,
  475,290 member rows, and 23 cabinet bridge rows.
- Both analysis artifact manifests contain the registry CSV and generated TeX.
  The 22-file submission archive contains `manuscript_values.tex`, contains no
  `accounting_numeric_macros.tex`, and requires no Julia source files.

The existing Julia default and pinned project dependencies were not changed.
Three previously untracked cabinet-coalescing source/test/fixture dependencies
already required by the current package are included unchanged so a fresh
checkout can run the same production and test paths.
