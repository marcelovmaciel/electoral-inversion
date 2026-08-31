# PSC cabinet audit: Gilson Machado Neto

## Adjudication

The raw field `PSC / PL` is a temporal sequence, not simultaneous membership and not an unresolved alternative. The repaired daily chronology is:

| Date interval | Office status | Party used in the cabinet |
|---|---|---|
| 2020-04-09 to 2020-12-08 | before Tourism appointment | PSC (background evidence only; not added to a cabinet) |
| 2020-12-09 to 2022-03-29 | Minister of Tourism | PSC |
| 2022-03-30 | Minister of Tourism, final day | PL |
| from 2022-03-31 | no longer minister; Carlos Brito begins | no Gilson-derived party |

All dates in the cabinet reconstruction are inclusive civil dates. The dated affiliation table therefore uses PSC through 29 March and PL from 30 March. The press report places the PL signature on the night of 30 March; because the repository has day rather than time resolution, the formal change date owns the whole day. The official responsibility roll places Gilson's final office day on 30 March and Carlos Brito's first day on 31 March.

## Source row

Pinned Wikipedia source revision: `71536920`.

- Source page: [Lista de membros do gabinete de Jair Bolsonaro](https://pt.wikipedia.org/w/index.php?oldid=71536920)
- Parser locator: page `jair_bolsonaro`, table 0, row 59
- Stable record key: `jair_bolsonaro:0:59`
- Ministry: `Ministério do Turismo`
- Person: `Gilson Machado Neto`
- Raw row: `Ministério do Turismo | Gilson Machado Neto | PSC / PL | 9 de dezembro de 2020 | 31 de março de 2022`

The raw end date is retained as `source_actual_end=2022-03-31`. The authoritative appointment interval is corrected to end on 2022-03-30 because the government roll identifies 31 March as Carlos Brito's first day.

## Dated evidence

Evidence was checked on 28 August 2026. Quotes below are deliberately short; the factual role of each source is stated separately.

| Publication / record date | Source | Factual claim and temporal role |
|---|---|---|
| 2020-04-14 (reports the affiliation event of 2020-04-09) | [Jornal do Commercio](https://jc.uol.com.br/politica/2020/04/5605995-recem-chegado-na-oposicao-a-paulo-camara--alberto-feitosa-reafirma-defesa-do-governo-bolsonaro.html) | Reports that Bolsonaro's Pernambuco group joined PSC and identifies then-Embratur president Gilson Machado in that group. This establishes PSC before the ministerial appointment. |
| 2022-01-07 13:30 | [Veja, Radar](https://veja.abril.com.br/coluna/radar/por-que-gilson-machado-so-vai-para-o-pl-se-bolsonaro-mandar/) | “Filiado ao PSC desde 2020”; it also says he would migrate to PL only if Bolsonaro directed it. This establishes continued PSC membership during the ministry in January 2022 and rules out PL for the whole spell. |
| 2022-03-30 23:19 | [Correio Braziliense](https://www.correiobraziliense.com.br/politica/2022/03/4997151-ministro-do-turismo-deixa-psc-e-assina-carta-de-filiacao-ao-pl.html) | Reports that he signed the PL letter on the night of 30 March and “migrou do PSC”. This fixes the formal PSC-to-PL transition date. The article's weekday word is inconsistent with its numeric date, publication timestamp, and surrounding chronology; the numeric date controls. |
| decree dated 2020-12-09, DOU 2020-12-10 | [Official appointment decree mirror](https://sintse.tse.jus.br/documentos/2020/Dez/10/diario-oficial-da-uniao-secao-2/decreto-de-9-de-dezembro-de-2020-nomeia-adenir-teixeira-peres-junior-para-compor-o-tribunal-regional) | Names Gilson Machado Neto Minister of Tourism. The repository convention uses the decree/possession date, 9 December, as the inclusive start. |
| decree dated 2022-03-30, DOU 2022-03-31 | [Diário Oficial da União](https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?captchafield=firstAccess&data=31%2F03%2F2022&jornal=529&pagina=2) | Exonerates Gilson and names Carlos Alberto Gomes de Brito. |
| official administrative roll, consulted 2026-08-28 | [Federal government responsibility roll](https://www.gov.br/cultura/pt-br/acesso-a-informacao/auditorias/RoldeResponsveis.pdf) | Records Gilson through 30 March 2022 and Carlos Brito from 31 March 2022. This resolves the one-day ambiguity in the Wikipedia end field. |

The evidence chain does not rely on Gilson's later 2022 candidacy to infer his party at appointment. It uses evidence dated inside the ministerial spell and the formal switch report.

## Old behavior

`parse_party_field` correctly preserved the two tokens `PSC` and `PL`, but `resolve_start_party` treated the row as an unresolved start-party ambiguity. The dashboard therefore emitted:

- `party_candidates=[PSC, PL]`;
- `party_codes=[]`;
- `resolved_party=null`;
- `needs_review=true`.

Because only resolved appointment parties contributed to contemporaneous party sets, neither token contributed through this row. This omitted PSC from every affected Bolsonaro cabinet period.

The older direct Wikipedia scraper is not authoritative for this repair: it splits slash-separated tokens and would apply both PSC and PL simultaneously for the whole appointment, which is also historically wrong.

## Corrected source-to-analysis path

1. `scraping/data/cabinet_source_snapshot.json` pins the four source revisions and the existing 2026-03-19 analysis cutoff.
2. `scraping/data/cabinet_party_affiliation_spells.csv` stores the dated PSC and PL spells, their evidence, inclusive boundary convention, and the official end-date override.
3. `extract_records` parses the raw row and `apply_affiliation_record_correction` validates that the two dated spells are contiguous, cover the corrected appointment exactly, and use only tokens present in the raw field.
4. `resolve_start_party` classifies the row as `dated_affiliation_spells`, with PSC at appointment.
5. `party_slices_for_interval` emits two non-overlapping appointment spells rather than one simultaneous or collapsed party assignment.
6. `build_party_periods` derives daily contemporaneous party sets from all resolved appointments, applies the DEM/PSL-to-UNIÃO lineage on 2022-02-08, coalesces adjacent identical sets, and validates gap-free coverage.
7. `write_party_periods` writes the authoritative `scraping/output/partidos_por_periodo.csv` and `.json`.
8. Julia loads the JSON through `Processing.coalitions_by_period_raw`, translates cabinet labels into the linked election-year party space, and computes the unchanged paper metrics.

Relative to the preserved old source, exactly 476 daily party sets change: 2020-12-09 through 2022-03-29 inclusive. Every daily set difference is exactly `+PSC`; no other party changes.

## Regression protection

- `python -m unittest discover -s scraping/tests -p 'test_*.py' -v`
  - protects the raw multi-party sequence, official end correction, exact two spells, single-party codes, and PSC period coverage.
- `processing/Processing/test/test_psc_baseline_repair.jl`
  - protects PSC coverage from 2020-12-09 through 2022-03-29, its absence from 2022.2, and PL's presence on that post-transition period.

The source reconstruction is deterministic: reruns use immutable Wikipedia revisions, a fixed source cutoff, and a fixed artifact timestamp.
