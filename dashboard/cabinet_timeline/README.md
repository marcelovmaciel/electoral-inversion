# Cabinet Timeline Audit

This dashboard is a thin audit layer over the cabinet reconstruction pipeline in [`scraping/reconstruct_cabinet_timeline.py`](/home/marcelovmaciel/Sync/Projects/electoral_inversions/scraping/reconstruct_cabinet_timeline.py).

## What it loads

- Raw event log: [`scraping/output/ministerios_eventos.json`](/home/marcelovmaciel/Sync/Projects/electoral_inversions/scraping/output/ministerios_eventos.json)
- Dashboard data: [`scraping/output/cabinet_timeline_dashboard.json`](/home/marcelovmaciel/Sync/Projects/electoral_inversions/scraping/output/cabinet_timeline_dashboard.json)
- Review report: [`scraping/output/ministerios_eventos_review_report.md`](/home/marcelovmaciel/Sync/Projects/electoral_inversions/scraping/output/ministerios_eventos_review_report.md)

## Reconstruction assumptions

- The pipeline reads the cabinet tables from the four Wikipedia cabinet pages already used in the repository.
- `interino` and similar wording are preserved as temporary assumptions instead of being flattened into permanent replacements.
- Structural events are only inferred when the row itself explicitly mentions creation, transformation, incorporation, or extinction.
- Ambiguous date rows are kept in the raw event output with `needs_review=true`; they are not forced into dashboard intervals.
- The dashboard clips intervals to cabinet/government windows so carryovers do not create duplicate visual spans across cabinet pages.

## Known limitations

- Cross-government institutional continuity is intentionally conservative. Similar ministry names are not aggressively merged across renames, mergers, or splits.
- Some Wikipedia rows are internally ambiguous or incomplete, especially interim rows with missing bounds or multiple date mentions.
- The dashboard is static and audit-oriented. It is for inspection, not publication.

## Regenerate outputs

From the repository root:

```bash
python scraping/reconstruct_cabinet_timeline.py
```

## Serve the dashboard

Serve the repository root so the dashboard can fetch the generated JSON with a relative path:

```bash
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/dashboard/cabinet_timeline/
```
