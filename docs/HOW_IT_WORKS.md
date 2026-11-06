# How the Jharkhand Mini-UDDP works

This document explains the path from the three source files to the district tables in PostgreSQL. The project report, with the published numbers, is in `docs/PROJECT_REPORT.md`.

## 1. Pieces

```text
data/raw/*.csv
      |
      v
bronze.raw_demographic / raw_housing / raw_pension
      |
      v
silver.dim_district
silver.fact_district_demographics
silver.fact_district_housing
silver.fact_pension_beneficiaries
      |
      v
gold.mart_district_pension_outreach
gold.ml_district_features
```

`audit` sits beside those layers. It records the run, the file counts, quarantined rows, quality checks, and alerts.

| Path | Responsibility |
| --- | --- |
| `config/settings.yaml` | Database, source URLs, retry count, alert webhook |
| `docker-compose.yml` | PostgreSQL 16 on host port 5433 |
| `run_pipeline.py` | Command-line runner and optional repeating scheduler |
| `uddp/download.py` | Download or read the three CSV files |
| `uddp/db.py` | Connect, create schemas, load bronze, run SQL files |
| `uddp/cleaning.py` | Names, numbers, duplicates, validation, pension unpivot |
| `uddp/pipeline.py` | Order of the steps, batch id, failure handling |
| `sql/001_schemas.sql` | Tables |
| `sql/002_gold.sql` | District mart and feature table |
| `uddp/quality.py` | Checks and the markdown quality report |
| `dags/jharkhand_uddp_dag.py` | Optional Airflow wrapper around the same function |

Airflow is not required. The runner used for this project is `python run_pipeline.py`.

## 2. Start the database

```bash
docker compose up -d
pip install -r requirements.txt
python run_pipeline.py
```

Port 5432 was already taken on this machine, so Compose publishes PostgreSQL on **5433**. The database name is `jharkhand_uddp`, the user is `uddp`, and the local password is `uddp_local`. Environment variables `UDDP_DB_HOST`, `UDDP_DB_PORT`, `UDDP_DB_NAME`, `UDDP_DB_USER`, and `UDDP_DB_PASSWORD` override `config/settings.yaml`.

The pipeline waits until PostgreSQL accepts a connection, then runs `sql/001_schemas.sql`. Running that file again does not drop existing data. Tables are created with `IF NOT EXISTS`.

## 3. One run, step by step

`run_pipeline()` in `uddp/pipeline.py` does the following.

1. Create a batch id such as `20261106T170833Z`, unless `--batch-id` was passed.
2. Insert `audit.pipeline_run` with status `RUNNING`.
3. Refresh `silver.district_alias` from the fixed alias list.
4. Download the three files into `data/raw/`, unless `--skip-download` or `--from-bronze` is set.
5. Replace this batch’s rows in the three bronze tables.
6. Clean the bronze rows in Python and replace this batch’s silver rows.
7. Run `sql/002_gold.sql`, which fills `staging` for this batch only. The published gold tables are not changed here.
8. Run quality checks. Blocking failures are committed to audit with the report and an alert, and the published gold snapshot is left as it was.
9. If every blocking check passes and this run is allowed to publish, replace both gold tables in one transaction. A replay (`--from-bronze`) does not publish unless `--promote` is set.

Downloads are retried up to three times, with the wait doubling after each failure. The retry count and the first wait are in `config/settings.yaml`.

If a step throws, the run is marked `FAILED`. The alert hook appends a line to `logs/alerts.jsonl`, inserts `audit.alert_event`, and POSTs to `UDDP_ALERT_WEBHOOK_URL` when that value is set. An empty webhook does not call the network. The same hook runs when a quality check fails. Logs for every run go to `logs/pipeline.log`.

## 4. Download

Demographic and housing files are downloaded from the GitHub raw URLs in settings.

The NSAP page publishes a direct CSV link. That link returns HTTP 403 to this client. `download_pension` tries the direct link first. On 403 it calls the Open Government Data API used by the page preview (`api.data.gov.in`, resource `4a97c662-ef81-4c84-b0d2-c9607c1059ab`) and writes a CSV with the official headers:

`Sl. No.`, `Category`, `States/UTs`, `Old Age Pension Scheme`, `Widow Pension Scheme`, `Divyangjan /Disability Pension Scheme`

The saved file is the raw input for later runs. `--skip-download` reads whatever is already in `data/raw/` and does not call the network.

## 5. Bronze

Each bronze table has the same shape:

- `batch_id`
- `source_name`
- `source_file_name`
- `ingestion_timestamp`
- `source_row_number` (file row number, with the header as row 1)
- `raw_record` (JSONB, original column names and original text values)

Bronze stores the full downloaded file, including other states. The Jharkhand filter happens in silver. That keeps the raw extract intact and makes the filtered count easy to reconcile: 640 demographic rows in the file, 24 after the state filter.

Loading a batch deletes that batch’s rows and inserts them again. A second run with the same `--batch-id` does not double the row count. A run with a new batch id appends another copy in bronze and silver. Gold does not keep that history. It is replaced so the mart stays at one row per district.

## 6. Cleaning

`clean_datasets()` in `uddp/cleaning.py` is pure Python. The database load happens after it returns. The rules are:

**State filter.** A row is in scope when the normalized state name is `JHARKHAND`.

**Housing grain.** Only a district total is kept: tehsil code 0, `Rural/Urban = Total`, and an area name that starts with “District”. Any other Jharkhand housing row is an error and is left out of silver.

**Names.** Text is upper-cased, punctuation becomes spaces, and a leading “District” token is removed. These aliases are applied explicitly:

| Incoming name | Canonical name |
| --- | --- |
| Hazaribag | Hazaribagh |
| East Singhbhum | Purbi Singhbhum |
| West Singhbhum | Pashchimi Singhbhum |
| Saraikela, Seraikela | Saraikela-Kharsawan |
| Koderma | Kodarma |
| Sahebganj | Sahibganj |

Match order for a housing row is district code, then canonical name. If the code exists and the name points somewhere else, the code is kept and a warning is stored. An unknown name is quarantined. There is no fuzzy match.

**Numbers.** Commas are removed, so `1,25,000` becomes `125000`. Blank, whitespace, `NA`, `NULL`, and `-` stay null. Text that is not a number is invalid. Counts must be whole numbers. Housing percentages must be from 0 to 100. Missing numbers are never replaced with zero.

**Duplicates.** Districts are grouped by code. Pension rows are grouped by state. Identical payloads keep one row. Different payloads for the same key are all quarantined.

**Validation.**

| Rule | Effect |
| --- | --- |
| Population missing, not numeric, or not greater than zero | District excluded |
| District name or code missing | District excluded |
| Invalid or negative pension count | That scheme excluded |
| Invalid housing percentage | That housing row excluded |
| Male + female does not equal total population | Warning, row kept |
| Households greater than population | Warning, row kept |
| SC or ST, or SC + ST, greater than population | Warning, row kept |

Errors and warnings both go to `audit.quarantine_record`. The quality report lists them. In batch `20261106T170833Z` the list was empty.

**Pension shape.** The source row is wide: one State/UT, three scheme columns. Silver unpivots Jharkhand into three rows with `grain = state_scheme`. Disability is kept in silver because it is in the source. The district mart only has columns for old-age and widow pensions, and those stay null.

## 7. Why the gold joins look like this

`sql/002_gold.sql` builds both candidate tables in `staging` from `silver.dim_district`. Publication copies them into `gold` only after the quality gate, in a single transaction. A session advisory lock stops a second run from writing at the same time.

The demographic fact is `LEFT JOIN`ed on `batch_id` and `district_code`. The district list is the driver, so a district remains even if a measure is missing. Both sides are unique on that key, so the join cannot create a second row for the same district. An inner join would drop a district that failed to land in the fact table. The left join keeps the “one row per demographic district” rule visible.

Housing is `LEFT JOIN`ed only in the feature insert, on the same key. The outreach mart does not need housing percentages. Leaving housing out of that insert means a housing fan-out cannot duplicate outreach rows. An unmatched housing district does not remove the demographic district. Latrine coverage and the vulnerability index become null for that district.

`silver.fact_pension_beneficiaries` is not in `sql/002_gold.sql`. Joining it would attach 993,567 and 272,108 to every district, or would require dividing those totals. Both would invent district counts. The pension columns are SQL `NULL`. A test reads the SQL file and fails if the pension fact is referenced there.

After the insert, quality checks count gold rows, distinct district codes, and any non-null pension or score value. A failed check marks the batch failed and sends an alert. The loaded rows are left in place so they can be inspected.

## 8. Feature formulas

All population rates use the district total population as the denominator and are on a 0–100 scale. A null or zero denominator produces null, not zero.

| Feature | Formula |
| --- | --- |
| `population_per_household` | total population / total households |
| `female_population_percentage` | 100 × female population / total population |
| `literacy_rate` | 100 × literate population / total population |
| `work_participation_rate` | 100 × workers / total population |
| `sc_population_percentage` | 100 × SC population / total population |
| `st_population_percentage` | 100 × ST population / total population |
| `household_coverage_ratio` | latrine-inside-premises percent / 100 |
| `household_vulnerability_index` | average of dilapidated percent and open-defecation percent, then / 100 |
| `old_age_pension_coverage` | null |
| `widow_pension_coverage` | null |

`household_coverage_ratio` means the fraction of households with a latrine inside the premises. The denominator is all households in the housing PCA district total. The source already publishes that share as a percent, so dividing by 100 turns it into a 0–1 ratio.

`household_vulnerability_index` is a housing description. It is not `pension_outreach_priority_score`.

If the feature table were used for training later, the score and the priority category must not be used as inputs or as the label. They are unpublished. Pension coverage is absent, so it cannot leak into a label built from the same coverage. Age 50+ is stored in silver and is not copied into the feature table, because using it as “elderly” would misstate the population the score was meant to use.

## 9. Priority score

The illustrative weights are 35% low old-age coverage, 25% low widow coverage, 20% high elderly share, and 20% high household vulnerability.

The pipeline checks whether those inputs exist. District pension coverage does not. Population aged 60 or older does not. The SQL therefore writes null for the score and the category, and writes the reason into `score_unavailable_reason`.

The method that would be used if both inputs existed is: min-max normalize each indicator across the 24 districts, reverse the coverage indicators so low coverage scores higher, apply the weights, and label tertiles as High, Medium, and Low. Equal scores share a category. Cut points would follow the sorted scores when a tie sits on a boundary. That calculation is not executed.

## 10. Batch id, reload, and backfill

| Command | What it does |
| --- | --- |
| `python run_pipeline.py` | New batch id, download, full load, publish gold if checks pass |
| `python run_pipeline.py --skip-download` | New batch id, local ingest of files already in `data/raw/` |
| `python run_pipeline.py --batch-id <id> --skip-download` | Replace that batch in bronze and silver, then publish if checks pass |
| `python run_pipeline.py --batch-id <id> --from-bronze` | Replay silver and the candidate from bronze. Published gold stays unchanged |
| `python run_pipeline.py --batch-id <id> --from-bronze --promote` | Replay, then publish that batch if the same quality gate passes |
| `python run_pipeline.py --schedule-seconds 86400` | Run, sleep 86400 seconds, run again. The batch id is stable inside one window |

Each ingested file is copied to `data/raw/batches/<batch_id>/`. `audit.source_manifest` stores the source URL, SHA-256, acquisition mode (`download`, `api_fallback`, `local_ingest`, or `bronze_replay`), reference period, and ingestion time. An NSAP API fallback also stores the original JSON. Local ingest does not record a download time.

`--from-bronze` without `--batch-id` is rejected. The scheduler catches a failed run, logs it, and waits for the next interval. The alert hook has already fired inside `run_pipeline`.

Published gold changes only when publication commits. Querying `gold.mart_district_pension_outreach` without a filter returns the latest successful publication. The bundled snapshot has 24 districts; the pipeline checks district codes rather than that count.

## 11. Where to look after a run

| Question | Place |
| --- | --- |
| Did the run succeed? | `audit.pipeline_run` |
| How many source rows versus Jharkhand rows? | `audit.source_manifest` and `reports/data_quality_<batch_id>.md` |
| Which rows were rejected or flagged? | `audit.quarantine_record` |
| Which checks passed? | `audit.data_quality_check` |
| Original values | `bronze.raw_* .raw_record` |
| State pension totals | `silver.fact_pension_beneficiaries` |
| District table to use for analysis | `gold.mart_district_pension_outreach` |
| Rates | `gold.ml_district_features` |

`scripts/check_snapshot.py` prints the bronze count, the gold count, and how many gold rows still have a null pension value and a null score for batch `20261106T170833Z`.
