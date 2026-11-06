# Jharkhand Mini-UDDP

District analytical pipeline for Jharkhand Census 2011 demographics and housing, plus National Social Assistance Programme (NSAP) pension beneficiaries. The published mart has one row per Census district. District pension counts and the outreach priority score are left null because the pension source is a state total.

- [Project report](docs/PROJECT_REPORT.md) — what was built and the district results
- [How it works](docs/HOW_IT_WORKS.md) — data flow, cleaning rules, joins, and operations

## Run

PostgreSQL is started with Docker Compose on host port **5433** (port 5432 was already in use on this machine). The database is `jharkhand_uddp`.

```bash
docker compose up -d
pip install -r requirements.txt
python run_pipeline.py
python -m pytest
```

Useful commands:

```bash
python run_pipeline.py --skip-download
python run_pipeline.py --batch-id 20261106T120000Z --from-bronze
python run_pipeline.py --schedule-seconds 86400
```

## Publication, failure, and replay

Candidate rows are built in `staging`, not in the published gold tables. `gold.mart_district_pension_outreach` and `gold.ml_district_features` are replaced together in one transaction, and only after every blocking check passes. If any blocking check fails, both published tables keep the previous successful batch. The failed batch still keeps its bronze rows, silver rows, quarantine records, quality checks, report, and alert.

A warning does not block publication. Missing optional values stay null. If one district has no housing match, that demographic row remains and the gap is reported.

Checks that block publication:

- required headers, a non-empty file, and at least one Jharkhand row for each source
- candidate district codes equal the Jharkhand demographic codes taken before quarantine
- bronze, silver, and candidate-gold counts reconcile, including the pension wide-to-long step
- required NSAP schemes have beneficiary counts
- state pension totals are not copied onto districts
- population is positive
- each source in the batch has an ingestion timestamp

`ingestion_timestamp` is when the pipeline stored the file. `reference_period` is 2011 or 2021–22 and is not used as the freshness clock. A source with no rows in the batch fails freshness. The production checks do not hard-code 24 districts. The bundled-file test does.

A session lock stops a second CLI or Airflow run from writing while the first run is in progress. Airflow retries of the same logical date reuse `airflow-YYYYMMDD`. A CLI schedule reuses `schedule-<seconds>-<window>` inside that window.

```bash
python run_pipeline.py --skip-download
python run_pipeline.py --batch-id 20261106T170833Z --skip-download
python run_pipeline.py --batch-id 20261106T170833Z --from-bronze
python run_pipeline.py --batch-id 20261106T170833Z --from-bronze --promote
```

`--skip-download` reads the CSVs already in `data/raw/`, copies them to `data/raw/batches/<batch_id>/`, and records `local_ingest`. It does not set a download time. A fresh run without `--skip-download` records `download` or, for NSAP, `api_fallback` and keeps the original JSON next to the CSV. `--from-bronze` rebuilds silver and the candidate from stored bronze rows and records `bronze_replay`. It does not replace published gold unless `--promote` is also set. Promotion uses the same quality gate.

## Reviewer verification

```bash
docker compose up -d
pip install -r requirements.txt
python -m pytest
python run_pipeline.py --skip-download
```

`python -m pytest` runs the cleaning tests and, when PostgreSQL is up, the integration tests in `tests/test_publication.py`. Those tests use database `jharkhand_uddp_itest` and the bundled CSVs. They do not download files. The walkthrough above loads the bundled CSVs into `jharkhand_uddp`.

Expected bundled snapshot: 24 districts in both gold tables, 24 housing rows, 3 Jharkhand state-scheme pension rows, and null district pension counts, coverage, score, and category.

Configuration is `config/settings.yaml`. `UDDP_DB_HOST`, `UDDP_DB_PORT`, `UDDP_DB_NAME`, `UDDP_DB_USER`, `UDDP_DB_PASSWORD`, `UDDP_ALERT_WEBHOOK_URL`, and `UDDP_DATA_GOV_API_KEY` override it. An empty webhook writes alerts to `logs/alerts.jsonl` and `audit.alert_event`.

`dags/jharkhand_uddp_dag.py` is an Airflow wrapper around the same `run_pipeline` function. Airflow is optional. The Python scheduler above is the runner used for this project.

## Sources

| Source | File | Grain used |
| --- | --- | --- |
| Census 2011 district demographics | `data/raw/india-districts-census-2011.csv` | One row per district |
| Census 2011 housing PCA | `data/raw/hlpca-total.csv` | District total row (`Rural/Urban = Total`, tehsil code 0) |
| NSAP beneficiaries, 2021-22 | `data/raw/RS_Session_256_AU_3148_1.csv` | One row per State/UT |

The NSAP page is [State/NE/UTs-wise beneficiaries, 2021-22](https://www.data.gov.in/resource/stateneuts-wise-beneficiaries-under-pension-schemes-national-social-assistance-programme). The direct file URL returns HTTP 403 to this client, so the loader uses the same Open Government Data API the page preview calls, then writes the official column names. Jharkhand in that file has 993,567 old-age beneficiaries, 272,108 widow beneficiaries, and 31,286 disability beneficiaries. Those figures are for the state.

The housing PCA district-total row sets `Total Number of households` to 100. The other housing measures are percentages of households, not absolute counts. Absolute household counts in the gold mart come from the `Households` column on the demographic file.

## Layers

`bronze` stores every source row as JSON, unchanged, with `source_name`, `source_file_name`, `ingestion_timestamp`, and `batch_id`. Reloading a batch deletes that batch and inserts it again.

`silver.dim_district` is one row per Jharkhand district in the demographic file.

`silver.fact_district_demographics` is one row per district. `age_group_50_plus` is the source band for age 50 and above. It is not a count of people aged 60 or older.

`silver.fact_district_housing` is one row per district. Indicators stay in percent-of-households units.

`silver.fact_pension_beneficiaries` is one row per state and scheme. The wide NSAP row is unpivoted here. Its grain is `state_scheme`.

`gold.mart_district_pension_outreach` is one row per demographic district.

`gold.ml_district_features` is one row per demographic district.

## Joins

The demographic district list is the spine. Demographics are joined to it with `LEFT JOIN` on `batch_id` and `district_code`. Both sides are unique on that key, so the join cannot multiply rows. A district remains in the mart if a demographic measure is missing.

Housing is `LEFT JOIN`ed only into the feature table, on the same key. An unmatched housing district does not remove the demographic district. Housing rows whose code is absent are matched through the alias table. Names that still do not match are quarantined. They are not folded into a nearby district.

Pension is not joined to the district mart. Copying the Jharkhand total onto every district, or dividing it by 24, would invent district counts. Old-age beneficiaries, widow beneficiaries, coverage ratios, the priority score, and the priority category are SQL `NULL`.

## Cleaning rules

District names are upper-cased, punctuation is removed, and these aliases are applied: Hazaribag, East Singhbhum, West Singhbhum, Saraikela, Seraikela, Koderma, and Sahebganj. A code that disagrees with the name is kept, and the disagreement is reported. Unknown names are quarantined.

Deduplication key for districts is the district code. For pension it is the state. Exact duplicate payloads keep one row. Conflicting payloads for the same key are all quarantined. Counts before and after are in the data-quality report.

Blank, whitespace-only, and non-numeric values stay null. They are not replaced with zero.

Population must be a positive integer or the district is quarantined. A negative pension count is quarantined. Male plus female not equal to total population, households greater than population, and SC or ST greater than population are warnings. Those districts stay in silver. `audit.quarantine_record` stores both errors and warnings.

## Features

Denominators below are the district total population, except where noted. A zero or null denominator leaves the feature null.

- `population_per_household` = total population / total households
- `female_population_percentage` = 100 * female population / total population
- `literacy_rate` = 100 * literate population / total population
- `work_participation_rate` = 100 * workers / total population
- `sc_population_percentage` = 100 * SC population / total population
- `st_population_percentage` = 100 * ST population / total population
- `household_coverage_ratio` = percent of households with a latrine inside the premises / 100. The denominator is all households in the housing PCA district total.
- `household_vulnerability_index` = the average of the dilapidated-house share and the open-defecation share. This is a housing description. It is not the pension priority score.
- `old_age_pension_coverage` and `widow_pension_coverage` are null.

If these features were later used for training, `pension_outreach_priority_score` and `priority_category` must not be features or targets. They are unpublished. Pension coverage is not present, so it cannot leak into a label built from the same coverage. The age-50 band is not used as an elderly feature, because that would mislabel the target the score was meant to represent.

## Priority score

The illustrative weight is 35% low old-age coverage, 25% low widow coverage, 20% high elderly share, and 20% high household vulnerability. The first two inputs do not exist at district level. The census extract has age bands 0–29, 30–49, and 50+, so the elderly share is not available either. The score and the High / Medium / Low category are therefore null in every district.

If a district pension source and an age-60 population were both supplied, the score would min-max normalize each indicator across the 24 districts, invert the coverage indicators so that low coverage scores higher, and use the weights above. High, Medium, and Low would be tertiles of that score. Equal scores would share a category. With only 24 districts, the cut points would be documented on the sorted score rather than forced into equal counts when ties sit on a boundary. That method is not applied to this extract.

## Operations

Each run logs to `logs/pipeline.log`, retries downloads with exponential backoff, and records the batch in `audit.pipeline_run`. A failed quality check or an uncaught exception marks the batch failed and calls the alert hook. The quality report is `reports/data_quality_<batch_id>.md`. Replacing a batch is idempotent. Gold is rebuilt as a single current snapshot, so it stays at one row per district.
