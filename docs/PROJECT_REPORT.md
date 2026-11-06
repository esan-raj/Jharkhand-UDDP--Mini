# Jharkhand Mini-UDDP — Project Report

**Assignment:** Mini Unified Digital Data Platform  
**Role:** ETL / Data Engineer  
**Stack:** Python, PostgreSQL, Docker  
**Orchestration:** Python scheduler, with an optional Airflow wrapper  
**Published batch:** `20261106T170833Z`  
**Status:** Success

## 1. What was built

A data pipeline that downloads public Census 2011 and NSAP pension files, keeps the original files, and loads them into PostgreSQL database `jharkhand_uddp`. The database has four schemas:

| Schema | Role |
| --- | --- |
| `bronze` | Original source rows, stored unchanged, with ingestion metadata |
| `silver` | Cleaned Jharkhand districts, housing indicators, and state-level pension schemes |
| `gold` | One analytical row per district, plus a machine-learning feature table |
| `audit` | Pipeline runs, source counts, quarantine records, quality checks, and alerts |

The published analytical table is `gold.mart_district_pension_outreach`. It has one row for each of the 24 Jharkhand districts in the Census 2011 demographic file. A second table, `gold.ml_district_features`, holds rates that can be calculated from those sources.

Raw files are kept under `data/raw/`. Each run writes a quality report under `reports/` and a log under `logs/`.

## 2. Sources used

| Dataset | File | What was loaded |
| --- | --- | --- |
| Census 2011 district demographics | `india-districts-census-2011.csv` | 640 India district rows in bronze; 24 Jharkhand rows in silver and gold |
| Census 2011 housing PCA | `hlpca-total.csv` | 640 India rows in bronze; 24 Jharkhand district-total rows in silver |
| NSAP pension beneficiaries, 2021–22 | `RS_Session_256_AU_3148_1.csv` | 37 State/UT rows in bronze; 1 Jharkhand row, unpivoted to 3 schemes in silver |

The pension resource is the data.gov.in dataset “State/NE/UTs-wise Beneficiaries under Pension Schemes of National Social Assistance Programme (NSAP) during 2021-22” (Rajya Sabha Session 256, Unstarred Question 3148). The direct file link returns HTTP 403, so the loader reads the same Open Government Data API that the dataset preview uses and writes the official column names to disk.

Jharkhand pension counts from that file, kept at state level:

| Scheme | Beneficiaries |
| --- | ---: |
| Indira Gandhi National Old Age Pension Scheme | 993,567 |
| Indira Gandhi National Widow Pension Scheme | 272,108 |
| Indira Gandhi National Disability Pension Scheme | 31,286 |

These numbers are not split across districts.

## 3. What the district mart contains

Each gold row has the district code and name, total / male / female population, literate population, workers, Scheduled Caste population, Scheduled Tribe population, and total households.

District pension columns are present and are null:

- old-age beneficiaries
- widow beneficiaries
- old-age beneficiaries per 1,000 population
- widow beneficiaries per 1,000 population
- beneficiaries per 1,000 households
- pension outreach priority score
- priority category

The column `score_unavailable_reason` records why the score is null.

Summing the 24 district populations gives **32,988,134**, which is the Census 2011 population of Jharkhand. Male plus female population matches the district total in every district. No rows were quarantined.

Absolute household counts come from the demographic file. The housing file’s district-total row uses `100` as a percentage base, so its other fields are shares of households, not household counts.

## 4. District profile

Figures below are from batch `20261106T170833Z`. Literacy, female share, and work participation use total population as the denominator. Latrine coverage is the share of households with a latrine inside the premises. Household vulnerability is the average of the dilapidated-house share and the open-defecation share. It describes housing. It is not a pension priority score.

| District | Population | Households | Literacy % | Female % | Work % | SC % | ST % | Latrine coverage | Housing vulnerability |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Bokaro | 2,062,330 | 508,764 | 61.75 | 47.98 | 33.23 | 14.51 | 12.40 | 0.335 | 0.346 |
| Chatra | 1,042,886 | 215,502 | 48.91 | 48.80 | 38.13 | 32.65 | 4.37 | 0.101 | 0.467 |
| Deoghar | 1,492,073 | 321,710 | 53.18 | 48.06 | 36.96 | 12.74 | 12.13 | 0.164 | 0.439 |
| Dhanbad | 2,684,487 | 664,219 | 64.15 | 47.63 | 31.46 | 16.29 | 8.68 | 0.398 | 0.316 |
| Dumka | 1,321,442 | 326,978 | 50.88 | 49.41 | 47.28 | 6.02 | 43.22 | 0.098 | 0.477 |
| Garhwa | 1,322,784 | 292,052 | 49.40 | 48.32 | 43.20 | 24.19 | 15.56 | 0.071 | 0.486 |
| Giridih | 2,445,474 | 499,450 | 51.26 | 48.55 | 42.38 | 13.31 | 9.74 | 0.108 | 0.458 |
| Godda | 1,313,551 | 292,030 | 46.02 | 48.39 | 42.04 | 8.80 | 21.26 | 0.105 | 0.489 |
| Gumla | 1,025,213 | 233,074 | 54.60 | 49.83 | 47.55 | 3.17 | 68.94 | 0.103 | 0.459 |
| Hazaribagh | 1,734,495 | 381,748 | 58.42 | 48.64 | 37.74 | 17.50 | 7.02 | 0.196 | 0.415 |
| Jamtara | 791,042 | 185,668 | 53.89 | 48.82 | 41.29 | 9.21 | 30.40 | 0.098 | 0.472 |
| Khunti | 531,885 | 135,719 | 53.50 | 49.93 | 48.88 | 4.52 | 73.25 | 0.084 | 0.471 |
| Kodarma | 716,259 | 151,485 | 54.48 | 48.73 | 35.94 | 15.22 | 0.96 | 0.165 | 0.436 |
| Latehar | 726,978 | 160,514 | 48.24 | 49.15 | 43.11 | 21.31 | 45.54 | 0.108 | 0.457 |
| Lohardaga | 461,790 | 111,484 | 56.24 | 49.62 | 47.93 | 3.32 | 56.89 | 0.149 | 0.438 |
| Pakur | 900,422 | 217,175 | 39.19 | 49.73 | 44.93 | 3.16 | 42.10 | 0.102 | 0.480 |
| Palamu | 1,939,869 | 407,904 | 52.82 | 48.13 | 36.76 | 27.65 | 9.34 | 0.131 | 0.464 |
| Pashchimi Singhbhum | 1,502,338 | 400,324 | 48.43 | 50.12 | 46.25 | 3.79 | 67.31 | 0.118 | 0.457 |
| Purbi Singhbhum | 2,293,919 | 604,663 | 65.73 | 48.69 | 36.50 | 4.86 | 28.51 | 0.499 | 0.263 |
| Ramgarh | 949,443 | 252,625 | 62.83 | 47.95 | 32.87 | 11.20 | 21.19 | 0.329 | 0.348 |
| Ranchi | 2,914,253 | 730,982 | 65.59 | 48.70 | 39.22 | 5.25 | 35.76 | 0.409 | 0.303 |
| Sahibganj | 1,150,567 | 260,993 | 42.00 | 48.77 | 42.61 | 6.29 | 26.80 | 0.153 | 0.457 |
| Saraikela-Kharsawan | 1,065,056 | 285,687 | 57.56 | 48.88 | 40.38 | 5.28 | 35.18 | 0.203 | 0.412 |
| Simdega | 599,578 | 155,283 | 57.29 | 49.91 | 48.14 | 7.45 | 70.78 | 0.082 | 0.471 |

Reading the table as a housing and literacy description, not as a pension ranking:

- Literacy is highest in Purbi Singhbhum (65.73%), Ranchi (65.59%), and Dhanbad (64.15%). It is lowest in Pakur (39.19%), Sahibganj (42.00%), and Godda (46.02%).
- Latrine coverage inside the premises is highest in Purbi Singhbhum (0.499), Ranchi (0.409), and Dhanbad (0.398). It is lowest in Garhwa (0.071), Simdega (0.082), and Khunti (0.084).
- Scheduled Tribe share is highest in Khunti (73.25%), Simdega (70.78%), and Gumla (68.94%). It is lowest in Kodarma (0.96%).
- People per household are about 3.7 to 4.9.

## 5. Cleaning and name matching

Census district codes 346–369 already agree between the demographic file and the housing file for all 24 Jharkhand districts. Names in the two files also agree, including Kodarma, Purbi Singhbhum, Pashchimi Singhbhum, and Saraikela-Kharsawan.

An alias table is still loaded for spellings the brief asked to check: Hazaribag, East Singhbhum, West Singhbhum, Saraikela, Seraikela, Koderma, and Sahebganj. A name that does not match a code or an alias is quarantined. It is not merged into a similar district.

Duplicate rule: the business key is the district code, or the state for pension. An exact duplicate payload keeps one row. Conflicting values for the same key are all quarantined. In this batch, duplicate counts before and after processing were the same: 24 demographic rows, 24 housing rows, and 1 Jharkhand pension row. Nothing was removed.

Missing values are left null. None of the published demographic or housing fields were blank in the Jharkhand extract, so no zero-fill was needed or applied.

## 6. Priority score

The suggested score weights low old-age coverage at 35%, low widow coverage at 25%, high elderly share at 20%, and high household vulnerability at 20%.

That score is not calculated.

- Old-age and widow coverage are not available by district. The NSAP file is one row per State/UT.
- Elderly share is not available. The census extract has age bands 0–29, 30–49, and 50+. Age 50 and above is stored in silver as `age_group_50_plus` and is not treated as age 60 and above.
- Household vulnerability can be described, but using it alone would present a housing index as a pension-outreach ranking. The gold score and the High / Medium / Low category are null in all 24 districts.

If a real district pension file and a population aged 60 or older were supplied later, the documented method would min-max normalize each indicator across the 24 districts, invert coverage so that low coverage scores higher, apply the weights above, and cut High / Medium / Low as tertiles. Ties would share a category. That method is not applied to this extract.

## 7. Checks performed

Batch `20261106T170833Z` passed every quality check:

- 24 gold rows and 24 distinct district codes
- no district row carries a pension count, coverage ratio, score, or category
- pension silver rows are all `state_scheme`
- pension coverage features are null
- every published population is greater than zero
- ingestion timestamps are from the current run
- cleaning produced no errors and no warnings

The same batch was loaded a second time. Bronze stayed at 640 demographic rows for that batch, and gold stayed at 24 rows. `--from-bronze` rebuilds silver and the candidate snapshot from those bronze rows and leaves published gold unchanged. `--from-bronze --promote` publishes that batch only when the same blocking checks pass.

## 8. What this platform can and cannot answer

It can show, for each Jharkhand district in 2011, population structure, literacy, work participation, SC and ST shares, household size, latrine coverage, and a housing-vulnerability description.

It cannot say which district needs more pension outreach. The beneficiary counts belong to the whole state for 2021–22, while the population and housing figures are from Census 2011. Those periods also do not line up, which is a second reason not to divide the state pension total by district population.
