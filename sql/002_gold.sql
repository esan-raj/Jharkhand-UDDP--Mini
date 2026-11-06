-- Candidate gold for one batch. The published gold tables are replaced later,
-- in one transaction, only after the quality gate passes.
-- Pension facts are intentionally absent: a state total must not be copied onto every district.
DELETE FROM staging.mart_district_pension_outreach WHERE batch_id = %(batch_id)s;
DELETE FROM staging.ml_district_features WHERE batch_id = %(batch_id)s;

INSERT INTO staging.mart_district_pension_outreach (
    batch_id,
    district_code,
    district_name,
    total_population,
    male_population,
    female_population,
    literate_population,
    total_workers,
    sc_population,
    st_population,
    total_households,
    old_age_pension_beneficiaries,
    widow_pension_beneficiaries,
    old_age_beneficiaries_per_1000_population,
    widow_beneficiaries_per_1000_population,
    beneficiaries_per_1000_households,
    pension_outreach_priority_score,
    priority_category,
    score_unavailable_reason
)
SELECT
    d.batch_id,
    d.district_code,
    d.district_name,
    f.total_population,
    f.male_population,
    f.female_population,
    f.literate_population,
    f.total_workers,
    f.sc_population,
    f.st_population,
    f.total_households,
    NULL::bigint,
    NULL::bigint,
    NULL::numeric,
    NULL::numeric,
    NULL::numeric,
    NULL::numeric,
    NULL::text,
    'Unavailable: NSAP beneficiary counts are state and scheme totals and were not allocated to districts. The census extract has age bands 0-29, 30-49, and 50+, not population aged 60+. A district pension-outreach score would be misleading.'
FROM silver.dim_district d
LEFT JOIN silver.fact_district_demographics f
    ON f.batch_id = d.batch_id
   AND f.district_code = d.district_code
WHERE d.batch_id = %(batch_id)s;

INSERT INTO staging.ml_district_features (
    batch_id,
    district_code,
    district_name,
    population_per_household,
    female_population_percentage,
    literacy_rate,
    work_participation_rate,
    sc_population_percentage,
    st_population_percentage,
    old_age_pension_coverage,
    widow_pension_coverage,
    household_coverage_ratio,
    household_vulnerability_index,
    feature_notes
)
SELECT
    d.batch_id,
    d.district_code,
    d.district_name,
    f.total_population::numeric / NULLIF(f.total_households, 0),
    100.0 * f.female_population::numeric / NULLIF(f.total_population, 0),
    100.0 * f.literate_population::numeric / NULLIF(f.total_population, 0),
    100.0 * f.total_workers::numeric / NULLIF(f.total_population, 0),
    100.0 * f.sc_population::numeric / NULLIF(f.total_population, 0),
    100.0 * f.st_population::numeric / NULLIF(f.total_population, 0),
    NULL::numeric,
    NULL::numeric,
    h.pct_latrine_within_premises / 100.0,
    ((h.pct_dilapidated + h.pct_open_defecation) / 2.0) / 100.0,
    'population_per_household = total_population / total_households. Percent features use total_population as the denominator and are expressed on a 0-100 scale. household_coverage_ratio = percent of households with a latrine inside the premises / 100; denominator is all households in the housing PCA district total. household_vulnerability_index is the mean of the dilapidated-house share and the open-defecation share; it is not a pension priority score. Missing inputs stay null and are not filled with zero. Pension coverage is null because no district pension source is loaded. Do not use pension_outreach_priority_score or priority_category as training targets or features; they are unpublished. Age 50+ is not used as an elderly feature.'
FROM silver.dim_district d
LEFT JOIN silver.fact_district_demographics f
    ON f.batch_id = d.batch_id
   AND f.district_code = d.district_code
LEFT JOIN silver.fact_district_housing h
    ON h.batch_id = d.batch_id
   AND h.district_code = d.district_code
WHERE d.batch_id = %(batch_id)s;
