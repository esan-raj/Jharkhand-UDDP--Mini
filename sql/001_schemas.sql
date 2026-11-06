CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    batch_id text PRIMARY KEY,
    status text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    parameters jsonb,
    error_message text
);

CREATE TABLE IF NOT EXISTS audit.source_manifest (
    batch_id text NOT NULL,
    source_name text NOT NULL,
    source_file_name text NOT NULL,
    full_row_count integer NOT NULL,
    filtered_row_count integer NOT NULL,
    bronze_row_count integer NOT NULL,
    downloaded_at timestamptz,
    PRIMARY KEY (batch_id, source_name)
);

CREATE TABLE IF NOT EXISTS audit.quarantine_record (
    quarantine_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    source_name text NOT NULL,
    rule_name text NOT NULL,
    severity text NOT NULL,
    business_key text,
    details jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.data_quality_check (
    check_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    check_name text NOT NULL,
    status text NOT NULL,
    observed jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.alert_event (
    alert_id bigserial PRIMARY KEY,
    batch_id text,
    severity text NOT NULL,
    message text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.raw_demographic (
    bronze_row_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    source_name text NOT NULL,
    source_file_name text NOT NULL,
    ingestion_timestamp timestamptz NOT NULL,
    source_row_number integer NOT NULL,
    raw_record jsonb NOT NULL,
    UNIQUE (batch_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS bronze.raw_housing (
    bronze_row_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    source_name text NOT NULL,
    source_file_name text NOT NULL,
    ingestion_timestamp timestamptz NOT NULL,
    source_row_number integer NOT NULL,
    raw_record jsonb NOT NULL,
    UNIQUE (batch_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS bronze.raw_pension (
    bronze_row_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    source_name text NOT NULL,
    source_file_name text NOT NULL,
    ingestion_timestamp timestamptz NOT NULL,
    source_row_number integer NOT NULL,
    raw_record jsonb NOT NULL,
    UNIQUE (batch_id, source_row_number)
);

COMMENT ON TABLE bronze.raw_demographic IS
    'One original Census 2011 demographic source row per record. Values are stored unchanged in raw_record. Grain: source file row. Jharkhand filter is applied before insert; the full file remains in data/raw.';
COMMENT ON TABLE bronze.raw_housing IS
    'One original Census 2011 housing PCA source row per record. Values are stored unchanged in raw_record.';
COMMENT ON TABLE bronze.raw_pension IS
    'One original NSAP source row per record. The published grain is one row per State/UT, with scheme counts in columns.';

CREATE TABLE IF NOT EXISTS silver.district_alias (
    alias_normalized text PRIMARY KEY,
    canonical_normalized text NOT NULL,
    alias_label text NOT NULL,
    notes text NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_district (
    batch_id text NOT NULL,
    district_code text NOT NULL,
    district_name text NOT NULL,
    state_name text NOT NULL,
    source_name_raw text NOT NULL,
    name_match_method text NOT NULL,
    PRIMARY KEY (batch_id, district_code)
);

COMMENT ON TABLE silver.dim_district IS
    'One row per Jharkhand district in the Census 2011 demographic source for a pipeline batch.';

CREATE TABLE IF NOT EXISTS silver.fact_district_demographics (
    batch_id text NOT NULL,
    district_code text NOT NULL,
    total_population bigint,
    male_population bigint,
    female_population bigint,
    literate_population bigint,
    total_workers bigint,
    sc_population bigint,
    st_population bigint,
    total_households bigint,
    age_group_0_29 bigint,
    age_group_30_49 bigint,
    age_group_50_plus bigint,
    PRIMARY KEY (batch_id, district_code)
);

COMMENT ON TABLE silver.fact_district_demographics IS
    'One row per district and batch. Households is the absolute Census household count published on the demographic file. age_group_50_plus is age 50 and above, not age 60 and above.';

CREATE TABLE IF NOT EXISTS silver.fact_district_housing (
    batch_id text NOT NULL,
    district_code text NOT NULL,
    district_name_raw text,
    indicator_unit text NOT NULL,
    published_household_base numeric,
    pct_good numeric,
    pct_livable numeric,
    pct_dilapidated numeric,
    pct_latrine_within_premises numeric,
    pct_open_defecation numeric,
    pct_owned numeric,
    PRIMARY KEY (batch_id, district_code)
);

COMMENT ON TABLE silver.fact_district_housing IS
    'One row per district and batch. Housing PCA district-total figures are percentages of households. published_household_base is the source value 100, not an absolute household count.';

CREATE TABLE IF NOT EXISTS silver.fact_pension_beneficiaries (
    batch_id text NOT NULL,
    state_name text NOT NULL,
    scheme_name text NOT NULL,
    beneficiary_count bigint,
    category text,
    reference_period text NOT NULL,
    grain text NOT NULL,
    source_sl_no text,
    PRIMARY KEY (batch_id, state_name, scheme_name)
);

COMMENT ON TABLE silver.fact_pension_beneficiaries IS
    'One row per state and pension scheme. Unpivoted from the wide NSAP state file. This is not a district grain.';

CREATE TABLE IF NOT EXISTS gold.mart_district_pension_outreach (
    batch_id text NOT NULL,
    district_code text NOT NULL,
    district_name text NOT NULL,
    total_population bigint,
    male_population bigint,
    female_population bigint,
    literate_population bigint,
    total_workers bigint,
    sc_population bigint,
    st_population bigint,
    total_households bigint,
    old_age_pension_beneficiaries bigint,
    widow_pension_beneficiaries bigint,
    old_age_beneficiaries_per_1000_population numeric,
    widow_beneficiaries_per_1000_population numeric,
    beneficiaries_per_1000_households numeric,
    pension_outreach_priority_score numeric,
    priority_category text,
    score_unavailable_reason text,
    PRIMARY KEY (batch_id, district_code)
);

COMMENT ON TABLE gold.mart_district_pension_outreach IS
    'One row per Jharkhand demographic district. District pension counts and the priority score are null because the NSAP source is state-level and elderly population aged 60+ is not in the census extract.';

CREATE TABLE IF NOT EXISTS gold.ml_district_features (
    batch_id text NOT NULL,
    district_code text NOT NULL,
    district_name text NOT NULL,
    population_per_household numeric,
    female_population_percentage numeric,
    literacy_rate numeric,
    work_participation_rate numeric,
    sc_population_percentage numeric,
    st_population_percentage numeric,
    old_age_pension_coverage numeric,
    widow_pension_coverage numeric,
    household_coverage_ratio numeric,
    household_vulnerability_index numeric,
    feature_notes text NOT NULL,
    PRIMARY KEY (batch_id, district_code)
);

COMMENT ON TABLE gold.ml_district_features IS
    'One row per district. Rates use demographic totals. household_coverage_ratio is the share of households with a latrine inside the premises. Pension coverage is null.';

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.mart_district_pension_outreach (
    LIKE gold.mart_district_pension_outreach INCLUDING ALL
);
CREATE TABLE IF NOT EXISTS staging.ml_district_features (
    LIKE gold.ml_district_features INCLUDING ALL
);

COMMENT ON TABLE staging.mart_district_pension_outreach IS
    'Candidate outreach mart. Downstream readers use gold.mart_district_pension_outreach, which is replaced only after publication.';
COMMENT ON TABLE staging.ml_district_features IS
    'Candidate feature table. Published together with the outreach mart or not at all.';

ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS source_url text;
ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS batch_file_path text;
ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS file_sha256 text;
ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS acquisition_mode text;
ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS reference_period text;
ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS ingestion_timestamp timestamptz;
ALTER TABLE audit.source_manifest ADD COLUMN IF NOT EXISTS original_payload_path text;
