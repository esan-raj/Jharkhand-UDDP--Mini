import json
from datetime import datetime, timezone

from uddp.cleaning import (
    DEMOGRAPHIC_FIELDS,
    HOUSING_FIELDS,
    PENSION_SCHEMES,
    classify_demographic_identifiers,
    field,
    is_state,
)

# status "fail" blocks publication. status "warn" is stored and does not.
PUBLICATION_GATES = (
    "required_headers",
    "nonempty_inputs",
    "jharkhand_source_coverage",
    "district_code_coverage",
    "row_count_reconciliation",
    "nsap_scheme_values",
    "state_pension_not_allocated_to_districts",
    "pension_grain_is_state_scheme",
    "pension_coverage_not_fabricated",
    "population_positive",
    "ingestion_freshness",
    "gold_candidate_row_shape",
)

WARNING_CHECKS = (
    "unusable_demographic_identifiers",
    "housing_coverage_gap",
    "cleaning_summary",
)

REQUIRED_HEADERS = {
    "demographic": list(DEMOGRAPHIC_FIELDS),
    "housing": [
        "State Name",
        "District Name",
        "District Code",
        "Tehsil Code",
        "Rural/Urban",
        "Area Name",
        *HOUSING_FIELDS[3:],
    ],
    "pension": [
        "States/UTs",
        "Old Age Pension Scheme",
        "Widow Pension Scheme",
        "Divyangjan /Disability Pension Scheme",
    ],
}

FRESHNESS_HOURS = 6


def _check(name, status, observed):
    return {
        "check_name": name,
        "status": status,
        "blocks_publication": name in PUBLICATION_GATES and status == "fail",
        "observed": observed,
    }


def blocking_checks(checks):
    return [check for check in checks if check["blocks_publication"]]


def _missing_headers(key, fieldnames):
    available = {name.strip().lower() for name in fieldnames}
    return [name for name in REQUIRED_HEADERS[key] if name.lower() not in available]


def _jharkhand_count(rows, key, state_name):
    if key == "demographic":
        names = ("State name", "State Name")
    elif key == "housing":
        names = ("State Name", "State name")
    else:
        names = ("States/UTs", "states_uts")
    return sum(1 for row in rows if is_state(field(row, *names), state_name))


def evaluate(conn, batch_id, clean, datasets, fieldnames, source_names, state_name, replay):
    checks = []
    header_gaps = {
        key: _missing_headers(key, fieldnames.get(key) or []) for key in REQUIRED_HEADERS
    }
    checks.append(
        _check(
            "required_headers",
            "pass" if not any(header_gaps.values()) else "fail",
            {"missing_headers": {key: names for key, names in header_gaps.items() if names}},
        )
    )
    counts = {key: len(rows) for key, rows in datasets.items()}
    checks.append(
        _check(
            "nonempty_inputs",
            "pass" if all(count > 0 for count in counts.values()) else "fail",
            {"row_counts": counts},
        )
    )
    coverage = {key: _jharkhand_count(datasets[key], key, state_name) for key in datasets}
    checks.append(
        _check(
            "jharkhand_source_coverage",
            "pass" if all(count > 0 for count in coverage.values()) else "fail",
            {"jharkhand_rows": coverage},
        )
    )

    usable_codes, unusable = classify_demographic_identifiers(
        datasets["demographic"], state_name
    )
    checks.append(
        _check(
            "unusable_demographic_identifiers",
            "warn" if unusable else "pass",
            {"rows": unusable},
        )
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT district_code
            FROM staging.mart_district_pension_outreach
            WHERE batch_id = %s
            ORDER BY district_code
            """,
            (batch_id,),
        )
        candidate_codes = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT district_code)
            FROM staging.mart_district_pension_outreach
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        mart_rows, mart_districts = cur.fetchone()
        cur.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT district_code)
            FROM staging.ml_district_features
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        feature_rows, feature_districts = cur.fetchone()
        cur.execute(
            """
            SELECT district_code
            FROM staging.ml_district_features
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        feature_codes = {row[0] for row in cur.fetchall()}
        cur.execute(
            """
            SELECT COUNT(*)
            FROM staging.mart_district_pension_outreach
            WHERE batch_id = %s
              AND (
                    old_age_pension_beneficiaries IS NOT NULL
                 OR widow_pension_beneficiaries IS NOT NULL
                 OR old_age_beneficiaries_per_1000_population IS NOT NULL
                 OR widow_beneficiaries_per_1000_population IS NOT NULL
                 OR beneficiaries_per_1000_households IS NOT NULL
                 OR pension_outreach_priority_score IS NOT NULL
                 OR priority_category IS NOT NULL
              )
            """,
            (batch_id,),
        )
        allocated = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*)
            FROM staging.ml_district_features
            WHERE batch_id = %s
              AND (
                    old_age_pension_coverage IS NOT NULL
                 OR widow_pension_coverage IS NOT NULL
              )
            """,
            (batch_id,),
        )
        fabricated_coverage = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE grain = 'state_scheme'),
                   COUNT(*) FILTER (WHERE beneficiary_count IS NULL)
            FROM silver.fact_pension_beneficiaries
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        pension_rows, state_scheme_rows, null_beneficiaries = cur.fetchone()
        cur.execute(
            """
            SELECT scheme_name, beneficiary_count
            FROM silver.fact_pension_beneficiaries
            WHERE batch_id = %s
            ORDER BY scheme_name
            """,
            (batch_id,),
        )
        scheme_rows = {name: count for name, count in cur.fetchall()}
        cur.execute(
            "SELECT COUNT(*) FROM silver.dim_district WHERE batch_id = %s",
            (batch_id,),
        )
        silver_districts = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM silver.fact_district_demographics WHERE batch_id = %s",
            (batch_id,),
        )
        silver_demographics = cur.fetchone()[0]
        cur.execute(
            "SELECT district_code FROM silver.fact_district_housing WHERE batch_id = %s",
            (batch_id,),
        )
        housing_codes = {row[0] for row in cur.fetchall()}
        cur.execute(
            """
            SELECT COUNT(*)
            FROM silver.fact_district_demographics
            WHERE batch_id = %s AND total_population > 0
            """,
            (batch_id,),
        )
        positive_population = cur.fetchone()[0]
        bronze_counts = {}
        freshness = {}
        for key, source_name in source_names.items():
            table = {
                "demographic": "bronze.raw_demographic",
                "housing": "bronze.raw_housing",
                "pension": "bronze.raw_pension",
            }[key]
            cur.execute(
                f"""
                SELECT COUNT(*), MAX(ingestion_timestamp)
                FROM {table}
                WHERE batch_id = %s AND source_name = %s
                """,
                (batch_id, source_name),
            )
            bronze_count, ingested_at = cur.fetchone()
            bronze_counts[key] = bronze_count
            freshness[source_name] = None if ingested_at is None else ingested_at.isoformat()
        cur.execute(
            """
            SELECT source_name, reference_period, acquisition_mode
            FROM audit.source_manifest
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        manifest_rows = cur.fetchall()

    candidate_set = set(candidate_codes)
    usable_set = set(usable_codes)
    missing_codes = sorted(usable_set - candidate_set)
    unexpected_codes = sorted(candidate_set - usable_set)
    checks.append(
        _check(
            "district_code_coverage",
            "fail" if missing_codes or unexpected_codes or not usable_set else "pass",
            {
                "source_codes_before_quarantine": usable_codes,
                "candidate_codes": candidate_codes,
                "missing_codes": missing_codes,
                "unexpected_codes": unexpected_codes,
                "note": (
                    "Compared with Jharkhand demographic codes before quarantine. "
                    "A district dropped by quarantine fails this check even when "
                    "candidate gold matches the reduced silver output."
                ),
            },
        )
    )
    shape_ok = (
        mart_rows == mart_districts == feature_rows == feature_districts
        and candidate_set == feature_codes
        and mart_rows == silver_districts
        and mart_rows > 0
    )
    checks.append(
        _check(
            "gold_candidate_row_shape",
            "pass" if shape_ok else "fail",
            {
                "mart_rows": mart_rows,
                "mart_distinct": mart_districts,
                "feature_rows": feature_rows,
                "feature_distinct": feature_districts,
                "silver_districts": silver_districts,
            },
        )
    )

    expected_long = clean.stats["pension_rows_after_dedup"] * len(PENSION_SCHEMES)
    reconciliation_ok = (
        bronze_counts.get("demographic") == clean.stats["demographic_rows_in_file"]
        and bronze_counts.get("housing") == clean.stats["housing_rows_in_file"]
        and bronze_counts.get("pension") == clean.stats["pension_rows_in_file"]
        and silver_districts == clean.stats["demographic_rows_published"]
        and silver_demographics == clean.stats["demographic_rows_published"]
        and len(housing_codes) == clean.stats["housing_rows_published"]
        and pension_rows == clean.stats["pension_scheme_rows_published"]
        and pension_rows == expected_long
        and mart_rows == silver_districts
        and feature_rows == silver_districts
    )
    checks.append(
        _check(
            "row_count_reconciliation",
            "pass" if reconciliation_ok else "fail",
            {
                "demographic": {
                    "source_rows": clean.stats["demographic_rows_in_file"],
                    "filtered_rows": clean.stats["demographic_rows_filtered"],
                    "exact_duplicates_removed": clean.stats["demographic_exact_duplicates_removed"],
                    "conflicting_keys": clean.stats["demographic_conflicting_keys"],
                    "published_rows": clean.stats["demographic_rows_published"],
                    "bronze_rows": bronze_counts.get("demographic"),
                },
                "housing": {
                    "source_rows": clean.stats["housing_rows_in_file"],
                    "filtered_state_rows": clean.stats["housing_rows_filtered_state"],
                    "district_grain_rows": clean.stats["housing_rows_district_grain"],
                    "exact_duplicates_removed": clean.stats["housing_exact_duplicates_removed"],
                    "conflicting_keys": clean.stats["housing_conflicting_keys"],
                    "published_rows": clean.stats["housing_rows_published"],
                    "bronze_rows": bronze_counts.get("housing"),
                },
                "pension_wide_to_long": {
                    "source_rows": clean.stats["pension_rows_in_file"],
                    "filtered_wide_rows": clean.stats["pension_rows_filtered"],
                    "wide_rows_after_dedup": clean.stats["pension_rows_after_dedup"],
                    "exact_duplicates_removed": clean.stats["pension_exact_duplicates_removed"],
                    "schemes_per_wide_row": len(PENSION_SCHEMES),
                    "expected_long_rows": expected_long,
                    "published_long_rows": clean.stats["pension_scheme_rows_published"],
                    "bronze_rows": bronze_counts.get("pension"),
                },
                "candidate_gold_rows": mart_rows,
                "quarantine_errors": clean.stats["error_count"],
                "quarantine_warnings": clean.stats["warning_count"],
            },
        )
    )

    missing_schemes = []
    null_schemes = []
    for _label, _api_id, scheme_name in PENSION_SCHEMES:
        if scheme_name not in scheme_rows:
            missing_schemes.append(scheme_name)
        elif scheme_rows[scheme_name] is None:
            null_schemes.append(scheme_name)
    checks.append(
        _check(
            "nsap_scheme_values",
            "pass" if not missing_schemes and not null_schemes and null_beneficiaries == 0 else "fail",
            {
                "present": {name: scheme_rows[name] for name in scheme_rows},
                "missing_schemes": missing_schemes,
                "null_beneficiary_schemes": null_schemes,
            },
        )
    )
    checks.append(
        _check(
            "state_pension_not_allocated_to_districts",
            "pass" if allocated == 0 else "fail",
            {"district_rows_with_pension_values": allocated},
        )
    )
    checks.append(
        _check(
            "pension_grain_is_state_scheme",
            "pass" if pension_rows == state_scheme_rows else "fail",
            {"pension_rows": pension_rows, "state_scheme_rows": state_scheme_rows},
        )
    )
    checks.append(
        _check(
            "pension_coverage_not_fabricated",
            "pass" if fabricated_coverage == 0 else "fail",
            {"rows_with_coverage": fabricated_coverage},
        )
    )
    checks.append(
        _check(
            "population_positive",
            "pass" if positive_population == silver_demographics and silver_demographics > 0 else "fail",
            {
                "positive_population_rows": positive_population,
                "silver_demographic_rows": silver_demographics,
            },
        )
    )

    published_codes = set(usable_codes) - set(missing_codes)
    housing_gaps = sorted(code for code in published_codes if code not in housing_codes)
    checks.append(
        _check(
            "housing_coverage_gap",
            "warn" if housing_gaps else "pass",
            {
                "demographic_codes_without_housing": housing_gaps,
                "note": (
                    "A missing housing match keeps the demographic district. "
                    "Optional housing measures stay null. This warning does not by itself block publication."
                ),
            },
        )
    )

    absent = [
        source_name for source_name, stamp in freshness.items() if stamp is None
    ]
    stale = []
    now = datetime.now(timezone.utc)
    if not replay:
        for source_name, stamp in freshness.items():
            if stamp is None:
                continue
            age_hours = (now - datetime.fromisoformat(stamp)).total_seconds() / 3600
            if age_hours > FRESHNESS_HOURS:
                stale.append({"source_name": source_name, "age_hours": round(age_hours, 2)})
    reference_periods = {name: period for name, period, _mode in manifest_rows}
    checks.append(
        _check(
            "ingestion_freshness",
            "fail" if absent or stale else "pass",
            {
                "ingestion_timestamps": freshness,
                "reference_periods": reference_periods,
                "absent_sources": absent,
                "stale": stale,
                "replay": replay,
                "note": (
                    "Freshness uses ingestion_timestamp. reference_period is the source's "
                    "census year or NSAP year and is not an ingestion clock. A source with "
                    "no rows in this batch fails this check."
                ),
            },
        )
    )
    checks.append(
        _check(
            "cleaning_summary",
            "warn" if clean.stats["error_count"] or clean.stats["warning_count"] else "pass",
            clean.stats,
        )
    )
    return checks


def store_checks(conn, batch_id, checks, commit=False):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit.data_quality_check WHERE batch_id = %s", (batch_id,))
        if checks:
            cur.executemany(
                """
                INSERT INTO audit.data_quality_check (batch_id, check_name, status, observed)
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (
                        batch_id,
                        check["check_name"],
                        check["status"],
                        json.dumps(check["observed"], default=str),
                    )
                    for check in checks
                ],
            )
    if commit:
        conn.commit()


def write_report(settings, batch_id, clean, checks, paths, published, provenance):
    settings["report_dir"].mkdir(parents=True, exist_ok=True)
    path = settings["report_dir"] / f"data_quality_{batch_id}.md"
    lines = [
        f"# Data quality report {batch_id}",
        "",
        f"Published gold snapshot: {'yes' if published else 'no'}.",
        "",
        "Checks with status FAIL block publication. Checks with status WARN do not.",
        "Missing optional measures stay null. A housing gap for one district keeps that demographic row.",
        "",
        "## Provenance",
        "",
    ]
    for item in provenance:
        lines.append(
            f"- {item['source_name']}: mode `{item['acquisition_mode']}`, "
            f"reference period `{item['reference_period']}`, "
            f"sha256 `{item['file_sha256']}`, path `{item['batch_file_path']}`"
        )
    lines.extend(["", "## Sources", ""])
    for key, file_path in paths.items():
        lines.append(f"- {key}: `{file_path.name}`")
    lines.extend(
        [
            "",
            "## Row counts",
            "",
            f"- Demographic file rows: {clean.stats['demographic_rows_in_file']}",
            f"- Demographic Jharkhand rows before deduplication: {clean.stats['demographic_rows_filtered']}",
            f"- Demographic exact duplicates removed: {clean.stats['demographic_exact_duplicates_removed']}",
            f"- Demographic conflicting keys quarantined: {clean.stats['demographic_conflicting_keys']}",
            f"- Demographic rows published to silver: {clean.stats['demographic_rows_published']}",
            f"- Housing file rows: {clean.stats['housing_rows_in_file']}",
            f"- Housing Jharkhand rows: {clean.stats['housing_rows_filtered_state']}",
            f"- Housing district-total rows before deduplication: {clean.stats['housing_rows_district_grain']}",
            f"- Housing exact duplicates removed: {clean.stats['housing_exact_duplicates_removed']}",
            f"- Housing rows published to silver: {clean.stats['housing_rows_published']}",
            f"- Pension file rows: {clean.stats['pension_rows_in_file']}",
            f"- Pension Jharkhand wide rows before deduplication: {clean.stats['pension_rows_filtered']}",
            f"- Pension wide rows after deduplication: {clean.stats['pension_rows_after_dedup']}",
            f"- Pension scheme rows after wide-to-long: {clean.stats['pension_scheme_rows_published']}",
            "",
            "## Missing values",
            "",
            clean.stats["missing_value_treatment"],
            "",
            "Demographic blanks on the filtered extract:",
            "",
        ]
    )
    for column, count in clean.stats["demographic_missing_values"].items():
        lines.append(f"- {column}: {count}")
    lines.extend(["", "Housing blanks on district-total rows:", ""])
    for column, count in clean.stats["housing_missing_values"].items():
        lines.append(f"- {column}: {count}")
    lines.extend(["", "## Quarantine and warnings", ""])
    if not clean.issues:
        lines.append("No quarantined or warning rows.")
    else:
        for issue in clean.issues:
            lines.append(
                f"- {issue.severity} `{issue.rule_name}` on {issue.source_name} "
                f"`{issue.business_key}`: {json.dumps(issue.details, default=str)}"
            )
    lines.extend(["", "## Checks", ""])
    for check in checks:
        gate = "blocks publication" if check["blocks_publication"] else "does not block publication"
        lines.append(f"- {check['status'].upper()} `{check['check_name']}` ({gate})")
    lines.extend(
        [
            "",
            "## Priority score",
            "",
            "The district priority score is null. Sixty percent of the illustrative weight needs district pension coverage, and the census file does not publish population aged 60 or older. The age-50+ band is not used as a substitute. Household vulnerability is retained only as a descriptive feature.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
