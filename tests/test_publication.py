import csv
import shutil
from pathlib import Path

import psycopg
import pytest

from uddp.config import load_settings
from uddp.db import PIPELINE_LOCK_KEY, acquire_pipeline_lock, connect, release_pipeline_lock
from uddp.pipeline import DataQualityError, run_pipeline

ROOT = Path(__file__).resolve().parents[1]
BUNDLED = ROOT / "data" / "raw"
SOURCE_FILES = (
    "india-districts-census-2011.csv",
    "hlpca-total.csv",
    "RS_Session_256_AU_3148_1.csv",
)


def _settings(database_name, raw_dir, work_dir):
    settings = load_settings()
    settings["database"]["name"] = database_name
    settings["raw_dir"] = raw_dir
    settings["report_dir"] = work_dir / "reports"
    settings["log_dir"] = work_dir / "logs"
    settings["pipeline"]["max_retries"] = 1
    settings["pipeline"]["retry_backoff_seconds"] = 0
    return settings


def _copy_sources(raw_dir):
    raw_dir.mkdir(parents=True, exist_ok=True)
    for name in SOURCE_FILES:
        shutil.copy(BUNDLED / name, raw_dir / name)


@pytest.fixture(scope="module")
def database_name():
    base = load_settings()["database"]
    admin_url = (
        f"postgresql://{base['user']}:{base['password']}@{base['host']}:{base['port']}/postgres"
    )
    name = "jharkhand_uddp_itest"
    try:
        admin = psycopg.connect(admin_url, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not available for integration tests: {exc}")
    admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin.execute(f"CREATE DATABASE {name}")
    yield name
    admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin.close()


@pytest.fixture
def settings(tmp_path, database_name):
    raw_dir = tmp_path / "raw"
    _copy_sources(raw_dir)
    return _settings(database_name, raw_dir, tmp_path)


def _publish(settings, batch_id):
    summary = run_pipeline(batch_id=batch_id, skip_download=True, settings=settings)
    assert summary["published"] is True
    return summary


def _gold_batch_ids(settings):
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT batch_id FROM gold.mart_district_pension_outreach")
            mart = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT DISTINCT batch_id FROM gold.ml_district_features")
            features = {row[0] for row in cur.fetchall()}
    return mart, features


def test_happy_path_publishes_the_bundled_snapshot(settings):
    _publish(settings, "good-snapshot")
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT district_code)
                FROM gold.mart_district_pension_outreach
                """
            )
            assert cur.fetchone() == (24, 24)
            cur.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT district_code)
                FROM gold.ml_district_features
                """
            )
            assert cur.fetchone() == (24, 24)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM silver.fact_district_housing
                WHERE batch_id = %s
                """,
                ("good-snapshot",),
            )
            assert cur.fetchone()[0] == 24
            cur.execute(
                """
                SELECT COUNT(*)
                FROM silver.fact_pension_beneficiaries
                WHERE batch_id = %s AND grain = 'state_scheme'
                """,
                ("good-snapshot",),
            )
            assert cur.fetchone()[0] == 3
            cur.execute(
                """
                SELECT COUNT(*)
                FROM gold.mart_district_pension_outreach
                WHERE old_age_pension_beneficiaries IS NULL
                  AND widow_pension_beneficiaries IS NULL
                  AND pension_outreach_priority_score IS NULL
                  AND priority_category IS NULL
                """
            )
            assert cur.fetchone()[0] == 24
            cur.execute(
                """
                SELECT acquisition_mode, downloaded_at, reference_period, file_sha256, batch_file_path
                FROM audit.source_manifest
                WHERE batch_id = %s
                ORDER BY source_name
                """,
                ("good-snapshot",),
            )
            rows = cur.fetchall()
    assert len(rows) == 3
    assert {row[0] for row in rows} == {"local_ingest"}
    assert all(row[1] is None for row in rows)
    assert {row[2] for row in rows} == {"2011", "2021-22"}
    assert all(row[3] and Path(row[4]).exists() for row in rows)


def test_invalid_batch_keeps_both_published_gold_tables(settings):
    _publish(settings, "good-before-failure")
    header = (BUNDLED / "hlpca-total.csv").read_text(encoding="utf-8").splitlines()[0]
    (settings["raw_dir"] / "hlpca-total.csv").write_text(header + "\n", encoding="utf-8")
    with pytest.raises(DataQualityError):
        run_pipeline(batch_id="missing-housing", skip_download=True, settings=settings)
    assert _gold_batch_ids(settings) == ({"good-before-failure"}, {"good-before-failure"})
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM audit.pipeline_run WHERE batch_id = %s",
                ("missing-housing",),
            )
            assert cur.fetchone()[0] == "FAILED"
            cur.execute(
                """
                SELECT check_name
                FROM audit.data_quality_check
                WHERE batch_id = %s AND status = 'fail'
                """,
                ("missing-housing",),
            )
            failed = {row[0] for row in cur.fetchall()}
            cur.execute(
                "SELECT COUNT(*) FROM audit.alert_event WHERE batch_id = %s",
                ("missing-housing",),
            )
            alerts = cur.fetchone()[0]
    assert "nonempty_inputs" in failed or "jharkhand_source_coverage" in failed
    assert alerts >= 1
    assert (settings["report_dir"] / "data_quality_missing-housing.md").exists()


def test_missing_nsap_scheme_does_not_publish(settings):
    _publish(settings, "good-before-nsap")
    (settings["raw_dir"] / "RS_Session_256_AU_3148_1.csv").write_text(
        "Sl. No.,Category,States/UTs,Old Age Pension Scheme\n8,State,Jharkhand,993567\n",
        encoding="utf-8",
    )
    with pytest.raises(DataQualityError):
        run_pipeline(batch_id="missing-scheme", skip_download=True, settings=settings)
    assert _gold_batch_ids(settings) == ({"good-before-nsap"}, {"good-before-nsap"})
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status FROM audit.data_quality_check
                WHERE batch_id = %s AND check_name = 'required_headers'
                """,
                ("missing-scheme",),
            )
            assert cur.fetchone()[0] == "fail"


def test_quarantined_district_blocks_publication(settings):
    _publish(settings, "good-before-loss")
    path = settings["raw_dir"] / "india-districts-census-2011.csv"
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames
    for row in rows:
        if row["State name"] == "JHARKHAND" and row["District name"] == "Ranchi":
            row["Population"] = "0"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(DataQualityError):
        run_pipeline(batch_id="lost-ranchi", skip_download=True, settings=settings)
    assert _gold_batch_ids(settings) == ({"good-before-loss"}, {"good-before-loss"})
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM audit.quarantine_record
                WHERE batch_id = %s AND rule_name = 'population_not_positive'
                """,
                ("lost-ranchi",),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                """
                SELECT status FROM audit.data_quality_check
                WHERE batch_id = %s AND check_name = 'district_code_coverage'
                """,
                ("lost-ranchi",),
            )
            assert cur.fetchone()[0] == "fail"


def test_one_missing_housing_district_is_a_warning(settings):
    path = settings["raw_dir"] / "hlpca-total.csv"
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = [
            row
            for row in reader
            if not (
                row["State Name"] == "JHARKHAND"
                and row["District Name"] == "Ranchi"
            )
        ]
        fieldnames = reader.fieldnames
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    summary = _publish(settings, "housing-gap")
    assert summary["published"] is True
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT total_population
                FROM gold.mart_district_pension_outreach
                WHERE batch_id = %s AND district_name = 'Ranchi'
                """,
                ("housing-gap",),
            )
            assert cur.fetchone()[0] > 0
            cur.execute(
                """
                SELECT household_coverage_ratio
                FROM gold.ml_district_features
                WHERE batch_id = %s AND district_name = 'Ranchi'
                """,
                ("housing-gap",),
            )
            assert cur.fetchone()[0] is None
            cur.execute(
                """
                SELECT status, observed::text
                FROM audit.data_quality_check
                WHERE batch_id = %s AND check_name = 'housing_coverage_gap'
                """,
                ("housing-gap",),
            )
            status, observed = cur.fetchone()
    assert status == "warn"
    assert "364" in observed


def test_replay_preserves_gold_until_promotion(settings):
    _copy_sources(settings["raw_dir"])
    _publish(settings, "replay-a")
    _publish(settings, "replay-b")
    assert _gold_batch_ids(settings) == ({"replay-b"}, {"replay-b"})
    replayed = run_pipeline(
        batch_id="replay-a",
        from_bronze=True,
        promote=False,
        settings=settings,
    )
    assert replayed["published"] is False
    assert _gold_batch_ids(settings) == ({"replay-b"}, {"replay-b"})
    promoted = run_pipeline(
        batch_id="replay-a",
        from_bronze=True,
        promote=True,
        settings=settings,
    )
    assert promoted["published"] is True
    assert _gold_batch_ids(settings) == ({"replay-a"}, {"replay-a"})


def test_same_batch_id_does_not_duplicate_rows(settings):
    _copy_sources(settings["raw_dir"])
    _publish(settings, "idempotent")
    _publish(settings, "idempotent")
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bronze.raw_demographic WHERE batch_id = %s",
                ("idempotent",),
            )
            bronze_rows = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM silver.dim_district WHERE batch_id = %s",
                ("idempotent",),
            )
            silver_rows = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.mart_district_pension_outreach")
            gold_rows = cur.fetchone()[0]
    assert bronze_rows == 640
    assert silver_rows == 24
    assert gold_rows == 24


def test_pipeline_lock_blocks_a_second_session(settings):
    first = connect(settings)
    second = connect(settings)
    try:
        acquire_pipeline_lock(first)
        with second.cursor() as cur:
            cur.execute("SET lock_timeout = '400ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                cur.execute("SELECT pg_advisory_lock(%s)", (PIPELINE_LOCK_KEY,))
    finally:
        release_pipeline_lock(first)
        first.close()
        second.close()
