import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from psycopg.types.json import Jsonb

from uddp.alerts import send_alert
from uddp.cleaning import ALIAS_NOTES, ALIASES, clean_datasets
from uddp.config import load_settings
from uddp.db import (
    acquire_pipeline_lock,
    connect,
    ensure_schema,
    execute_sql_file,
    fetch_bronze,
    load_bronze,
    publish_gold,
    release_pipeline_lock,
    wait_for_database,
)
from uddp.download import SourcePayload, load_source_files, sha256_file
from uddp.logging_setup import setup_logging
from uddp.quality import blocking_checks, evaluate, store_checks, write_report
from uddp.retry import with_retry

logger = logging.getLogger("uddp")


def _executemany(cur, sql, rows):
    if rows:
        cur.executemany(sql, rows)

BRONZE_TABLES = {
    "demographic": "bronze.raw_demographic",
    "housing": "bronze.raw_housing",
    "pension": "bronze.raw_pension",
}


class DataQualityError(RuntimeError):
    pass


def _record_run(conn, batch_id, status, started_at, parameters, error_message=None, commit=False):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.pipeline_run (batch_id, status, started_at, finished_at, parameters, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE
            SET status = EXCLUDED.status,
                finished_at = EXCLUDED.finished_at,
                parameters = EXCLUDED.parameters,
                error_message = EXCLUDED.error_message
            """,
            (
                batch_id,
                status,
                started_at,
                datetime.now(timezone.utc),
                Jsonb(parameters),
                error_message,
            ),
        )
    if commit:
        conn.commit()


def _load_aliases(conn, commit=False):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM silver.district_alias")
        _executemany(cur, 
            """
            INSERT INTO silver.district_alias (alias_normalized, canonical_normalized, alias_label, notes)
            VALUES (%s, %s, %s, %s)
            """,
            [
                (alias, canonical, alias.title(), ALIAS_NOTES[alias])
                for alias, canonical in ALIASES.items()
            ],
        )
    if commit:
        conn.commit()


def _replace_silver(conn, batch_id, clean, reference_period, commit=False):
    with conn.cursor() as cur:
        for table in (
            "silver.dim_district",
            "silver.fact_district_demographics",
            "silver.fact_district_housing",
            "silver.fact_pension_beneficiaries",
        ):
            cur.execute(f"DELETE FROM {table} WHERE batch_id = %s", (batch_id,))
        cur.execute("DELETE FROM audit.quarantine_record WHERE batch_id = %s", (batch_id,))
        _executemany(cur, 
            """
            INSERT INTO silver.dim_district (
                batch_id, district_code, district_name, state_name, source_name_raw, name_match_method
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    row["district_code"],
                    row["district_name"],
                    row["state_name"],
                    row["source_name_raw"],
                    row["name_match_method"],
                )
                for row in clean.districts
            ],
        )
        _executemany(cur, 
            """
            INSERT INTO silver.fact_district_demographics (
                batch_id, district_code, total_population, male_population, female_population,
                literate_population, total_workers, sc_population, st_population, total_households,
                age_group_0_29, age_group_30_49, age_group_50_plus
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    row["district_code"],
                    row["total_population"],
                    row["male_population"],
                    row["female_population"],
                    row["literate_population"],
                    row["total_workers"],
                    row["sc_population"],
                    row["st_population"],
                    row["total_households"],
                    row["age_group_0_29"],
                    row["age_group_30_49"],
                    row["age_group_50_plus"],
                )
                for row in clean.demographics
            ],
        )
        _executemany(cur, 
            """
            INSERT INTO silver.fact_district_housing (
                batch_id, district_code, district_name_raw, indicator_unit, published_household_base,
                pct_good, pct_livable, pct_dilapidated, pct_latrine_within_premises,
                pct_open_defecation, pct_owned
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    row["district_code"],
                    row["district_name_raw"],
                    row["indicator_unit"],
                    row["published_household_base"],
                    row["pct_good"],
                    row["pct_livable"],
                    row["pct_dilapidated"],
                    row["pct_latrine_within_premises"],
                    row["pct_open_defecation"],
                    row["pct_owned"],
                )
                for row in clean.housing
            ],
        )
        _executemany(cur, 
            """
            INSERT INTO silver.fact_pension_beneficiaries (
                batch_id, state_name, scheme_name, beneficiary_count, category,
                reference_period, grain, source_sl_no
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    row["state_name"],
                    row["scheme_name"],
                    row["beneficiary_count"],
                    row["category"],
                    row["reference_period"] or reference_period,
                    row["grain"],
                    row["source_sl_no"],
                )
                for row in clean.pensions
            ],
        )
        _executemany(cur, 
            """
            INSERT INTO audit.quarantine_record (
                batch_id, source_name, rule_name, severity, business_key, details
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    issue.source_name,
                    issue.rule_name,
                    issue.severity,
                    issue.business_key,
                    Jsonb(issue.details),
                )
                for issue in clean.issues
            ],
        )
    if commit:
        conn.commit()


def _manifest(conn, batch_id, provenance, full_count, filtered_count, bronze_count, commit=False):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.source_manifest (
                batch_id, source_name, source_file_name, full_row_count,
                filtered_row_count, bronze_row_count, downloaded_at,
                source_url, batch_file_path, file_sha256, acquisition_mode,
                reference_period, ingestion_timestamp, original_payload_path
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id, source_name) DO UPDATE
            SET source_file_name = EXCLUDED.source_file_name,
                full_row_count = EXCLUDED.full_row_count,
                filtered_row_count = EXCLUDED.filtered_row_count,
                bronze_row_count = EXCLUDED.bronze_row_count,
                downloaded_at = CASE
                    WHEN EXCLUDED.acquisition_mode IN ('download', 'api_fallback') THEN EXCLUDED.downloaded_at
                    ELSE audit.source_manifest.downloaded_at
                END,
                source_url = EXCLUDED.source_url,
                batch_file_path = EXCLUDED.batch_file_path,
                file_sha256 = EXCLUDED.file_sha256,
                acquisition_mode = EXCLUDED.acquisition_mode,
                reference_period = EXCLUDED.reference_period,
                ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                original_payload_path = EXCLUDED.original_payload_path
            """,
            (
                batch_id,
                provenance["source_name"],
                provenance["file_name"],
                full_count,
                filtered_count,
                bronze_count,
                provenance["downloaded_at"],
                provenance["source_url"],
                provenance["batch_file_path"],
                provenance["file_sha256"],
                provenance["acquisition_mode"],
                provenance["reference_period"],
                provenance["ingestion_timestamp"],
                provenance["original_payload_path"],
            ),
        )
    if commit:
        conn.commit()


def scheduled_batch_id(logical_date):
    """Stable id for retries of one Airflow data interval."""
    return logical_date.strftime("airflow-%Y%m%d")


def window_batch_id(schedule_seconds, now=None):
    """Stable id for CLI runs that fall in the same schedule window."""
    if schedule_seconds <= 0:
        raise ValueError("schedule_seconds must be positive")
    moment = time.time() if now is None else now
    window = int(moment // schedule_seconds)
    return f"schedule-{schedule_seconds}-{window}"


def _replay_payload(settings, key, rows, batch_id):
    source = settings["sources"][key]
    batch_path = settings["raw_dir"] / "batches" / batch_id / source["filename"]
    json_path = batch_path.with_suffix(".json")
    fieldnames = list(rows[0].keys()) if rows else []
    if batch_path.exists():
        digest = sha256_file(batch_path)
        size = batch_path.stat().st_size
        path = batch_path
    else:
        encoded = json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        size = len(encoded)
        path = batch_path
    original = json_path if json_path.exists() else None
    return SourcePayload(
        rows=rows,
        fieldnames=fieldnames,
        batch_path=path,
        source_url="",
        sha256=digest,
        acquisition_mode="bronze_replay",
        reference_period=str(source.get("reference_period") or ""),
        original_payload_path=original,
        byte_size=size,
    )


def _provenance_record(source, payload, ingested_at):
    downloaded_at = ingested_at if payload.acquisition_mode in {"download", "api_fallback"} else None
    original = payload.original_payload_path
    return {
        "source_name": source["source_name"],
        "file_name": Path(payload.batch_path).name,
        "downloaded_at": downloaded_at,
        "source_url": payload.source_url,
        "batch_file_path": str(payload.batch_path),
        "file_sha256": payload.sha256,
        "acquisition_mode": payload.acquisition_mode,
        "reference_period": payload.reference_period,
        "ingestion_timestamp": ingested_at,
        "original_payload_path": None if original is None else str(original),
    }


def _persist_failure(conn, settings, batch_id, started_at, parameters, message):
    conn.rollback()
    _record_run(conn, batch_id, "FAILED", started_at, parameters, message)
    send_alert(settings, message, batch_id=batch_id, conn=conn)
    conn.commit()


def run_pipeline(batch_id=None, skip_download=False, from_bronze=False, promote=None, settings=None):
    settings = settings or load_settings()
    setup_logging(settings)
    if from_bronze and not batch_id:
        raise ValueError("Replay from bronze requires --batch-id")
    if promote is None:
        promote = not from_bronze
    batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    started_at = datetime.now(timezone.utc)
    parameters = {
        "skip_download": skip_download,
        "from_bronze": from_bronze,
        "promote": promote,
    }
    retries = settings["pipeline"]["max_retries"]
    backoff = settings["pipeline"]["retry_backoff_seconds"]
    state_name = settings["pipeline"]["state_name"]
    reference_period = settings["pipeline"]["reference_period"]

    wait_for_database(settings)
    conn = connect(settings)
    locked = False
    try:
        ensure_schema(conn, settings)
        acquire_pipeline_lock(conn)
        locked = True
        _record_run(conn, batch_id, "RUNNING", started_at, parameters)
        conn.commit()
        _load_aliases(conn)
        ingested_at = datetime.now(timezone.utc)

        if from_bronze:
            payloads = {}
            original_stamps = []
            for key, table in BRONZE_TABLES.items():
                rows = fetch_bronze(conn, table, batch_id)
                payloads[key] = _replay_payload(settings, key, rows, batch_id)
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT MAX(ingestion_timestamp) FROM {table} WHERE batch_id = %s",
                        (batch_id,),
                    )
                    stamp = cur.fetchone()[0]
                if stamp is not None:
                    original_stamps.append(stamp)
            if original_stamps:
                ingested_at = min(original_stamps)
            logger.info("Replaying batch %s from bronze without publishing gold yet", batch_id)
        else:
            download = with_retry(retries, backoff, (OSError, TimeoutError, Exception))(
                load_source_files
            )
            payloads = download(settings, batch_id, skip_download=skip_download)
            for key, table in BRONZE_TABLES.items():
                source = settings["sources"][key]
                numbered = list(enumerate(payloads[key].rows, start=2))
                loaded = load_bronze(
                    conn,
                    table,
                    batch_id,
                    source["source_name"],
                    source["filename"],
                    numbered,
                    ingested_at,
                )
                logger.info("Loaded %s bronze rows for %s", loaded, key)

        datasets = {key: payloads[key].rows for key in BRONZE_TABLES}
        fieldnames = {key: payloads[key].fieldnames for key in BRONZE_TABLES}
        clean = clean_datasets(
            datasets["demographic"],
            datasets["housing"],
            datasets["pension"],
            state_name,
            reference_period,
        )
        filtered_counts = {
            "demographic": clean.stats["demographic_rows_filtered"],
            "housing": clean.stats["housing_rows_district_grain"],
            "pension": clean.stats["pension_rows_filtered"],
        }
        provenance = []
        for key in BRONZE_TABLES:
            source = settings["sources"][key]
            record = _provenance_record(source, payloads[key], ingested_at)
            provenance.append(record)
            _manifest(
                conn,
                batch_id,
                record,
                len(datasets[key]),
                filtered_counts[key],
                len(datasets[key]),
            )
        _replace_silver(conn, batch_id, clean, reference_period)
        execute_sql_file(conn, settings["sql_dir"] / "002_gold.sql", {"batch_id": batch_id})
        source_names = {key: settings["sources"][key]["source_name"] for key in BRONZE_TABLES}
        checks = evaluate(
            conn,
            batch_id,
            clean,
            datasets,
            fieldnames,
            source_names,
            state_name,
            replay=from_bronze,
        )
        store_checks(conn, batch_id, checks)
        paths = {key: payloads[key].batch_path for key in payloads}
        failed = blocking_checks(checks)
        published = False
        # Persist bronze, silver, candidate gold, quarantine, and checks before the publish decision.
        conn.commit()

        if failed or not promote:
            if failed:
                message = "Data quality failed: " + ", ".join(check["check_name"] for check in failed)
                report_path = write_report(
                    settings, batch_id, clean, checks, paths, published=False, provenance=provenance
                )
                _record_run(conn, batch_id, "FAILED", started_at, parameters, message)
                send_alert(settings, message, batch_id=batch_id, conn=conn)
                conn.commit()
                raise DataQualityError(message)
            report_path = write_report(
                settings, batch_id, clean, checks, paths, published=False, provenance=provenance
            )
            _record_run(conn, batch_id, "SUCCESS", started_at, parameters, "Replay did not promote gold")
            conn.commit()
            logger.info("Batch %s replayed. Published gold was left unchanged.", batch_id)
            return _summary(batch_id, report_path, clean, published=False)

        publish_gold(conn, batch_id)
        published = True
        report_path = write_report(
            settings, batch_id, clean, checks, paths, published=True, provenance=provenance
        )
        _record_run(conn, batch_id, "SUCCESS", started_at, parameters)
        conn.commit()
        logger.info("Batch %s published. Report: %s", batch_id, report_path)
        return _summary(batch_id, report_path, clean, published=True)
    except DataQualityError:
        raise
    except Exception as exc:
        logger.exception("Pipeline failed")
        try:
            _persist_failure(conn, settings, batch_id, started_at, parameters, f"Pipeline failed: {exc}")
        except Exception:
            logger.exception("Could not record the failure alert")
        raise
    finally:
        if locked:
            try:
                release_pipeline_lock(conn)
            except Exception:
                conn.rollback()
                logger.exception("Could not release the pipeline lock")
        conn.close()


def _summary(batch_id, report_path, clean, published):
    return {
        "batch_id": batch_id,
        "status": "SUCCESS",
        "published": published,
        "report": str(report_path),
        "districts": clean.stats["demographic_rows_published"],
        "pension_scheme_rows": clean.stats["pension_scheme_rows_published"],
        "warnings": clean.stats["warning_count"],
        "quarantine_errors": clean.stats["error_count"],
    }
