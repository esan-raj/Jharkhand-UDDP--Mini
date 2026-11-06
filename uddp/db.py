import logging
import time

import psycopg
from psycopg.types.json import Jsonb

from uddp.config import database_url

logger = logging.getLogger("uddp")


def connect(settings):
    return psycopg.connect(database_url(settings))


def wait_for_database(settings, attempts=30, delay_seconds=2):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            conn = connect(settings)
            conn.close()
            return
        except psycopg.OperationalError as exc:
            last_error = exc
            logger.info("Waiting for PostgreSQL (%s/%s)", attempt, attempts)
            time.sleep(delay_seconds)
    raise RuntimeError("PostgreSQL did not become ready") from last_error


def _split_sql(text):
    statements = []
    current = []
    in_quote = False
    for char in text:
        if char == "'":
            in_quote = not in_quote
            current.append(char)
            continue
        if char == ";" and not in_quote:
            statement = "".join(current).strip()
            lines = [
                line
                for line in statement.splitlines()
                if line.strip() and not line.strip().startswith("--")
            ]
            statement = "\n".join(lines).strip()
            if statement:
                statements.append(statement)
            current = []
            continue
        current.append(char)
    return statements


def execute_sql_file(conn, path, params=None, commit=False):
    statements = _split_sql(path.read_text(encoding="utf-8"))

    with conn.cursor() as cur:
        for statement in statements:
            if params and "%(" in statement:
                cur.execute(statement, params)
            else:
                cur.execute(statement)
    if commit:
        conn.commit()


def ensure_schema(conn, settings):
    execute_sql_file(conn, settings["sql_dir"] / "001_schemas.sql", commit=True)


PIPELINE_LOCK_KEY = 8142011

MART_COLUMNS = (
    "batch_id",
    "district_code",
    "district_name",
    "total_population",
    "male_population",
    "female_population",
    "literate_population",
    "total_workers",
    "sc_population",
    "st_population",
    "total_households",
    "old_age_pension_beneficiaries",
    "widow_pension_beneficiaries",
    "old_age_beneficiaries_per_1000_population",
    "widow_beneficiaries_per_1000_population",
    "beneficiaries_per_1000_households",
    "pension_outreach_priority_score",
    "priority_category",
    "score_unavailable_reason",
)

FEATURE_COLUMNS = (
    "batch_id",
    "district_code",
    "district_name",
    "population_per_household",
    "female_population_percentage",
    "literacy_rate",
    "work_participation_rate",
    "sc_population_percentage",
    "st_population_percentage",
    "old_age_pension_coverage",
    "widow_pension_coverage",
    "household_coverage_ratio",
    "household_vulnerability_index",
    "feature_notes",
)


def acquire_pipeline_lock(conn):
    """Session lock. It survives the commits inside a run and is released explicitly."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (PIPELINE_LOCK_KEY,))
    conn.commit()


def release_pipeline_lock(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (PIPELINE_LOCK_KEY,))
    conn.commit()


def publish_gold(conn, batch_id):
    """Replace both published gold tables. The caller commits or rolls back both."""
    mart = ", ".join(MART_COLUMNS)
    features = ", ".join(FEATURE_COLUMNS)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM gold.mart_district_pension_outreach")
        cur.execute("DELETE FROM gold.ml_district_features")
        cur.execute(
            f"""
            INSERT INTO gold.mart_district_pension_outreach ({mart})
            SELECT {mart}
            FROM staging.mart_district_pension_outreach
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        cur.execute(
            f"""
            INSERT INTO gold.ml_district_features ({features})
            SELECT {features}
            FROM staging.ml_district_features
            WHERE batch_id = %s
            """,
            (batch_id,),
        )


def load_bronze(conn, table, batch_id, source_name, source_file_name, rows, ingested_at, commit=False):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE batch_id = %s", (batch_id,))
        cur.executemany(
            f"""
            INSERT INTO {table} (
                batch_id, source_name, source_file_name, ingestion_timestamp,
                source_row_number, raw_record
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    source_name,
                    source_file_name,
                    ingested_at,
                    row_number,
                    Jsonb(record),
                )
                for row_number, record in rows
            ],
        )
    if commit:
        conn.commit()
    return len(rows)


def fetch_bronze(conn, table, batch_id):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT raw_record
            FROM {table}
            WHERE batch_id = %s
            ORDER BY source_row_number
            """,
            (batch_id,),
        )
        return [row[0] for row in cur.fetchall()]
