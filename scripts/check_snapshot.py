import psycopg

conn = psycopg.connect("postgresql://uddp:uddp_local@localhost:5433/jharkhand_uddp")
cur = conn.cursor()
batch_id = "20261106T170833Z"
cur.execute("SELECT COUNT(*) FROM bronze.raw_demographic WHERE batch_id = %s", (batch_id,))
print("bronze_demographic", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM gold.mart_district_pension_outreach")
print("gold_rows", cur.fetchone()[0])
cur.execute(
    """
    SELECT COUNT(*)
    FROM gold.mart_district_pension_outreach
    WHERE old_age_pension_beneficiaries IS NULL
      AND pension_outreach_priority_score IS NULL
    """
)
print("gold_null_pension_and_score", cur.fetchone()[0])
cur.execute("SELECT status FROM audit.pipeline_run WHERE batch_id = %s", (batch_id,))
print("run_status", cur.fetchone()[0])
