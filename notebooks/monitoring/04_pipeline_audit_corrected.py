# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pipeline Audit Notebook
# MAGIC
# MAGIC ## Purpose
# MAGIC Collect real pipeline monitoring metrics for the Hospital Analytics POC and append them to:
# MAGIC
# MAGIC `hospital_analytics.monitoring.pipeline_audit`
# MAGIC
# MAGIC ## What It Does
# MAGIC 1. Reads each raw CSV file from the landing volume.
# MAGIC 2. Inspects the actual schemas of the Bronze and Silver tables.
# MAGIC 3. Counts records in Landing, Bronze, Silver and the relevant Gold tables.
# MAGIC 4. Calculates data-quality metrics:
# MAGIC    - `duplicate_record_count` — duplicate rows found in Bronze
# MAGIC    - `null_record_count` — total null values found in Bronze
# MAGIC    - `rejected_record_count` — rows lost between Bronze and Silver
# MAGIC    - `valid_record_count` — records that reached Silver
# MAGIC    - `data_quality_percentage` — `100.0 * valid / landing`
# MAGIC 5. Appends one audit row per source dataset.
# MAGIC 6. Displays the latest run results.
# MAGIC
# MAGIC ## Data Sources
# MAGIC - Landing volume: `/Volumes/hospital_analytics/landing/hospital_raw/`
# MAGIC - Bronze: `hospital_analytics.bronze.*`
# MAGIC - Silver: `hospital_analytics.silver.*`
# MAGIC - Gold: `hospital_analytics.gold.*`

# COMMAND ----------

# Import required libraries
import logging
import uuid
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configure logging for the notebook
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline_audit")

# Unity Catalog catalog and schema names
CATALOG = "hospital_analytics"
LANDING_SCHEMA = "landing"
LANDING_VOLUME = "hospital_raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MONITORING_SCHEMA = "monitoring"
AUDIT_TABLE = f"{CATALOG}.{MONITORING_SCHEMA}.pipeline_audit"

# Base path for the landing volume
LANDING_BASE = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/"

# Source datasets to audit
SOURCES = ["patients", "doctors", "appointments", "treatments", "billing"]

# Mapping from each source table to the most relevant Gold business table.
# This mapping is based on the Gold table names already in the workspace;
# it is NOT a 1-to-1 record-level relationship.
GOLD_MAPPING = {
    "patients": "gold_patient_summary",
    "doctors": "gold_doctor_performance",
    "appointments": "gold_appointment_dashboard",
    "treatments": "gold_treatment_analysis",
    "billing": "gold_hospital_revenue",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def inspect_table(table_name):
    """
    Print the schema and column list of a Delta table.
    """
    try:
        df = spark.read.format("delta").table(table_name)
        print(f"--- Schema for {table_name} ---")
        df.printSchema()
        print(f"Columns: {df.columns}")
        logger.info(f"Inspected {table_name}: columns {df.columns}")
        return df
    except Exception as e:
        logger.error(f"Failed to inspect {table_name}: {e}")
        raise

# COMMAND ----------

def count_csv_rows(file_name):
    """
    Count the number of rows in a CSV file located in the landing volume.
    """
    try:
        file_path = f"{LANDING_BASE}{file_name}"
        logger.info(f"Counting rows in {file_path}...")

        # Read the CSV file with header and inferred schema, then count
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        count = df.count()

        logger.info(f"{file_name}: {count} landing rows.")
        return count
    except Exception as e:
        logger.error(f"Failed to count landing rows for {file_name}: {e}")
        raise

# COMMAND ----------

def count_delta_table(schema_name, table_name):
    """
    Count the number of rows in a Delta table.
    """
    full_name = f"{CATALOG}.{schema_name}.{table_name}"
    try:
        df = spark.read.format("delta").table(full_name)
        count = df.count()
        logger.info(f"{full_name}: {count} rows.")
        return count
    except Exception as e:
        logger.error(f"Failed to count {full_name}: {e}")
        raise

# COMMAND ----------

def count_duplicate_rows(df, source_name):
    """
    Count duplicate rows in a DataFrame by comparing total rows to distinct rows.
    """
    try:
        total_rows = df.count()
        distinct_rows = df.dropDuplicates().count()
        duplicate_count = total_rows - distinct_rows
        logger.info(f"{source_name}: {duplicate_count} duplicate rows found.")
        return duplicate_count
    except Exception as e:
        logger.error(f"Failed to count duplicate rows for {source_name}: {e}")
        raise

# COMMAND ----------

def count_null_values(df, source_name):
    """
    Count the total number of null (or empty string) values across all columns.
    """
    try:
        # One expression per column: 1 if the value is null, otherwise 0
        null_exprs = [
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in df.columns
        ]

        # Sum the null counts across all columns
        null_row = df.select(null_exprs).first()
        null_count = sum(value if value is not None else 0 for value in null_row)

        logger.info(f"{source_name}: {null_count} null values found.")
        return null_count
    except Exception as e:
        logger.error(f"Failed to count null values for {source_name}: {e}")
        raise

# COMMAND ----------

def count_gold_table(gold_table_name, source_name):
    """
    Count rows in a Gold business table. Returns 0 and logs a warning if the table does not exist.
    """
    full_name = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table_name}"
    try:
        df = spark.read.format("delta").table(full_name)
        count = df.count()
        logger.info(f"{full_name}: {count} rows.")
        return count
    except Exception as e:
        logger.warning(f"Could not read Gold table {full_name} for {source_name}: {e}")
        return 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Bronze and Silver Schemas Before Processing

# COMMAND ----------

# Print the schemas for every Bronze and Silver source table
for source in SOURCES:
    inspect_table(f"{CATALOG}.{BRONZE_SCHEMA}.{source}")
    inspect_table(f"{CATALOG}.{SILVER_SCHEMA}.{source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the Audit Run ID and Timestamp

# COMMAND ----------

# A single run_id and timestamp is used for every audit row inserted in this execution
run_id = str(uuid.uuid4())
run_timestamp = datetime.now()

logger.info(f"Starting audit run {run_id} at {run_timestamp}")
print(f"Run ID: {run_id}")
print(f"Run Timestamp: {run_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Metrics and Build Audit Rows

# COMMAND ----------

audit_rows = []

for source in SOURCES:
    # Default values for a failed attempt
    landing_count = 0
    bronze_count = 0
    silver_count = 0
    gold_count = 0
    duplicate_count = 0
    null_count = 0
    rejected_count = 0
    valid_count = 0
    data_quality_pct = 0.0
    status = "FAILED"
    error_msg = None

    try:
        logger.info(f"Processing source: {source}")

        # Landing CSV row count
        landing_count = count_csv_rows(f"{source}.csv")

        # Bronze row count and quality metrics
        bronze_df = spark.read.format("delta").table(f"{CATALOG}.{BRONZE_SCHEMA}.{source}")
        bronze_count = bronze_df.count()
        duplicate_count = count_duplicate_rows(bronze_df, source)
        null_count = count_null_values(bronze_df, source)

        # Silver row count
        silver_df = spark.read.format("delta").table(f"{CATALOG}.{SILVER_SCHEMA}.{source}")
        silver_count = silver_df.count()

        # Gold row count for the most relevant Gold business table
        gold_count = count_gold_table(GOLD_MAPPING[source], source)

        # Rejected rows = rows that existed in Bronze but did not make it to Silver
        rejected_count = bronze_count - silver_count

        # Valid rows = rows that reached the Silver layer
        valid_count = silver_count

        # Data quality percentage: percentage of landing rows that became valid Silver rows
        if landing_count > 0:
            data_quality_pct = 100.0 * valid_count / landing_count
        else:
            data_quality_pct = 0.0

        # Critical data quality rule:
        # Appointments must have a non-null, non-blank patient_id.
        # Check the Bronze data directly so duplicate removal does not
        # incorrectly mark the pipeline as FAILED.
        if source == "appointments":
            missing_patient_id_count = spark.sql(f"""
                SELECT COUNT(*) AS missing_patient_id_count
                FROM {CATALOG}.{BRONZE_SCHEMA}.appointments
                WHERE patient_id IS NULL
                   OR TRIM(patient_id) = ''
            """).first()["missing_patient_id_count"]

            if missing_patient_id_count > 0:
                status = "FAILED"
                error_msg = (
                    f"Critical data quality failure: "
                    f"{missing_patient_id_count} appointment record(s) "
                    f"have missing patient_id."
                )
            else:
                status = "SUCCESS"
                error_msg = None
        else:
            status = "SUCCESS"
            error_msg = None

        logger.info(f"{source}: audit metrics calculated successfully.")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to process {source}: {error_msg}")

    # Append the audit row for this source
    audit_rows.append(
        {
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "source_file": f"{source}.csv",
            "table_name": source,
            "layer_name": "source",
            "landing_record_count": landing_count,
            "bronze_record_count": bronze_count,
            "silver_record_count": silver_count,
            "gold_record_count": gold_count,
            "duplicate_record_count": duplicate_count,
            "rejected_record_count": rejected_count,
            "null_record_count": null_count,
            "valid_record_count": valid_count,
            "data_quality_percentage": data_quality_pct,
            "pipeline_status": status,
            "error_message": error_msg,
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append Audit Rows to the Monitoring Table

# COMMAND ----------

try:
    # Get the exact schema from the existing audit Delta table
    audit_schema = spark.table(AUDIT_TABLE).schema

    # Create DataFrame using the known audit-table schema
    audit_df = spark.createDataFrame(
        audit_rows,
        schema=audit_schema
    )

    print("--- Audit rows to be inserted ---")
    display(audit_df)

    # Append records - do NOT overwrite audit history
    row_count = audit_df.count()

    logger.info(
        f"Appending {row_count} audit rows to {AUDIT_TABLE}..."
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )

    logger.info("Audit rows appended successfully.")

except Exception as e:
    logger.error(f"Failed to append audit rows: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Latest Run Results

# COMMAND ----------

# Show the audit rows that were just inserted for this run
latest_run_df = (
    spark.read.format("delta").table(AUDIT_TABLE)
    .filter(F.col("run_id") == run_id)
    .orderBy(F.col("table_name"))
)

print("--- Latest run audit rows ---")
display(latest_run_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Audit Table History

# COMMAND ----------

# Show all audit records, ordered from most recent run to oldest
audit_history_df = spark.read.table(AUDIT_TABLE).orderBy(F.col("run_timestamp").desc())

print("--- Full audit history ---")
display(audit_history_df)

# COMMAND ----------

# Run the same query using Spark SQL
spark.sql(f"""
    SELECT *
    FROM {AUDIT_TABLE}
    ORDER BY run_timestamp DESC
""")   