# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC
# MAGIC ## Purpose
# MAGIC Ingest raw CSV files from the Unity Catalog volume into the `hospital_analytics.bronze` Delta Lake schema.
# MAGIC
# MAGIC ## Source Files
# MAGIC - patients.csv
# MAGIC - doctors.csv
# MAGIC - appointments.csv
# MAGIC - treatments.csv
# MAGIC - billing.csv
# MAGIC
# MAGIC ## Steps Performed
# MAGIC 1. Read each CSV file from the volume with schema inference.
# MAGIC 2. Display the inferred schema and sample records.
# MAGIC 3. Display the row count.
# MAGIC 4. Validate null values and duplicate records.
# MAGIC 5. Write each dataset as a Delta table in `hospital_analytics.bronze` using overwrite mode.
# MAGIC 6. Log each operation and handle errors with try-except blocks.

# COMMAND ----------

# Import required libraries
import logging
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configure logging for the notebook
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_ingestion")

# Unity Catalog volume path where raw CSV files are stored
VOLUME_PATH = "/Volumes/hospital_analytics/landing/hospital_raw/"

# Target catalog and schema for the bronze layer
BRONZE_CATALOG = "hospital_analytics"
BRONZE_SCHEMA = "bronze"

# Mapping of source file names to target bronze table names
SOURCE_FILES = {
    "patients": "patients.csv",
    "doctors": "doctors.csv",
    "appointments": "appointments.csv",
    "treatments": "treatments.csv",
    "billing": "billing.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Source CSV Files into Bronze Delta Tables

# COMMAND ----------

def validate_dataframe(df, table_name):
    """
    Display schema, sample records, row count, null counts, and duplicate counts.
    """
    try:
        logger.info(f"Validating data for {table_name}...")

        # Display inferred schema
        print(f"--- Schema for {table_name} ---")
        df.printSchema()

        # Display sample records
        print(f"--- Sample records for {table_name} ---")
        display(df.limit(5))

        # Display row count
        row_count = df.count()
        print(f"--- Row count for {table_name}: {row_count} ---")

        # Count null values for each column
        null_counts = df.select(
            [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
        )
        print(f"--- Null counts for {table_name} ---")
        null_counts.show(truncate=False)

        # Count duplicate records by comparing total rows to distinct rows
        distinct_count = df.dropDuplicates().count()
        duplicate_count = row_count - distinct_count
        print(f"--- Duplicate records for {table_name}: {duplicate_count} ---")

    except Exception as e:
        logger.error(f"Validation failed for {table_name}: {e}")
        raise

# COMMAND ----------

def write_to_bronze(df, table_name):
    """
    Write a DataFrame to the bronze Delta table using overwrite mode.
    """
    try:
        full_table_name = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        logger.info(f"Writing {table_name} to {full_table_name}...")

        # Save the DataFrame as a managed Delta table in the bronze schema
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

        logger.info(f"Successfully wrote {table_name} to {full_table_name}.")
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {e}")
        raise

# COMMAND ----------

# Iterate through each source file, read, validate, and write to bronze
for table_name, file_name in SOURCE_FILES.items():
    try:
        file_path = f"{VOLUME_PATH}{file_name}"
        logger.info(f"Reading {file_name} from {file_path}...")

        # Read CSV with automatic schema inference
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Validate and inspect the DataFrame
        validate_dataframe(df, table_name)

        # Write the validated DataFrame to the bronze Delta table
        write_to_bronze(df, table_name)

    except Exception as e:
        logger.error(f"Failed to process {table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All source CSV files have been read from the landing volume, validated, and written as Delta tables in `hospital_analytics.bronze`.
