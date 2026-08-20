# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation Notebook
# MAGIC
# MAGIC ## Purpose
# MAGIC Transform and cleanse raw data from the `hospital_analytics.bronze` schema and write the cleaned datasets into the `hospital_analytics.silver` schema.
# MAGIC
# MAGIC ## Source Tables
# MAGIC - `hospital_analytics.bronze.patients`
# MAGIC - `hospital_analytics.bronze.doctors`
# MAGIC - `hospital_analytics.bronze.appointments`
# MAGIC - `hospital_analytics.bronze.treatments`
# MAGIC - `hospital_analytics.bronze.billing`
# MAGIC
# MAGIC ## Steps Performed
# MAGIC 1. Read each Bronze Delta table.
# MAGIC 2. Remove duplicate records.
# MAGIC 3. Trim leading and trailing spaces from string columns.
# MAGIC 4. Replace null values with sensible defaults where appropriate.
# MAGIC 5. Standardize date columns to a common `DateType`.
# MAGIC 6. Write each cleansed DataFrame to the Silver Delta schema using overwrite mode.
# MAGIC 7. Log every step and handle errors with try-except blocks.

# COMMAND ----------

# Import required libraries
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType, DateType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configure logging for the notebook
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transformation")

# Source and target schemas
CATALOG = "hospital_analytics"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# List of tables to transform
TABLES = [
    "patients",
    "doctors",
    "appointments",
    "treatments",
    "billing",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing Functions

# COMMAND ----------

def remove_duplicates(df, table_name):
    """
    Remove exact duplicate rows from the DataFrame.
    """
    try:
        before = df.count()
        df = df.dropDuplicates()
        after = df.count()
        logger.info(f"{table_name}: removed {before - after} duplicate rows.")
        return df
    except Exception as e:
        logger.error(f"Failed to remove duplicates for {table_name}: {e}")
        raise

# COMMAND ----------

def trim_string_columns(df, table_name):
    """
    Trim leading and trailing spaces from all string columns.
    """
    try:
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in string_cols:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
        logger.info(f"{table_name}: trimmed {len(string_cols)} string columns.")
        return df
    except Exception as e:
        logger.error(f"Failed to trim string columns for {table_name}: {e}")
        raise

# COMMAND ----------

def replace_null_values(df, table_name):
    """
    Replace null values with sensible defaults based on column type.
    String columns are replaced with 'Unknown' and numeric columns are replaced with 0.
    """
    try:
        for field in df.schema.fields:
            col_name = field.name
            data_type = field.dataType

            if isinstance(data_type, StringType):
                df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), "Unknown").otherwise(F.col(col_name)))
            elif isinstance(data_type, NumericType):
                df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), 0).otherwise(F.col(col_name)))

        logger.info(f"{table_name}: replaced null values where appropriate.")
        return df
    except Exception as e:
        logger.error(f"Failed to replace null values for {table_name}: {e}")
        raise

# COMMAND ----------

def standardize_date_columns(df, table_name):
    """
    Standardize columns that contain 'date' in their name to a DateType.
    """
    try:
        for field in df.schema.fields:
            col_name = field.name
            data_type = field.dataType

            # Identify likely date columns by name
            if "date" in col_name.lower() and not isinstance(data_type, (DateType, TimestampType)):
                df = df.withColumn(col_name, F.to_date(F.col(col_name)))
                logger.info(f"{table_name}: standardized date column '{col_name}'.")

        return df
    except Exception as e:
        logger.error(f"Failed to standardize date columns for {table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform and Write Bronze Tables to Silver

# COMMAND ----------

def write_to_silver(df, table_name):
    """
    Write a cleansed DataFrame to the Silver Delta table using overwrite mode.
    """
    try:
        full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
        logger.info(f"Writing {table_name} to {full_table_name}...")

        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

        logger.info(f"Successfully wrote {table_name} to {full_table_name}.")
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {e}")
        raise

# COMMAND ----------

# Process each Bronze table and write the cleansed output to Silver
for table_name in TABLES:
    try:
        bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        logger.info(f"Reading {bronze_table}...")

        # Read the Bronze Delta table
        df = spark.read.format("delta").table(bronze_table)

        # Apply data cleansing steps in sequence
        df = remove_duplicates(df, table_name)
        df = trim_string_columns(df, table_name)
        df = replace_null_values(df, table_name)
        df = standardize_date_columns(df, table_name)

        # Write the cleansed DataFrame to the Silver schema
        write_to_silver(df, table_name)

    except Exception as e:
        logger.error(f"Failed to process {table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All Bronze tables have been read, cleansed, and written as Delta tables in `hospital_analytics.silver` using overwrite mode.