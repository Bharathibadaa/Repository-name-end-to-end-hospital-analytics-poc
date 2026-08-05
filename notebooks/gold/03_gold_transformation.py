# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Transformation Notebook
# MAGIC
# MAGIC ## Purpose
# MAGIC Transform and aggregate cleansed Silver hospital data into business-ready Gold Delta tables for analytics and reporting.
# MAGIC
# MAGIC ## Source
# MAGIC - `hospital_analytics.silver.patients`
# MAGIC - `hospital_analytics.silver.doctors`
# MAGIC - `hospital_analytics.silver.appointments`
# MAGIC - `hospital_analytics.silver.treatments`
# MAGIC - `hospital_analytics.silver.billing`
# MAGIC
# MAGIC ## Target
# MAGIC - `hospital_analytics.gold`
# MAGIC
# MAGIC ## Steps Performed
# MAGIC 1. Ensure the `hospital_analytics.gold` schema exists.
# MAGIC 2. Read all Silver Delta tables.
# MAGIC 3. Print the schema and column list of every Silver table.
# MAGIC 4. Build aggregate Gold tables:
# MAGIC    - `gold_patient_summary`
# MAGIC    - `gold_doctor_performance`
# MAGIC    - `gold_appointment_dashboard`
# MAGIC    - `gold_treatment_analysis`
# MAGIC    - `gold_hospital_revenue`
# MAGIC 5. Pre-aggregate metrics at the correct grain before joining to avoid duplicate counts.
# MAGIC 6. Use DataFrame aliases and fully qualify ambiguous columns.
# MAGIC 7. Replace null numeric values with zero using `coalesce`.
# MAGIC 8. Print schemas, display samples, and log row counts before writing.
# MAGIC 9. Write each Gold table with overwrite mode and `overwriteSchema` enabled.
# MAGIC 10. Verify each Gold table exists and log its row count.
# MAGIC 11. Log every step and handle errors with try-except blocks.

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
logger = logging.getLogger("gold_transformation")

CATALOG = "hospital_analytics"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Silver table references
silver_tables = {
    "patients": f"{CATALOG}.{SILVER_SCHEMA}.patients",
    "doctors": f"{CATALOG}.{SILVER_SCHEMA}.doctors",
    "appointments": f"{CATALOG}.{SILVER_SCHEMA}.appointments",
    "treatments": f"{CATALOG}.{SILVER_SCHEMA}.treatments",
    "billing": f"{CATALOG}.{SILVER_SCHEMA}.billing",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Gold Schema Exists

# COMMAND ----------

def ensure_gold_schema():
    """
    Create the Gold schema if it does not already exist and apply a comment.
    """
    try:
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}
            COMMENT 'Gold schema for business-ready hospital analytics tables.'
        """)
        logger.info(f"Schema {CATALOG}.{GOLD_SCHEMA} is available.")
    except Exception as e:
        logger.error(f"Failed to create or verify {CATALOG}.{GOLD_SCHEMA}: {e}")
        raise

# COMMAND ----------

ensure_gold_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Tables

# COMMAND ----------

# Read all Silver Delta tables into DataFrames
try:
    df_patients = spark.read.format("delta").table(silver_tables["patients"])
    df_doctors = spark.read.format("delta").table(silver_tables["doctors"])
    df_appointments = spark.read.format("delta").table(silver_tables["appointments"])
    df_treatments = spark.read.format("delta").table(silver_tables["treatments"])
    df_billing = spark.read.format("delta").table(silver_tables["billing"])
    logger.info("Successfully read all Silver tables.")
except Exception as e:
    logger.error(f"Failed to read Silver tables: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Silver Table Schemas

# COMMAND ----------

def inspect_silver_table(name, df):
    """
    Print the schema and column list of a Silver DataFrame.
    """
    try:
        print(f"--- {name} ---")
        df.printSchema()
        print(f"Columns: {df.columns}")
        logger.info(f"Inspected {name}: columns {df.columns}")
    except Exception as e:
        logger.error(f"Failed to inspect {name}: {e}")
        raise

# COMMAND ----------

# Display schemas and column lists for all Silver tables
inspect_silver_table(silver_tables["patients"], df_patients)
inspect_silver_table(silver_tables["doctors"], df_doctors)
inspect_silver_table(silver_tables["appointments"], df_appointments)
inspect_silver_table(silver_tables["treatments"], df_treatments)
inspect_silver_table(silver_tables["billing"], df_billing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aliased DataFrames for Joins

# COMMAND ----------

# Alias the Silver DataFrames so all column references can be fully qualified
p = df_patients.alias("p")
d = df_doctors.alias("d")
a = df_appointments.alias("a")
t = df_treatments.alias("t")
b = df_billing.alias("b")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Patient Summary

# COMMAND ----------

def build_patient_summary():
    """
    Aggregate patient-level metrics from appointments, treatments, and billing.
    Pre-aggregations are computed before joining to avoid duplicate counts.
    """
    try:
        logger.info("Building gold_patient_summary...")

        # Appointment metrics per patient (count distinct appointment IDs)
        appt_by_patient = a.groupBy(a.patient_id).agg(
            F.countDistinct(a.appointment_id).alias("total_appointments"),
            F.countDistinct(
                F.when(F.lower(F.trim(a.status)) == "completed", a.appointment_id)
            ).alias("completed_appointments"),
        )

        # Treatment metrics per patient: treatments -> appointments -> patient_id
        treatments_by_patient = (
            t.join(a, t.appointment_id == a.appointment_id, how="left")
            .groupBy(a.patient_id)
            .agg(
                F.countDistinct(t.treatment_id).alias("total_treatments"),
                F.sum(F.coalesce(t.cost, F.lit(0))).alias("total_treatment_cost"),
            )
        )

        # Billing metrics per patient: billing -> treatments -> appointments -> patient_id
        billing_by_patient = (
            b.join(t, b.treatment_id == t.treatment_id, how="left")
            .join(a, t.appointment_id == a.appointment_id, how="left")
            .groupBy(a.patient_id)
            .agg(
                F.sum(F.coalesce(b.amount, F.lit(0))).alias("total_billed_amount"),
                F.sum(
                    F.when(
                        F.lower(F.trim(b.payment_status)) == "paid",
                        F.coalesce(b.amount, F.lit(0)),
                    ).otherwise(0)
                ).alias("total_paid_amount"),
                F.sum(
                    F.when(
                        F.lower(F.trim(b.payment_status)) != "paid",
                        F.coalesce(b.amount, F.lit(0)),
                    ).otherwise(0)
                ).alias("total_pending_amount"),
            )
        )

        # Combine patient dimension with the pre-aggregated metrics
        patient_summary = (
            p.select(
                p.patient_id,
                F.concat_ws(" ", p.first_name, p.last_name).alias("patient_name"),
                p.gender,
                p.insurance_provider,
            )
            .join(appt_by_patient, p.patient_id == appt_by_patient.patient_id, how="left")
            .join(treatments_by_patient, p.patient_id == treatments_by_patient.patient_id, how="left")
            .join(billing_by_patient, p.patient_id == billing_by_patient.patient_id, how="left")
            .select(
                p.patient_id,
                F.col("patient_name"),
                p.gender,
                p.insurance_provider,
                F.col("total_appointments"),
                F.col("completed_appointments"),
                F.col("total_treatments"),
                F.col("total_treatment_cost"),
                F.col("total_billed_amount"),
                F.col("total_paid_amount"),
                F.col("total_pending_amount"),
            )
        )

        # Replace null numeric values with zero
        numeric_cols = [
            "total_appointments",
            "completed_appointments",
            "total_treatments",
            "total_treatment_cost",
            "total_billed_amount",
            "total_paid_amount",
            "total_pending_amount",
        ]
        patient_summary = patient_summary.fillna(0, subset=numeric_cols)

        return patient_summary

    except Exception as e:
        logger.error(f"Failed to build gold_patient_summary: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Doctor Performance

# COMMAND ----------

def build_doctor_performance():
    """
    Aggregate doctor-level performance metrics.
    Uses hospital_branch as the department dimension since department is not present in Silver.
    """
    try:
        logger.info("Building gold_doctor_performance...")

        # Appointment metrics per doctor
        appt_by_doctor = a.groupBy(a.doctor_id).agg(
            F.countDistinct(a.appointment_id).alias("total_appointments"),
            F.countDistinct(
                F.when(F.lower(F.trim(a.status)) == "completed", a.appointment_id)
            ).alias("completed_appointments"),
            F.countDistinct(
                F.when(F.lower(F.trim(a.status)) == "cancelled", a.appointment_id)
            ).alias("cancelled_appointments"),
        )

        # Treatment metrics per doctor: treatments -> appointments -> doctor_id
        treatments_by_doctor = (
            t.join(a, t.appointment_id == a.appointment_id, how="left")
            .groupBy(a.doctor_id)
            .agg(
                F.countDistinct(t.treatment_id).alias("total_treatments"),
                F.sum(F.coalesce(t.cost, F.lit(0))).alias("total_treatment_revenue"),
            )
        )

        # Combine doctor dimension with the pre-aggregated metrics
        doctor_performance = (
            d.select(
                d.doctor_id,
                F.concat_ws(" ", d.first_name, d.last_name).alias("doctor_name"),
                d.specialization,
                d.hospital_branch.alias("department"),
            )
            .join(appt_by_doctor, d.doctor_id == appt_by_doctor.doctor_id, how="left")
            .join(treatments_by_doctor, d.doctor_id == treatments_by_doctor.doctor_id, how="left")
            .select(
                d.doctor_id,
                F.col("doctor_name"),
                d.specialization,
                F.col("department"),
                F.col("total_appointments"),
                F.col("completed_appointments"),
                F.col("cancelled_appointments"),
                F.col("total_treatments"),
                F.col("total_treatment_revenue"),
            )
        )

        # Replace null numeric values with zero
        numeric_cols = [
            "total_appointments",
            "completed_appointments",
            "cancelled_appointments",
            "total_treatments",
            "total_treatment_revenue",
        ]
        doctor_performance = doctor_performance.fillna(0, subset=numeric_cols)

        return doctor_performance

    except Exception as e:
        logger.error(f"Failed to build gold_doctor_performance: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Appointment Dashboard

# COMMAND ----------

def build_appointment_dashboard():
    """
    Aggregate appointment metrics by date, status, and department.
    Uses hospital_branch as the department dimension.
    """
    try:
        logger.info("Building gold_appointment_dashboard...")

        # Join appointments with doctors and aggregate
        appointment_dashboard = (
            a.join(d, a.doctor_id == d.doctor_id, how="left")
            .groupBy(a.appointment_date, a.status, d.hospital_branch)
            .agg(
                F.countDistinct(a.appointment_id).alias("total_appointments"),
                F.countDistinct(
                    F.when(F.lower(F.trim(a.status)) == "completed", a.appointment_id)
                ).alias("completed_appointments"),
                F.countDistinct(
                    F.when(F.lower(F.trim(a.status)) == "cancelled", a.appointment_id)
                ).alias("cancelled_appointments"),
            )
            .withColumnRenamed("hospital_branch", "department")
        )

        # Replace null numeric values with zero
        numeric_cols = [
            "total_appointments",
            "completed_appointments",
            "cancelled_appointments",
        ]
        appointment_dashboard = appointment_dashboard.fillna(0, subset=numeric_cols)

        return appointment_dashboard

    except Exception as e:
        logger.error(f"Failed to build gold_appointment_dashboard: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Treatment Analysis

# COMMAND ----------

def build_treatment_analysis():
    """
    Aggregate treatment cost metrics by treatment type.
    """
    try:
        logger.info("Building gold_treatment_analysis...")

        treatment_analysis = t.groupBy(t.treatment_type).agg(
            F.countDistinct(t.treatment_id).alias("total_treatments"),
            F.avg(F.coalesce(t.cost, F.lit(0))).alias("average_treatment_cost"),
            F.min(t.cost).alias("minimum_treatment_cost"),
            F.max(t.cost).alias("maximum_treatment_cost"),
            F.sum(F.coalesce(t.cost, F.lit(0))).alias("total_treatment_cost"),
        )

        # Replace null numeric values with zero
        numeric_cols = [
            "total_treatments",
            "average_treatment_cost",
            "minimum_treatment_cost",
            "maximum_treatment_cost",
            "total_treatment_cost",
        ]
        treatment_analysis = treatment_analysis.fillna(0, subset=numeric_cols)

        return treatment_analysis

    except Exception as e:
        logger.error(f"Failed to build gold_treatment_analysis: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Hospital Revenue

# COMMAND ----------

def build_hospital_revenue():
    """
    Aggregate billing and revenue metrics by bill date, payment status, and payment method.
    Because only one amount column exists, paid and pending amounts are derived from payment_status.
    """
    try:
        logger.info("Building gold_hospital_revenue...")

        hospital_revenue = b.groupBy(b.bill_date, b.payment_status, b.payment_method).agg(
            F.countDistinct(b.bill_id).alias("total_bills"),
            F.sum(F.coalesce(b.amount, F.lit(0))).alias("total_billed_amount"),
            F.sum(
                F.when(
                    F.lower(F.trim(b.payment_status)) == "paid",
                    F.coalesce(b.amount, F.lit(0)),
                ).otherwise(0)
            ).alias("total_paid_amount"),
            F.sum(
                F.when(
                    F.lower(F.trim(b.payment_status)) != "paid",
                    F.coalesce(b.amount, F.lit(0)),
                ).otherwise(0)
            ).alias("total_pending_amount"),
        )

        # Replace null numeric values with zero
        numeric_cols = [
            "total_bills",
            "total_billed_amount",
            "total_paid_amount",
            "total_pending_amount",
        ]
        hospital_revenue = hospital_revenue.fillna(0, subset=numeric_cols)

        return hospital_revenue

    except Exception as e:
        logger.error(f"Failed to build gold_hospital_revenue: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function to Write and Verify Gold Tables

# COMMAND ----------

def write_gold_table(df, table_name):
    """
    Print the schema, display sample rows, log the row count,
    write the DataFrame to the Gold Delta schema with overwrite and overwriteSchema,
    then verify the table exists and log the stored row count.
    """
    try:
        full_table_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
        logger.info(f"Preparing to write {full_table_name}...")

        # Print schema before writing
        print(f"--- Schema for {table_name} ---")
        df.printSchema()

        # Display sample rows before writing
        print(f"--- Sample rows for {table_name} ---")
        display(df.limit(5))

        # Log the output row count
        output_count = df.count()
        logger.info(f"{table_name} output row count: {output_count}")
        print(f"{table_name} output row count: {output_count}")

        # Write to Gold with overwrite mode and overwriteSchema enabled
        logger.info(f"Writing {table_name} to {full_table_name}...")
        df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(full_table_name)
        logger.info(f"Successfully wrote {table_name} to {full_table_name}.")

        # Verify the table exists and retrieve its row count
        verify_df = spark.read.format("delta").table(full_table_name)
        verify_count = verify_df.count()
        logger.info(f"Verified {full_table_name}: {verify_count} rows stored.")
        print(f"Verified {full_table_name}: {verify_count} rows stored.")

        return verify_count
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Transformations

# COMMAND ----------

# Map of Gold table names to their build functions
gold_builders = {
    "gold_patient_summary": build_patient_summary,
    "gold_doctor_performance": build_doctor_performance,
    "gold_appointment_dashboard": build_appointment_dashboard,
    "gold_treatment_analysis": build_treatment_analysis,
    "gold_hospital_revenue": build_hospital_revenue,
}

# Build, write, and verify each Gold table with error handling
for table_name, builder in gold_builders.items():
    try:
        gold_df = builder()
        write_gold_table(gold_df, table_name)
    except Exception as e:
        logger.error(f"Pipeline failed at {table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All Gold tables have been built from the Silver layer, written to `hospital_analytics.gold`, and verified with row counts.
