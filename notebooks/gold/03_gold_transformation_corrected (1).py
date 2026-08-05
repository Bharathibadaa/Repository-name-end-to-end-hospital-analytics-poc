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
# MAGIC 3. Build aggregate Gold tables:
# MAGIC    - `gold_patient_summary`
# MAGIC    - `gold_doctor_performance`
# MAGIC    - `gold_appointment_dashboard`
# MAGIC    - `gold_treatment_analysis`
# MAGIC    - `gold_hospital_revenue`
# MAGIC 4. Use correct joins based on the healthcare data model.
# MAGIC 5. Pre-aggregate metrics before joining to avoid duplicate counts.
# MAGIC 6. Replace null numeric values with zero where appropriate.
# MAGIC 7. Write each Gold table with overwrite mode and overwriteSchema enabled.
# MAGIC 8. Log every step and handle errors with try-except blocks.

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
# MAGIC ## Build Gold Patient Summary

# COMMAND ----------

def build_patient_summary():
    """
    Create patient-level Gold metrics without ambiguous columns
    or duplicate aggregation.
    """
    try:
        logger.info("Building gold_patient_summary...")

        appointments_for_patient = df_appointments.select(
            F.col("appointment_id"),
            F.col("patient_id"),
            F.col("appointment_status"),
        )

        appt_by_patient = (
            appointments_for_patient
            .groupBy("patient_id")
            .agg(
                F.countDistinct("appointment_id").alias("total_appointments"),
                F.countDistinct(
                    F.when(
                        F.lower(F.trim(F.col("appointment_status"))) == "completed",
                        F.col("appointment_id"),
                    )
                ).alias("completed_appointments"),
            )
        )

        treatments_with_patient = (
            df_treatments.alias("t")
            .join(
                appointments_for_patient
                .select("appointment_id", "patient_id")
                .alias("a"),
                F.col("t.appointment_id") == F.col("a.appointment_id"),
                "left",
            )
            .select(
                F.col("a.patient_id").alias("patient_id"),
                F.col("t.treatment_id").alias("treatment_id"),
                F.col("t.treatment_cost").alias("treatment_cost"),
            )
        )

        treatments_by_patient = (
            treatments_with_patient
            .groupBy("patient_id")
            .agg(
                F.countDistinct("treatment_id").alias("total_treatments"),
                F.sum(
                    F.coalesce(F.col("treatment_cost"), F.lit(0))
                ).alias("total_treatment_cost"),
            )
        )

        billing_required = df_billing.select(
            F.col("treatment_id"),
            F.col("billed_amount"),
            F.col("paid_amount"),
        )

        billing_with_patient = (
            billing_required.alias("b")
            .join(
                df_treatments
                .select("treatment_id", "appointment_id")
                .alias("t"),
                F.col("b.treatment_id") == F.col("t.treatment_id"),
                "left",
            )
            .join(
                appointments_for_patient
                .select("appointment_id", "patient_id")
                .alias("a"),
                F.col("t.appointment_id") == F.col("a.appointment_id"),
                "left",
            )
            .select(
                F.col("a.patient_id").alias("patient_id"),
                F.col("b.treatment_id").alias("treatment_id"),
                F.col("b.billed_amount").alias("billed_amount"),
                F.col("b.paid_amount").alias("paid_amount"),
            )
        )

        billing_by_patient = (
            billing_with_patient
            .groupBy("patient_id")
            .agg(
                F.sum(
                    F.coalesce(F.col("billed_amount"), F.lit(0))
                ).alias("total_billed_amount"),
                F.sum(
                    F.coalesce(F.col("paid_amount"), F.lit(0))
                ).alias("total_paid_amount"),
            )
        )

        patient_summary = (
            df_patients
            .select(
                "patient_id",
                "patient_name",
                "gender",
                "insurance_provider",
            )
            .join(appt_by_patient, on="patient_id", how="left")
            .join(treatments_by_patient, on="patient_id", how="left")
            .join(billing_by_patient, on="patient_id", how="left")
        )

        numeric_cols = [
            "total_appointments",
            "completed_appointments",
            "total_treatments",
            "total_treatment_cost",
            "total_billed_amount",
            "total_paid_amount",
        ]

        patient_summary = patient_summary.fillna(0, subset=numeric_cols)

        logger.info("Successfully built gold_patient_summary.")
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
    """
    try:
        logger.info("Building gold_doctor_performance...")

        # Appointment metrics per doctor
        appt_by_doctor = df_appointments.groupBy("doctor_id").agg(
            F.count("*").alias("total_appointments"),
            F.sum(
                F.when(F.lower(F.trim(F.col("appointment_status"))) == "completed", 1).otherwise(0)
            ).alias("completed_appointments"),
            F.sum(
                F.when(F.lower(F.trim(F.col("appointment_status"))) == "cancelled", 1).otherwise(0)
            ).alias("cancelled_appointments"),
        )

        # Treatment metrics per doctor using the appointment -> treatment join
        treatments_with_doctor = df_treatments.join(
            df_appointments.select("appointment_id", "doctor_id"),
            on="appointment_id",
            how="left",
        )

        treatments_by_doctor = treatments_with_doctor.groupBy("doctor_id").agg(
            F.count("*").alias("total_treatments"),
            F.sum(F.coalesce(F.col("treatment_cost"), F.lit(0))).alias("total_treatment_revenue"),
        )

        # Combine with doctor dimension
        doctor_performance = (
            df_doctors.select("doctor_id", "doctor_name", "specialization", "department")
            .join(appt_by_doctor, on="doctor_id", how="left")
            .join(treatments_by_doctor, on="doctor_id", how="left")
        )

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
    """
    try:
        logger.info("Building gold_appointment_dashboard...")

        # Enrich appointments with the department from the doctor dimension
        appointments_with_dept = df_appointments.join(
            df_doctors.select("doctor_id", "department"),
            on="doctor_id",
            how="left",
        )

        appointment_dashboard = appointments_with_dept.groupBy(
            "appointment_date", "appointment_status", "department"
        ).agg(
            F.count("*").alias("total_appointments"),
            F.sum(
                F.when(F.lower(F.trim(F.col("appointment_status"))) == "completed", 1).otherwise(0)
            ).alias("completed_appointments"),
            F.sum(
                F.when(F.lower(F.trim(F.col("appointment_status"))) == "cancelled", 1).otherwise(0)
            ).alias("cancelled_appointments"),
        )

        numeric_cols = ["total_appointments", "completed_appointments", "cancelled_appointments"]
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

        treatment_analysis = df_treatments.groupBy("treatment_type").agg(
            F.count("*").alias("total_treatments"),
            F.avg(F.coalesce(F.col("treatment_cost"), F.lit(0))).alias("average_treatment_cost"),
            F.min("treatment_cost").alias("minimum_treatment_cost"),
            F.max("treatment_cost").alias("maximum_treatment_cost"),
            F.sum(F.coalesce(F.col("treatment_cost"), F.lit(0))).alias("total_treatment_cost"),
        )

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
    """
    try:
        logger.info("Building gold_hospital_revenue...")

        hospital_revenue = df_billing.groupBy(
            "bill_date", "payment_status", "payment_method"
        ).agg(
            F.count("*").alias("total_bills"),
            F.sum(F.coalesce(F.col("billed_amount"), F.lit(0))).alias("total_billed_amount"),
            F.sum(F.coalesce(F.col("paid_amount"), F.lit(0))).alias("total_paid_amount"),
            F.sum(
                F.coalesce(F.col("billed_amount"), F.lit(0)) - F.coalesce(F.col("paid_amount"), F.lit(0))
            ).alias("total_pending_amount"),
        )

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
# MAGIC ## Helper Function to Write Gold Tables

# COMMAND ----------

def write_gold_table(df, table_name):
    """
    Write a DataFrame to the Gold Delta schema using overwrite mode and overwriteSchema enabled.
    """
    try:
        full_table_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
        logger.info(f"Writing {table_name} to {full_table_name}...")

        df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(full_table_name)

        logger.info(f"Successfully wrote {table_name} to {full_table_name}.")
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

# Build and write each Gold table with error handling
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
# MAGIC All Gold tables have been built from the Silver layer and written to `hospital_analytics.gold` using overwrite mode.