# Project Overview

## Project Objective
This End-to-End Hospital Analytics POC demonstrates a scalable analytics pipeline that transforms raw healthcare operational data into actionable insights. The goal is to provide a reusable blueprint for building a medallion-style data lakehouse that supports reporting and visualization while keeping the implementation focused and maintainable.

## Healthcare Domain
Healthcare operations and patient management, including patients, doctors, appointments, treatments, and billing records.

## Technology Stack
- **Source files**: CSV
- **Data platform**: Databricks / Delta Lake
- **Data processing**: PySpark notebooks
- **Data transformations**: SQL
- **Visualization**: Power BI
- **Development support**: Devin AI

## High-Level Data Flow
1. CSV source files are ingested into the Bronze layer.
2. Bronze data is cleansed and standardized into the Silver layer.
3. Silver data is aggregated and modeled into the Gold layer.
4. Gold datasets are consumed by Power BI for reporting and dashboards.
