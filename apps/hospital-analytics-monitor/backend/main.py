import os
import logging
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Hospital Analytics Pipeline Monitor")

warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
if not warehouse_id:
    raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set")

w = WorkspaceClient()


def run_query(statement: str, catalog: str = "hospital_analytics", schema: str = "monitoring") -> List[Dict[str, Any]]:
    logger.info("Executing SQL: %s", statement[:120].replace("\n", " "))
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        catalog=catalog,
        schema=schema,
        wait_timeout="50s"
    )
    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error or "Unknown SQL error"
        logger.error("SQL execution failed: %s", error)
        raise HTTPException(status_code=500, detail=f"Databricks SQL error: {error}")
    columns = [c.name for c in response.manifest.schema.columns]
    rows = response.result.data_array or []
    return [dict(zip(columns, row)) for row in rows]


LATEST_CTE = """
WITH latest AS (
  SELECT run_id
  FROM hospital_analytics.monitoring.pipeline_audit
  ORDER BY run_timestamp DESC, run_id DESC
  LIMIT 1
)
""".strip()


@app.get("/api/health")
def health_check():
    return {"status": "healthy"}


@app.get("/api/overview")
def overview():
    kpi_sql = f"""
    {LATEST_CTE}
    SELECT
      COUNT(DISTINCT p.source_file) AS files_received,
      SUM(p.bronze_record_count) AS bronze_records,
      SUM(p.silver_record_count) AS silver_valid,
      SUM(p.rejected_record_count) AS records_removed,
      100.0 * SUM(p.valid_record_count) / SUM(p.landing_record_count) AS dq_pct,
      MAX(p.run_timestamp) AS run_timestamp,
      CASE
        WHEN SUM(CASE WHEN p.pipeline_status != 'SUCCESS' THEN 1 ELSE 0 END) > 0 THEN 'FAILED'
        ELSE 'HEALTHY'
      END AS pipeline_status
    FROM hospital_analytics.monitoring.pipeline_audit p
    JOIN latest l ON p.run_id = l.run_id
    """
    kpi = run_query(kpi_sql)[0]

    flow_sql = f"""
    {LATEST_CTE}
    SELECT 'Landing' AS stage, 'CSV Files' AS label, CAST(COUNT(DISTINCT p.source_file) AS STRING) AS value
    FROM hospital_analytics.monitoring.pipeline_audit p JOIN latest l ON p.run_id = l.run_id
    UNION ALL
    SELECT 'Bronze', 'Raw Ingestion', CAST(SUM(p.bronze_record_count) AS STRING)
    FROM hospital_analytics.monitoring.pipeline_audit p JOIN latest l ON p.run_id = l.run_id
    UNION ALL
    SELECT 'Silver', 'Cleaned & Validated', CAST(SUM(p.silver_record_count) AS STRING)
    FROM hospital_analytics.monitoring.pipeline_audit p JOIN latest l ON p.run_id = l.run_id
    UNION ALL
    SELECT 'Silver', 'Records Removed', CAST(SUM(p.rejected_record_count) AS STRING)
    FROM hospital_analytics.monitoring.pipeline_audit p JOIN latest l ON p.run_id = l.run_id
    UNION ALL
    SELECT 'Gold', 'Business Ready Tables', '5'
    UNION ALL
    SELECT 'Monitoring', 'Latest DQ %', CAST(ROUND(100.0 * SUM(p.valid_record_count) / SUM(p.landing_record_count), 2) AS STRING)
    FROM hospital_analytics.monitoring.pipeline_audit p JOIN latest l ON p.run_id = l.run_id
    UNION ALL
    SELECT 'Monitoring', 'Latest Status',
      CASE
        WHEN SUM(CASE WHEN p.pipeline_status != 'SUCCESS' THEN 1 ELSE 0 END) > 0 THEN 'FAILED'
        ELSE 'HEALTHY'
      END
    FROM hospital_analytics.monitoring.pipeline_audit p JOIN latest l ON p.run_id = l.run_id
    """
    flow = run_query(flow_sql)
    return {"kpi": kpi, "pipeline_flow": flow}


@app.get("/api/datasets")
def datasets():
    sql = f"""
    {LATEST_CTE}
    SELECT
      p.source_file AS dataset,
      p.bronze_record_count,
      p.silver_record_count,
      p.rejected_record_count,
      p.duplicate_record_count,
      p.null_record_count,
      p.data_quality_percentage,
      p.pipeline_status
    FROM hospital_analytics.monitoring.pipeline_audit p
    JOIN latest l ON p.run_id = l.run_id
    ORDER BY p.table_name
    """
    return {"datasets": run_query(sql)}


@app.get("/api/run-history")
def run_history():
    sql = """
    SELECT
      p.run_id,
      MAX(p.run_timestamp) AS run_timestamp,
      CASE WHEN SUM(CASE WHEN p.pipeline_status != 'SUCCESS' THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'HEALTHY' END AS status,
      SUM(p.bronze_record_count) AS bronze_records,
      SUM(p.silver_record_count) AS silver_records,
      SUM(p.rejected_record_count) AS records_removed,
      100.0 * SUM(p.valid_record_count) / SUM(p.landing_record_count) AS dq_pct
    FROM hospital_analytics.monitoring.pipeline_audit p
    GROUP BY p.run_id
    ORDER BY MAX(p.run_timestamp) DESC
    LIMIT 7
    """
    return {"runs": run_query(sql)}


@app.get("/api/dq-trend")
def dq_trend():
    sql = """
    SELECT
      MAX(p.run_timestamp) AS run_timestamp,
      100.0 * SUM(p.valid_record_count) / SUM(p.landing_record_count) AS overall_dq_pct
    FROM hospital_analytics.monitoring.pipeline_audit p
    GROUP BY p.run_id
    ORDER BY MAX(p.run_timestamp) ASC
    """
    return {"trend": run_query(sql)}


@app.get("/api/gold-outputs")
def gold_outputs():
    sql = """
    SELECT 'gold_patient_summary' AS gold_table, COUNT(*) AS row_count FROM hospital_analytics.gold.gold_patient_summary
    UNION ALL
    SELECT 'gold_doctor_performance', COUNT(*) FROM hospital_analytics.gold.gold_doctor_performance
    UNION ALL
    SELECT 'gold_appointment_dashboard', COUNT(*) FROM hospital_analytics.gold.gold_appointment_dashboard
    UNION ALL
    SELECT 'gold_treatment_analysis', COUNT(*) FROM hospital_analytics.gold.gold_treatment_analysis
    UNION ALL
    SELECT 'gold_hospital_revenue', COUNT(*) FROM hospital_analytics.gold.gold_hospital_revenue
    """
    return {"gold": run_query(sql, catalog="hospital_analytics", schema="gold")}


static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


@app.get("/{full_path:path}")
def serve_react(full_path: str):
    index_html = os.path.join(static_dir, "index.html")
    if os.path.exists(index_html):
        return FileResponse(index_html)
    raise HTTPException(status_code=404, detail="Frontend not built. Run npm run build first.")
