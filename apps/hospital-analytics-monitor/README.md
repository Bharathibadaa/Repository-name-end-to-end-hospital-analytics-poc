# Hospital Analytics Pipeline Monitor

A Databricks App that provides a production-style web dashboard for monitoring the Hospital Analytics POC pipeline.

## Architecture

- **Frontend:** React + Vite + Recharts
- **Backend:** FastAPI (Python)
- **Data:** Databricks SQL Statement Execution API against the Serverless Starter Warehouse
- **Auth:** Databricks-managed service principal credentials (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`)

## Structure

```
.
├── app.yaml                  # Databricks Apps runtime configuration
├── manifest.yaml             # App metadata
├── requirements.txt          # Python dependencies
├── package.json              # Node.js build scripts and frontend dependencies
├── backend/
│   └── main.py               # FastAPI app with API endpoints and static file serving
└── frontend/
    ├── index.html
    ├── vite.config.js
    └── src/
        ├── main.jsx
        ├── App.jsx
        └── index.css
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/health` | Health check |
| `GET /api/overview` | Latest-run KPIs and pipeline flow metrics |
| `GET /api/datasets` | Latest-run dataset tracking table |
| `GET /api/run-history` | Recent pipeline runs (max 7) |
| `GET /api/dq-trend` | Historical DQ trend by run |
| `GET /api/gold-outputs` | Current Gold table row counts |

## Environment Variables

The app expects the following at runtime (Databricks Apps sets defaults automatically):

- `DATABRICKS_HOST` — workspace URL
- `DATABRICKS_CLIENT_ID` — service principal client ID
- `DATABRICKS_CLIENT_SECRET` — service principal client secret
- `DATABRICKS_WAREHOUSE_ID` — SQL warehouse ID
- `DATABRICKS_APP_PORT` — runtime port

## Local Development

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Install Node.js dependencies:
   ```bash
   npm install
   ```

3. Start the FastAPI backend:
   ```bash
   uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
   ```

4. In another terminal, start the frontend dev server:
   ```bash
   npm run dev
   ```

5. Open `http://localhost:5173`.

## Production Build

```bash
npm run build
uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

## Databricks Apps Deployment

Deploy from the `apps/hospital-analytics-monitor` directory using the Databricks CLI:

```bash
databricks apps deploy hospital-analytics-monitor --source-code-path "/Workspace/Users/<user>/hospital-analytics-monitor" --mode SNAPSHOT
```

Or use the Databricks Apps UI to deploy from a workspace folder or Git repository.
