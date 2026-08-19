import { useState, useEffect } from 'react'
import { BrowserRouter, Routes, Route, NavLink, Outlet } from 'react-router-dom'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  LineChart, Line, PieChart, Pie, Cell
} from 'recharts'

const COLORS = ['#10b981', '#ef4444', '#f59e0b', '#3b82f6']

function useFetch(url) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    fetch(url)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(d => { if (!cancelled) { setData(d); setLoading(false) } })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })
    return () => { cancelled = true }
  }, [url])
  return { data, loading, error }
}

function fmt(value) {
  if (value === null || value === undefined) return '-'
  const n = Number(value)
  if (!isNaN(n)) return Number.isInteger(n) ? n.toLocaleString() : n.toFixed(2)
  return value
}

function fmtTs(value) {
  if (!value) return '-'
  const d = new Date(value)
  return isNaN(d.getTime()) ? value : d.toLocaleString()
}

function KPICard({ label, value, subtitle, color = 'blue' }) {
  return (
    <div className="kpi-card">
      <div className="kpi-label">{label}</div>
      <div className={`kpi-value ${color}`}>{fmt(value)}</div>
      {subtitle && <div className="kpi-subtitle">{subtitle}</div>}
    </div>
  )
}

function DataTable({ columns, rows, getRowClass }) {
  return (
    <table className="data-table">
      <thead>
        <tr>{columns.map((c, i) => <th key={i}>{c.label}</th>)}</tr>
      </thead>
      <tbody>
        {rows.map((row, rIdx) => (
          <tr key={rIdx} className={getRowClass ? getRowClass(row) : ''}>
            {columns.map((c, cIdx) => {
              const value = row[c.key]
              if (c.render) return <td key={cIdx}>{c.render(value, row)}</td>
              return <td key={cIdx}>{fmt(value)}</td>
            })}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function StatusPill({ status }) {
  const healthy = status === 'HEALTHY' || status === 'SUCCESS'
  return <span className={`status-pill ${healthy ? 'healthy' : 'failed'}`}>{status}</span>
}

function Loading() { return <div className="loading">Loading data…</div> }
function ErrorBox({ msg }) { return <div className="error">Error: {msg}</div> }

function Sidebar() {
  const links = [
    { to: '/', label: 'Overview' },
    { to: '/datasets', label: 'Dataset Tracking' },
    { to: '/layers', label: 'Layer Outputs' },
    { to: '/dq', label: 'Data Quality' },
    { to: '/runs', label: 'Pipeline Runs' },
    { to: '/gold', label: 'Gold Outputs' },
  ]
  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <h2>Hospital Analytics</h2>
        <p>Pipeline Monitor</p>
      </div>
      <nav className="sidebar-nav">
        {links.map(l => (
          <NavLink key={l.to} to={l.to} end={l.to === '/'}>{l.label}</NavLink>
        ))}
      </nav>
      <div className="sidebar-footer">Databricks App • hospital-analytics-monitor</div>
    </aside>
  )
}

function HeaderInfo() {
  const { data, loading, error } = useFetch('/api/overview')
  const kpi = data?.kpi || {}
  const status = kpi.pipeline_status || 'LOADING'
  return (
    <div className="main-header">
      <div className="main-header-title">
        <h1>Hospital Analytics – Pipeline Monitoring</h1>
        <p>End-to-End Data Pipeline Health and Data Quality</p>
      </div>
      <div className="main-header-status">
        {!loading && !error && (
          <>
            <div className="timestamp">Latest Run: {fmtTs(kpi.run_timestamp)}</div>
            <span className={`badge ${status === 'HEALTHY' ? 'healthy' : 'failed'}`}>
              {status}
            </span>
          </>
        )}
      </div>
    </div>
  )
}

function Layout() {
  return (
    <div className="app">
      <Sidebar />
      <div className="main">
        <HeaderInfo />
        <div className="content">
          <Outlet />
        </div>
      </div>
    </div>
  )
}

function PipelineFlowCards() {
  const { data, loading, error } = useFetch('/api/overview')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const flow = data?.pipeline_flow || []
  return (
    <section style={{ marginBottom: 28 }}>
      <h2 className="section-title">Pipeline Flow</h2>
      <div className="pipeline-flow">
        {flow.map((f, i) => (
          <div className="flow-card" key={i}>
            <div className="flow-stage">{f.stage}</div>
            <div className="flow-label">{f.label}</div>
            <div className="flow-value">{f.value}</div>
          </div>
        ))}
      </div>
    </section>
  )
}

function RecordsByLayerChart() {
  const { data, loading, error } = useFetch('/api/datasets')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const rows = data?.datasets || []
  const chartData = rows.map(r => ({ name: r.dataset, Bronze: r.bronze_record_count, Silver: r.silver_record_count }))
  return (
    <div className="card">
      <h2 className="section-title">Records by Layer</h2>
      <div className="chart-wrapper chart-sm">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} />
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="Bronze" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            <Bar dataKey="Silver" fill="#10b981" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function DQBreakdownChart() {
  const { data, loading, error } = useFetch('/api/datasets')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const rows = data?.datasets || []
  const total = rows.reduce((acc, r) => ({
    valid: acc.valid + Number(r.silver_record_count || 0),
    removed: acc.removed + Number(r.rejected_record_count || 0),
    duplicate: acc.duplicate + Number(r.duplicate_record_count || 0),
    null: acc.null + Number(r.null_record_count || 0)
  }), { valid: 0, removed: 0, duplicate: 0, null: 0 })
  const chartData = [
    { name: 'Valid Records', value: total.valid },
    { name: 'Rejected Records', value: total.removed },
    { name: 'Duplicate Records', value: total.duplicate },
    { name: 'Null Records', value: total.null }
  ]
  return (
    <div className="card">
      <h2 className="section-title">Data Quality Breakdown</h2>
      <div className="chart-wrapper chart-sm">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Tooltip />
            <Pie data={chartData} dataKey="value" nameKey="name" innerRadius={60} outerRadius={90} paddingAngle={3}>
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Legend />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function DQTrendChart() {
  const { data, loading, error } = useFetch('/api/dq-trend')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const trend = (data?.trend || []).map(t => ({ ts: new Date(t.run_timestamp).toLocaleString(), dq: Number(t.overall_dq_pct) }))
  return (
    <div className="card">
      <h2 className="section-title">DQ Trend</h2>
      <div className="chart-wrapper chart-sm">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={trend} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} />
            <XAxis dataKey="ts" tick={{ fontSize: 11 }} angle={-20} height={50} />
            <YAxis domain={[0, 100]} tickFormatter={v => `${v}%`} />
            <Tooltip formatter={v => `${Number(v).toFixed(2)}%`} />
            <Line type="monotone" dataKey="dq" stroke="#2563eb" strokeWidth={3} dot={{ r: 3 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function Overview() {
  const { data, loading, error } = useFetch('/api/overview')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const kpi = data?.kpi || {}
  const statusColor = kpi.pipeline_status === 'HEALTHY' ? 'green' : 'red'
  return (
    <>
      <section>
        <h2 className="section-title">Key Performance Indicators</h2>
        <div className="kpi-row">
          <KPICard label="Files Received" value={kpi.files_received} subtitle="CSV files in latest run" color="blue" />
          <KPICard label="Bronze Records" value={kpi.bronze_records} subtitle="Raw ingested records" color="blue" />
          <KPICard label="Silver Valid" value={kpi.silver_valid} subtitle="Cleaned valid records" color="green" />
          <KPICard label="Rejected Records" value={kpi.records_removed} subtitle="Removed in Silver" color="red" />
          <KPICard label="Data Quality %" value={kpi.dq_pct} subtitle="Overall latest DQ" color="green" />
          <KPICard label="Pipeline Status" value={kpi.pipeline_status} subtitle={kpi.pipeline_status === 'HEALTHY' ? 'All systems healthy' : 'Failures detected'} color={statusColor} />
        </div>
      </section>
      <PipelineFlowCards />
      <div className="two-col">
        <DatasetTrackingTable />
        <RecordsByLayerChart />
      </div>
      <div className="two-col">
        <DQBreakdownChart />
        <DQTrendChart />
      </div>
    </>
  )
}

function DatasetTrackingTable({ showTitle = true }) {
  const { data, loading, error } = useFetch('/api/datasets')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const columns = [
    { key: 'dataset', label: 'Dataset' },
    { key: 'bronze_record_count', label: 'Bronze' },
    { key: 'silver_record_count', label: 'Silver Valid' },
    { key: 'rejected_record_count', label: 'Rejected' },
    { key: 'duplicate_record_count', label: 'Duplicates' },
    { key: 'null_record_count', label: 'Nulls' },
    { key: 'data_quality_percentage', label: 'DQ %' },
    { key: 'pipeline_status', label: 'Status', render: v => <StatusPill status={v} /> }
  ]
  return (
    <div className="card">
      {showTitle && <h2 className="section-title">Dataset Tracking - Latest Run</h2>}
      <DataTable columns={columns} rows={data?.datasets || []} />
    </div>
  )
}

function DatasetTrackingPage() {
  return <DatasetTrackingTable />
}

function LayerOutputsPage() {
  return (
    <>
      <PipelineFlowCards />
      <DatasetTrackingTable />
    </>
  )
}

function DataQualityPage() {
  const { data, loading, error } = useFetch('/api/datasets')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const rows = data?.datasets || []
  const columns = [
    { key: 'dataset', label: 'Dataset' },
    { key: 'duplicate_record_count', label: 'Duplicates' },
    { key: 'null_record_count', label: 'Nulls' },
    { key: 'rejected_record_count', label: 'Rejected' },
    { key: 'silver_record_count', label: 'Valid Records' },
    { key: 'data_quality_percentage', label: 'DQ %' }
  ]
  return (
    <>
      <div className="two-col">
        <DQBreakdownChart />
        <DQTrendChart />
      </div>
      <div className="card">
        <h2 className="section-title">Quality Breakdown by Dataset</h2>
        <DataTable columns={columns} rows={rows} />
      </div>
    </>
  )
}

function PipelineRunsPage() {
  const { data, loading, error } = useFetch('/api/run-history')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const columns = [
    { key: 'run_id', label: 'Run ID' },
    { key: 'run_timestamp', label: 'Run Timestamp', render: v => fmtTs(v) },
    { key: 'status', label: 'Status', render: v => <StatusPill status={v} /> },
    { key: 'bronze_records', label: 'Bronze Records' },
    { key: 'silver_records', label: 'Silver Valid' },
    { key: 'records_removed', label: 'Rejected' },
    { key: 'dq_pct', label: 'DQ %' }
  ]
  return (
    <div className="card">
      <h2 className="section-title">Recent Pipeline Runs</h2>
      <DataTable columns={columns} rows={data?.runs || []} />
    </div>
  )
}

function GoldOutputsPage() {
  const { data, loading, error } = useFetch('/api/gold-outputs')
  if (loading) return <Loading />
  if (error) return <ErrorBox msg={error} />
  const columns = [
    { key: 'gold_table', label: 'Gold Table' },
    { key: 'row_count', label: 'Row Count' }
  ]
  return (
    <div className="card">
      <h2 className="section-title">Gold Outputs</h2>
      <DataTable columns={columns} rows={data?.gold || []} />
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Overview />} />
          <Route path="datasets" element={<DatasetTrackingPage />} />
          <Route path="layers" element={<LayerOutputsPage />} />
          <Route path="dq" element={<DataQualityPage />} />
          <Route path="runs" element={<PipelineRunsPage />} />
          <Route path="gold" element={<GoldOutputsPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App
