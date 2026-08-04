# Environment Configuration

## Project Name

Hospital Analytics POC

## Folder Structure

The repository follows a medallion-style layout:

- `data/raw/` - Raw CSV source files
- `notebooks/` - PySpark notebooks for Bronze, Silver, and Gold layers
- `sql/` - SQL scripts for each medallion layer and validation
- `config/` - Configuration constants and environment documentation
- `utils/` - Reusable utility functions
- `tests/` - Unit and integration tests
- `docs/` - Project documentation
- `powerbi/` - Power BI report artifacts

## DBFS Raw Path

Raw source files are expected at:

```
dbfs:/FileStore/hospital_analytics/raw/
```

## Bronze Layer

- **Database**: `bronze`
- **Purpose**: Raw ingestion of source files

## Silver Layer

- **Database**: `silver`
- **Purpose**: Cleansed and standardized data

## Gold Layer

- **Database**: `gold`
- **Purpose**: Aggregated and modeled data ready for analytics
