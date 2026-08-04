# Project Structure

This repository follows a medallion architecture for the Hospital Analytics POC. Each folder is organized by responsibility and pipeline layer.

## Purpose of Every Folder

- `data/raw/` - Stores raw CSV source files before ingestion.
- `notebooks/` - Contains PySpark notebooks organized by medallion layer.
  - `notebooks/bronze/` - Ingestion notebooks.
  - `notebooks/silver/` - Cleansing and standardization notebooks.
  - `notebooks/gold/` - Aggregation and modeling notebooks.
- `sql/` - Contains SQL scripts organized by layer.
  - `sql/bronze/` - Bronze ingestion SQL scripts.
  - `sql/silver/` - Silver transformation SQL scripts.
  - `sql/gold/` - Gold aggregation SQL scripts.
  - `sql/validation/` - Data validation and quality SQL scripts.
- `config/` - Configuration files for the pipeline.
- `utils/` - Reusable utility functions and helpers.
- `tests/` - Unit and integration tests for the pipeline.
- `docs/` - Project documentation.
- `powerbi/` - Power BI report and dashboard artifacts.

## Complete Folder Tree

```
Repository-name-end-to-end-hospital-analytics-poc/
├── .gitignore
├── README.md
├── config/
│   └── .gitkeep
├── data/
│   └── raw/
│       └── .gitkeep
├── docs/
│   ├── project_overview.md
│   ├── project_structure.md
│   ├── progress_tracker.md
│   ├── data_dictionary.md
│   └── architecture.md
├── notebooks/
│   ├── bronze/
│   │   └── .gitkeep
│   ├── silver/
│   │   └── .gitkeep
│   └── gold/
│       └── .gitkeep
├── powerbi/
│   └── .gitkeep
├── sql/
│   ├── bronze/
│   │   └── .gitkeep
│   ├── silver/
│   │   └── .gitkeep
│   ├── gold/
│   │   └── .gitkeep
│   └── validation/
│       └── .gitkeep
├── tests/
│   └── .gitkeep
└── utils/
    └── .gitkeep
```
