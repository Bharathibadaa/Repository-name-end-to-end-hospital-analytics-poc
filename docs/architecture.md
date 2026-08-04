# Architecture

## High-Level Architecture

```
CSV → Bronze → Silver → Gold → Power BI
```

- **CSV**: Raw source files containing hospital operational data.
- **Bronze**: Raw ingestion layer storing data as-is.
- **Silver**: Cleansed and standardized data layer.
- **Gold**: Aggregated and modeled data ready for analytics.
- **Power BI**: Visualization and reporting layer.

Technical implementation details will be added as the project progresses.
