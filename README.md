# Databricks PySpark + Delta Live Tables Project

A production-style data engineering project built on **Databricks**, using **PySpark** and **Delta Live Tables (DLT)** to process airline booking data through Bronze, Silver, and Gold layers.

## Project Overview

This repository demonstrates a medallion architecture workflow:

- **Bronze**: raw data ingestion from CSV source files.
- **Silver**: cleaned and standardized dimension and fact tables.
- **Gold**: curated analytics-ready outputs for reporting and downstream BI.

The project includes Databricks notebooks for environment setup and Bronze ingestion, plus Python DLT transformation scripts for Silver/Gold layers.

## Tech Stack

- Databricks
- Apache Spark / PySpark
- Delta Live Tables (DLT)
- Delta Lake

## Repository Structure

```text
.
├── data/                          # Source datasets (CSV)
├── notebooks/                     # Databricks notebooks
│   ├── 00_setup.ipynb
│   ├── 01_imports.ipynb
│   └── 02_bronze_layer.ipynb
├── src/
│   └── dlt/
│       └── transformations/       # DLT Python transformations
│           ├── airports.py
│           ├── bookings.py
│           ├── flights.py
│           ├── gold_fact.py
│           └── passengers.py
└── README.md
```

## Data Model

### Dimensions
- Airports
- Flights
- Passengers

### Fact
- Bookings

The pipeline supports both full and incremental source patterns for selected entities.

## Running in Databricks

1. Import this repository into a Databricks Repo.
2. Open and run notebooks in order:
   - `notebooks/00_setup.ipynb`
   - `notebooks/01_imports.ipynb`
   - `notebooks/02_bronze_layer.ipynb`
3. Create a Delta Live Tables pipeline and point it to:
   - `src/dlt/transformations/`
4. Configure storage/catalog targets and run the pipeline.

## Operational Notes

- Keep schema changes versioned through the DLT transformation scripts.
- Validate data quality in Silver before promoting to Gold.
- Use Unity Catalog + managed storage for production deployments.

## Future Enhancements

- Add automated data quality expectations in DLT.
- Add CI checks for notebook and PySpark code quality.
- Add dbt models for semantic/business-layer transformations.

---

If you'd like, I can also add a **`databricks.yml` bundle scaffold** and **CI workflow** so this project is deployment-ready.
