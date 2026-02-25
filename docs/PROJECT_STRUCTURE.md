# Project Structure

This document explains the repository layout and responsibility of each folder.

## Directory Layout

- `data/`  
  Raw and incremental CSV datasets used for ingestion and local validation.

- `notebooks/`  
  Databricks notebooks for setup, imports, and Bronze-layer ingestion.

- `src/dlt/transformations/`  
  Python modules used by Delta Live Tables for Silver/Gold transformations.

## Naming Conventions

- Notebooks are prefixed with execution order: `00_`, `01_`, `02_`, etc.
- DLT scripts are grouped by business entity (for example: `flights.py`, `airports.py`).
- Shared logic should be extracted into reusable modules under `src/` as the project grows.

## Recommended Next Folders

For scaling this codebase, add:

- `tests/` for unit/integration checks
- `conf/` for environment-specific configurations
- `infra/` for Databricks bundle or Terraform assets
