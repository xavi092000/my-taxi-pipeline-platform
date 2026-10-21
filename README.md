# NYC Taxi Pipeline with Bruin

A pedagogical data pipeline project built with Bruin, Python, SQL, and DuckDB. It ingests NYC Taxi yellow taxi data and produces a small aggregated report.

This repository is a learning exercise based on a course template, not an independently invented orchestration framework or a production deployment. It has no CI, automated tests, monitoring, or production orchestration configured.

## Origin and scope

The [pipeline configuration](pipeline/pipeline.yml) identifies its origin as a **course template**. This repository is an adapted course exercise, not an independently invented orchestration framework or a deployed production platform.

The current implementation contains Python ingestion, a CSV payment lookup, and a reporting model. There is no staging asset. These files are the reviewable deliverable.

## Where to start

- [Pipeline configuration](pipeline/pipeline.yml)
- [Python ingestion](pipeline/assets/ingestion/trips.py)
- [Payment lookup seed](pipeline/assets/ingestion/payment_lookup.csv)
- [Reporting model](pipeline/assets/reports/trips_report.sql)

## What the pipeline does

- `ingestion.trips` downloads monthly NYC TLC yellow taxi Parquet files.
- It streams each Parquet file row group by row group, selects a fixed set of columns, adds `taxi_type`, and writes the result to DuckDB without concatenating the full month in memory.
- `reports.trips_report` aggregates trips by date, taxi type, and payment type.
- `ingestion.payment_lookup` is a CSV seed included in the repository. It is not joined by the current report.
- There is no separate staging asset; the report reads directly from `ingestion.trips`.

## Repository structure

```text
my-taxi-pipeline-platform/
|-- pipeline/
|   |-- pipeline.yml
|   `-- assets/
|       |-- ingestion/
|       |   |-- trips.py
|       |   |-- requirements.txt
|       |   |-- payment_lookup.asset.yml
|       |   `-- payment_lookup.csv
|       `-- reports/
|           `-- trips_report.sql
|-- README.md
`-- .gitignore
```

## Requirements

- Bruin CLI
- Python dependencies installed by Bruin from `pipeline/assets/ingestion/requirements.txt`
- Network access to the NYC TLC data source
- A local DuckDB connection named `duckdb-default`

Bruin reads the `duckdb-default` connection from a local `.bruin.yml`. This file is ignored by Git. Example:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
```

Do not commit `.bruin.yml` or any credentials.

## Run

Validate the pipeline:

```powershell
bruin validate .\pipeline
```

Run a bounded backfill for January 2024:

```powershell
bruin run .\pipeline --start-date 2024-01-01 --end-date 2024-01-31
```

Inspect the report:

```powershell
bruin query --connection duckdb-default --query "select * from reports.trips_report limit 10"
```

The January 2024 run downloads one monthly Parquet file from the NYC TLC source. The download size depends on the source file. The pipeline writes to the local DuckDB database only.

## Validation

The pipeline validates with Bruin:

```powershell
bruin validate .\pipeline
```

Result: 3 assets validated, no issues.

A local bounded run for January 2024 completed successfully using an isolated local DuckDB configuration, because the repository does not include `.bruin.yml`.

- `ingestion.payment_lookup`: 7 rows.
- `ingestion.trips`: 2,964,624 rows.
- `reports.trips_report`: 161 rows.
- Downloaded Parquet file: 49,961,641 bytes.

This was a local DuckDB run, not a production deployment.

## Current limitations

- Yellow taxi only: `taxi_types` defaults to `["yellow"]`, and the ingestion column mapping uses yellow taxi fields such as `tpep_pickup_datetime` and `Airport_fee`. Green taxi support is not implemented.
- No staging layer: the report reads directly from `ingestion.trips`.
- `ingestion.payment_lookup` is present but unused by the report.
- The source file contains a small number of pickup timestamps outside January 2024; the report does not filter them out.
- No automated tests, CI, monitoring, or production deployment.
- The DuckDB connection is local and is not tracked in the repository.

Maintained by Felix Brillant.
