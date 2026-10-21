# NYC Taxi Pipeline with Bruin

A learning project that ingests NYC taxi data and organizes DuckDB transformations into ingestion, staging, and reporting assets.

## Origin and scope

The [pipeline configuration](pipeline/pipeline.yml) identifies its origin as a **course template**. This repository is an adapted course exercise, not an independently invented orchestration framework or a deployed production platform.

The current implementation contains Python ingestion, a payment lookup, SQL staging, and a reporting model. These files are the reviewable deliverable; the exact division between upstream template code and personal additions has not been independently reconstructed.

## Where to start

- [Pipeline configuration](pipeline/pipeline.yml)
- [Python ingestion](pipeline/assets/ingestion/trips.py)
- [Staging model](pipeline/assets/staging/trips.sql)
- [Reporting model](pipeline/assets/reports/trips_report.sql)

## Runtime requirements

Use Bruin with a local `duckdb-default` connection, the Python dependencies in [ingestion/requirements.txt](pipeline/assets/ingestion/requirements.txt), and access to the taxi data URLs used by the ingestion asset. Consult the [upstream course](https://github.com/DataTalksClub) and the Bruin documentation linked in the configuration for setup.

No public automated runtime validation is included. The template comments and local connection requirements should be reviewed before execution. Successful execution in a fresh environment has not been established by the documentation cleanup.

Maintained by Felix Brillant.
