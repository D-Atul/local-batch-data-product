# NFL Local Batch Data Product

A contract-first local batch pipeline that processes historical NFL game data into validated analytical datasets with deterministic execution, explicit data validation, and run-level evidence.

---

# Overview

This project demonstrates how a small batch data pipeline can be built with production-style engineering discipline while running locally.

The pipeline ingests raw NFL game data, validates the input through explicit data contracts, transforms the dataset into curated events, computes analytical metrics, and publishes reproducible outputs.

Each run produces a run log that records execution metadata and validation outcomes, providing traceability and audit-style evidence of pipeline behavior.

The goal of the project is to demonstrate practical data engineering patterns such as:

* contract-first data validation
* deterministic batch processing
* explicit transformation stages
* reproducible analytical outputs
* run-level execution evidence
* automated test validation via CI

---

# Architecture

The pipeline architecture is shown below.

![Pipeline Architecture](docs/architecture.png)

The system follows a contract-first batch pipeline design where data moves through clearly defined stages with validation gates between them.

Key stages include:

* raw input validation
* event transformation
* curated dataset validation
* metric computation
* output validation
* run logging and evidence generation

---

# Project Structure

```
src/
  contracts/        data validation rules
  pipelines/        transformation and metric logic
  runner/           pipeline orchestration entrypoint

tests/              validation and pipeline tests
framework/          design framework and pipeline guarantees
data/raw/           raw dataset used by the pipeline
outputs/            generated analytical metric tables
logs/               run logs generated during pipeline execution
evidence/           notebook used to inspect outputs and run logs
docs/               architecture diagram and documentation assets
```

---

# How To Run

Clone the repository.

```
git clone <repo-url>
cd nfl-local-batch-data-product
```

Create a virtual environment.

```
python -m venv .venv
source .venv/bin/activate
```

Install dependencies.

```
pip install -r requirements.txt
```

Run the pipeline.

```
python -m src.runner.run_local_batch
```

---

# What This Produces

After a successful run the pipeline generates analytical datasets:

```
outputs/metrics/

season_summaries.parquet
team_outcomes.parquet
venue_neutral_counts.parquet
```

Each run also produces execution evidence:

```
logs/run_<timestamp>.json
```

The run log records:

* pipeline start and completion timestamps
* validation results
* row counts across stages
* output artifacts produced

---

# Evidence Inspection

The notebook below can be used to inspect pipeline results.

```
evidence/evidence_notebook.ipynb
```

The notebook allows quick inspection of:

* generated metric tables
* output schema validation
* run log metadata

---

# Engineering Features

This repository demonstrates several production-oriented engineering practices:

* contract-first data validation
* deterministic local batch execution
* structured pipeline orchestration
* run-level logging and evidence generation
* automated tests using `pytest`
* continuous integration using GitHub Actions

---

# Framework

The `framework/` directory contains the design framework that defines the execution guarantees and architectural rules used to build this data product.

The framework documents the intended behavior of the pipeline independently from the implementation code.

---

# Purpose of the Project

The goal of this repository is to demonstrate how a reproducible data pipeline can be structured using clear validation boundaries, explicit transformation stages, and deterministic outputs.

The project focuses on engineering discipline rather than scale, showing how production-style patterns can be applied even in a small local batch system.
