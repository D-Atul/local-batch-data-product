# NFL Events Data Product — Local Batch Framework

## Purpose
Design and deliver a local batch data product for historical NFL game events with clear guarantees, controlled transformations, observability, and reproducibility.

This framework defines rules and scope before analysis and does not change during execution.

---

## Non-Goals
- No forecasting, prediction, or optimisation
- No causal claims
- No betting, strategy, or ranking recommendations
- No silent data fixing or imputation
- No franchise normalisation across eras

---

## PHASE A — PRODUCT DEFINITION

### 1) Product Charter
- **Product:** NFL Historical Events (Local Batch)
- **Owner:** Data Engineering (Portfolio Project)
- **Consumers:** Analysts, reporting pipelines, downstream batch jobs

**Supported use cases:**
- Descriptive historical summaries
- Dataset completeness and reliability assessment
- Metric prototyping under strict constraints

**Outputs:**
- Deterministic batch summary tables
- Validation and run logs

**Freshness:**
- Static historical dataset with future scheduled rows possible

---

### 2) Source Trust & Lineage
- **Source:** Kaggle (distribution platform)
- **System of record:** Unknown / compiled historical sources

**Trust posture:**
- Neutral
- Outputs are conditional on internal dataset consistency
- No external verification assumed

---

### 3) Data Model & Grain

**Entities:**
- **Events:** game-level records (primary fact entity)
- **Teams:** team-level metadata (reference entity)

**Grain:**
- Events: one row per match
- Teams: one row per team

**Identifiers:**
- Events: no explicit primary key; composite identifiers assumed but not enforced
- Teams: team name assumed unique

> **Clarification (execution scope):**  
> While the Teams entity is defined in the data model, ** metric computation
> operates exclusively on the Events entity**.  
> The Teams dataset is treated as **reference-only** and is not joined or
> enforced via contracts in outputs.

---

## PHASE B — CONTRACTS & GUARANTEES

### 4) Data Contracts
Contracts define:
- Required columns per metric
- Data types and nullability rules
- Allowed value domains
- Semantic meaning of fields

**Principles:**
- Contracts are fail-fast and stop execution on violation
- Contracts are metric-scoped
- No silent coercion or implicit fixes

> **Scope note:**  
> Contracts are enforced **only for datasets that materially affect published
> outputs**. Reference-only datasets may be inspected without active contracts.

---

### 5) Quality Gates

**Structural:**
- Files must load without ingestion failure
- Exact duplicate rows are **reported but not removed** for contracted datasets; deduplication is **explicitly out of scope**

**Completeness:**
- Metrics define their own required completeness
- Sparse fields are allowed but coverage must be reported

---

## PHASE C — METRIC SCOPE (DEFINED PRE-ANALYSIS)

### 6) Eligible Metric Categories
Subject to validation, the dataset is expected to support:
- Match counts and participation metrics
- Win/loss outcome summaries (where scores exist)
- Venue usage and neutrality summaries
- Temporal distributions (seasonal, yearly)
- Dataset completeness and coverage metrics
- Event-type frequency summaries (regular season, playoffs)

**Explicitly excluded:**
- Predictive or forward-looking metrics
- Causal inference
- Prescriptive rankings or optimisation

> **Constraint:**  
> All metrics must be derivable from the Events entity alone, without
> enrichment from reference datasets.

---

### 6.1 Reconciliation Invariants
At least one invariant is enforced to validate end-to-end correctness, such as:
- Total games = wins + losses (+ ties where applicable)
- Event counts are preserved across transform stages

Violations are treated as **hard failures**.

---

## PHASE D — LOCAL BATCH PIPELINE DESIGN

### 7) Batch Stages
1. Ingest (raw, read-only)
2. Validate (contracts)
3. Transform (minimal, justified)
4. Publish (summary outputs)
5. Log (run metadata)

**Design principles:**
- Idempotent reruns
- Deterministic outputs
- No destructive transformations

---

### 8) Transformation Policy

**Allowed:**
- Non-destructive date parsing
- Type enforcement via contracts; violations fail-fast and are logged
- Derived helper flags (non-destructive)

**Disallowed by default:**
- Null filling
- Deduplication
- Text normalisation
- Identity merging

---

## PHASE E — OBSERVABILITY & RELIABILITY

### 9) Run Logging
Each run records:
- Run ID and timestamp
- Input sources
- Row counts
- Validation outcomes
- Output locations

---

### 10) Error Handling & Failure Semantics
- **Hard failures:** contract violations immediately stop execution
- **Soft failures:** completeness gaps and coverage issues are reported but do not block execution
- **No partial publish:** outputs are written only after successful validation
- **Recovery:** rerun after correcting inputs or logic

---

### 11) Testing
- Runtime contract validation that fails execution on violation
- Successful end-to-end execution via the batch runner (no separate unit test suite)

---

### 12) Reproducibility & Determinism
- Raw data preserved
- Versioned contracts
- Deterministic reruns: identical inputs produce identical outputs
- No time-dependent logic or hidden state
- Reruns overwrite outputs atomically

---

## Known Limitations
- No deduplication or late-arriving data handling
- No external reference joins
- No backfills or incremental processing
- Static historical scope only

---

## PHASE F — REVIEW & GOVERNANCE

### 13) Evidence Artifacts
- Raw data (excluded from version control)
- Contracts (for active datasets)
- Run logs and validation outputs
- Output tables
- Documentation and evidence notebook

---

### 14) Framework Governance
- This document is versioned
- Analytical findings do not alter this framework
- Validated execution decisions may be incorporated in **v1.1** with a change log
