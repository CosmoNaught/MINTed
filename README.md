# MINTed

An R package to **explore, query, visualise, and tabulate** MINT (Malaria INTervention) simulation results stored in a DuckDB database. MINTed focuses on *post-processing*: quickly slicing static inputs, plotting prevalence with intervention markers, and producing baseline-vs-future case tables.

> If you’re looking for the pipeline that **converts** RDS simulation outputs into a DuckDB database, see the companion project **segMINT**. MINTed assumes you already have a `simulation_results` table in a `.duckdb` file.

---

## Features

- **Zero-copy DB access** — open a DuckDB file and work read-only
- **R-style filter syntax** — intuitive scalar, range, and multi-value filters
- **Static input introspection** — fetch the key input parameters for any run
- **Publication-ready plots** — prevalence over time with intervention markers
- **Baseline vs future tables** — annual cases per 1000 with absolute and % change
- **Composable** — bring your own DBI/duckdb connection or just pass a file path

---

## Installation

```r
# Install from GitHub
# (Change the repo path if your remote is different)
devtools::install_github("CosmoNaught/MINTed")
```

### Dependencies

- R (>= 3.5.0)
- **DBI** (>= 1.1.0)
- **duckdb** (>= 0.8.0)
- base graphics & utils
- **spearMINT** (for connection helpers) — installed automatically if declared as a dependency

---

## Quick Start

```r
library(MINTed)
library(DBI)
library(duckdb)

# Paths
raw_db_path <- "/path/to/simulation_database.duckdb"
plot_dir    <- "/path/to/output"

# Connect (read-only)
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = raw_db_path, read_only = TRUE)

# --- Query examples ---
res_basic <- MINTed::query_database(
  con = con,
  filters = list(
    seasonal = 1,
    routine  = 1,
    eir      = c(0, 100)
  )
)
head(res_basic)

res_complex <- MINTed::query_database(
  raw_db_path = raw_db_path,  # you can pass a path instead of an open connection
  filters = list(
    seasonal     = 1,
    routine      = 0,
    eir          = c(30, 50),
    Q0           = c(0.5, 0.7),
    phi_bednets  = c(0.4, 0.8),
    irs_use      = c(0.2, 0.6),
    lsm          = c(0.2, 0.5)
  )
)
head(res_complex)

# Static inputs for one parameter set
MINTed::fetch_inputs(con, parameter_index = 1012)

# --- Plot prevalence (with intervention markers) ---
MINTed::plot_prevalence(
  con,
  parameter_index = 1012,
  start_timestep  = 6*365,
  end_timestep    = 12*365,
  output_dir      = plot_dir
)

# Combine multiple plots into a single PDF manually
pdf(file.path(plot_dir, "combined_plots.pdf"), width = 10, height = 7)
MINTed::plot_prevalence(con, parameter_index = 1012, start_timestep = 6*365, end_timestep = 12*365)
MINTed::plot_prevalence(con, parameter_index = 1013, start_timestep = 6*365, end_timestep = 12*365)
MINTed::plot_prevalence(con, parameter_index = 1014, start_timestep = 6*365, end_timestep = 12*365)
dev.off()

# --- Tabulate cases: baseline vs future ---
MINTed::tabulate_cases(
  con,
  parameter_index     = 3,
  baseline_year       = 8,
  future_years        = 9:11,
  stochastic_average  = TRUE
)

DBI::dbDisconnect(con)
```

---

## Core Functions

### `query_database()`

Query **distinct** rows for key inputs and indices using simple, composable filters.

```r
query_database(
  con = NULL,
  raw_db_path = NULL,
  filters = list(),
  limit = NULL,
  table_name = "simulation_results"
)
```

**Filters:**
- **Scalar** (`numeric`, `character`, `logical`) → equality (`col = value`)
- **Numeric length-2** (`c(lo, hi)`) → inclusive range (`col BETWEEN lo AND hi`)
- **Character vector** (`c("A", "B")`) → `IN (...)`
- Multiple filters are **AND**-ed together

**Returns:** a `data.frame` with distinct `parameter_index`, `global_index`, and the key static inputs:  
`eir, Q0, phi_bednets, seasonal, routine, dn0_use, dn0_future, itn_use, irs_use, itn_future, irs_future, lsm`

**Examples:**
```r
# Basic
query_database(con, filters = list(seasonal = 1, routine = 1))

# Ranges & multi-value
query_database(
  raw_db_path,
  filters = list(
    eir = c(30, 50),
    Q0 = c(0.5, 0.7),
    global_index = c("fileA.rds", "fileB.rds")
  ),
  limit = 100
)
```

---

### `fetch_inputs()`

Fetch a one-row table of static input parameters for a given `parameter_index` **or** `global_index`.

```r
fetch_inputs(
  con = NULL,
  raw_db_path = NULL,
  parameter_index = NULL,
  global_index = NULL,
  table_name = "simulation_results"
)
```

**Returns:** one-row `data.frame` with `eir, Q0, phi_bednets, seasonal, routine, dn0_use, dn0_future, itn_use, irs_use, itn_future, irs_future, lsm`.

---

### `plot_prevalence()`

Plot prevalence over **years**, showing individual simulations, the across-simulation **mean**, and vertical **intervention markers** for bednets, IRS, and LSM.

```r
plot_prevalence(
  con = NULL,
  raw_db_path = NULL,
  parameter_index = NULL,
  global_index = NULL,
  start_timestep = NULL,
  end_timestep = NULL,
  output_dir = NULL,
  table_name = "simulation_results",
  days_per_year = 365
)
```

**Features**
- All simulations (grey), mean trajectory (red)
- Vertical dashed markers + labels for bednet / IRS / LSM campaigns
- Optional `output_dir` writes a PDF per parameter/global index
- Convenience line at year 9 for common pre/post windows

**Tip:** To save multiple parameter sets into one PDF, open a device with `pdf()` and call `plot_prevalence()` repeatedly before `dev.off()` (see Quick Start).

---

### `tabulate_cases()`

Compute annual mean **cases_per_1000** for a **baseline year** vs **future year(s)**, returning absolute and percentage changes. Works per-simulation or as a **stochastic average** across sims that include **both** baseline and the given future year.

```r
tabulate_cases(
  con = NULL,
  raw_db_path = NULL,
  parameter_index = NULL,  # or global_index
  global_index = NULL,     # or parameter_index
  baseline_year,
  future_years,
  stochastic_average = TRUE,
  output_dir = NULL,
  table_name = "simulation_results",
  days_per_year = 365
)
```

**Returned columns (stochastic average = TRUE):**  
`parameter_index, global_index, baseline_year, future_year, baseline_cases_per_1000, future_cases_per_1000, delta_cases_per_1000, percent_change, n_sims_used`

**Year windows (0-indexed):** `year = floor(timesteps / days_per_year)`  
- Year 0 → `[0×365, 1×365)`  
- Year 1 → `[1×365, 2×365)`  
- …

---

## Database Structure (expected)

Table: **`simulation_results`**

| Column Type    | Columns                                                                 | Description                                                 |
|----------------|-------------------------------------------------------------------------|-------------------------------------------------------------|
| **Identifiers**| `parameter_index`, `simulation_index`, `global_index`                   | Simulation keys                                             |
| **Time**       | `timesteps`                                                             | Day index (integer)                                         |
| **Outcomes**   | `n_detect_lm_0_1825`, `n_age_0_1825`, `n_inc_clinical_0_36500`, `n_age_0_36500` | Raw outputs (schema from upstream simulations)              |
| **Calculated** | `prevalence`, `cases_per_1000`                                          | Derived metrics                                             |
| **Parameters** | all MINT inputs (e.g., `eir`, `Q0`, `phi_bednets`, `seasonal`, `routine`, …) | Flattened from nested structure                             |
| **Interventions** | `treatment_dt_bednet`, `treatment_dt_irs`, `treatment_dt_lsm`        | Comma-separated timesteps for campaign events               |

> The exact column set depends on your upstream schema, but the functions above only rely on the columns listed in their docs.

---

## End-to-End Example

```r
library(MINTed)
library(DBI)
library(duckdb)

# =============================================================================
# Configuration
# =============================================================================
raw_db_path <- "/home/cosmo/net/malaria/Cosmo/testaMINT/segMINT/simulation_database.duckdb"
plot_dir    <- "/home/cosmo/net/malaria/Cosmo/testaMINT/segMINT/"

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = raw_db_path, read_only = TRUE)

# =============================================================================
# Query database
# =============================================================================
res_basic <- MINTed::query_database(
  con = con,
  filters = list(
    seasonal = 1,
    routine  = 1,
    eir      = c(0, 100)
  )
)
head(res_basic)

res_complex <- MINTed::query_database(
  raw_db_path = raw_db_path,
  filters = list(
    seasonal     = 1,
    routine      = 0,
    eir          = c(30, 50),
    Q0           = c(0.5, 0.7),
    phi_bednets  = c(0.4, 0.8),
    irs_use      = c(0.2, 0.6),
    lsm          = c(0.2, 0.5)
  )
)
head(res_complex)

MINTed::fetch_inputs(con, parameter_index = 1012)

# =============================================================================
# Plot prevalence
# =============================================================================
MINTed::plot_prevalence(con, parameter_index = 1012,
                        start_timestep = 6*365, end_timestep = 12*365,
                        output_dir = plot_dir)

# Open PDF device manually for multiple plots
pdf(file.path(plot_dir, "combined_plots.pdf"), width = 10, height = 7)
MINTed::plot_prevalence(con, parameter_index = 1012, start_timestep = 6*365, end_timestep = 12*365)
MINTed::plot_prevalence(con, parameter_index = 1013, start_timestep = 6*365, end_timestep = 12*365)
MINTed::plot_prevalence(con, parameter_index = 1014, start_timestep = 6*365, end_timestep = 12*365)
dev.off()

# =============================================================================
# Tabulate cases
# =============================================================================
MINTed::tabulate_cases(
  con,
  parameter_index = 3,
  baseline_year = 8,
  future_years = 9:11,
  stochastic_average = TRUE
)

DBI::dbDisconnect(con)
```

---

## Advanced Notes & Tips

- **Bring your own connection**: If you pass `con`, `raw_db_path` is ignored. MINTed will not close your connection (except when it creates one internally).
- **Time windows**: Use `start_timestep`/`end_timestep` (in **days**) to focus plots on specific periods.
- **CSV output**: `tabulate_cases()` can optionally write a CSV to `output_dir` while also returning the data frame.
- **Performance**: DuckDB is fast; still, prefer previewing with `limit` in `query_database()` for large scans.
- **Schema checks**: Functions validate the presence of the `simulation_results` table and fail fast with clear errors.

---

## Troubleshooting

**“Table 'simulation_results' not found”**  
Ensure your DuckDB file was created by the upstream processing pipeline (e.g., segMINT) and contains that table.

**“No rows matched your filter”**  
Double-check your `parameter_index`/`global_index` and any timestep range you set.

**“Years outside data range” warnings**  
Your `baseline_year`/`future_years` may not exist in the data; adjust the requested windows.

**Permissions / paths**  
Verify `output_dir` exists and is writable when exporting plots/CSVs.

---

## Author

**CosmoNaught**  
GitHub: https://github.com/CosmoNaught

## License

MIT License