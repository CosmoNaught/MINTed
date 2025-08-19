#' Tabulate annual cases_per_1000 vs a baseline year
#'
#' @description
#' For a given parameter set (by `parameter_index` OR `global_index`), compute the
#' annual mean of `cases_per_1000` for a baseline year and one or more future years,
#' then report absolute and percent changes relative to baseline.
#'
#' Windows are year-aligned as:
#'   year y -> timesteps in [y*days_per_year, (y+1)*days_per_year)
#' (with timesteps assumed to start at 1).
#'
#' @param con Optional DuckDB connection object. If provided, `raw_db_path` is ignored.
#' @param raw_db_path Path to the .duckdb file. Required if `con` is not provided.
#' @param parameter_index Integer (optional). One of `parameter_index` OR `global_index` must be provided.
#' @param global_index Character (optional). One of `parameter_index` OR `global_index` must be provided.
#' @param baseline_year Integer (>= 0). Year used as the baseline (e.g., 2 means days [2*365, 3*365)).
#' @param future_years Integer vector. Years strictly greater than `baseline_year`
#'   (e.g., c(4,5,6)). Any years <= baseline are dropped with a warning.
#' @param stochastic_average Logical. If TRUE (default), average across simulations for
#'   each year using only simulations that have data for both the baseline and that
#'   future year. If FALSE, return per-simulation rows.
#' @param output_dir Optional directory to write a CSV summary. If NULL/"" no file is written.
#' @param table_name Table name in DuckDB. Default "simulation_results".
#' @param days_per_year Numeric, default 365.
#'
#' @return
#' If `stochastic_average = TRUE`: a data.frame with one row per future year and columns:
#'   parameter_index, global_index, baseline_year, future_year, baseline_cases_per_1000,
#'   future_cases_per_1000, delta_cases_per_1000, percent_change, n_sims_used.
#'
#' If `stochastic_average = FALSE`: one row per (simulation_index, future_year) with the same
#'   metrics plus `simulation_index`.
#'
#' If `output_dir` is provided, a CSV is written and the function still returns the data.frame.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbExistsTable dbGetQuery dbQuoteIdentifier dbQuoteString
#' @importFrom duckdb duckdb
tabulate_cases <- function(con = NULL,
                           raw_db_path = NULL,
                           parameter_index = NULL,
                           global_index = NULL,
                           baseline_year,
                           future_years,
                           stochastic_average = TRUE,
                           output_dir = NULL,
                           table_name = "simulation_results",
                           days_per_year = 365) {

  if (is.null(con)) {
    if (is.null(raw_db_path)) stop("Either con or raw_db_path must be provided")
    stopifnot(file.exists(raw_db_path))
    con <- dbConnect(duckdb(), dbdir = raw_db_path, read_only = TRUE)
    close_con <- TRUE
  } else {
    close_con <- FALSE
  }
  if (close_con) on.exit(dbDisconnect(con), add = TRUE)

  if (!dbExistsTable(con, table_name)) {
    stop(sprintf("Table '%s' not found in database", table_name))
  }

  if (is.null(parameter_index) && is.null(global_index)) {
    stop("Provide either parameter_index OR global_index")
  }
  if (!is.null(parameter_index) && !is.null(global_index)) {
    warning("Both parameter_index and global_index supplied; using parameter_index.")
    global_index <- NULL
  }

  # --- validate baseline/future years ---
  if (length(baseline_year) != 1 || is.na(baseline_year) || baseline_year < 0) {
    stop("baseline_year must be a single non-negative integer")
  }
  future_years <- unique(as.integer(future_years))
  if (length(future_years) == 0) stop("future_years must contain at least one year")
  if (any(future_years <= baseline_year)) {
    kept <- future_years[future_years > baseline_year]
    warning(sprintf("Dropping non-future years: %s",
                    paste(setdiff(future_years, kept), collapse = ", ")))
    future_years <- kept
    if (length(future_years) == 0) stop("No valid future years remain after filtering")
  }

  #clean_filename <- function(x) gsub("[^A-Za-z0-9_-]", "_", x)

  key_clause <- if (!is.null(parameter_index)) {
    sprintf("parameter_index = %d", as.integer(parameter_index))
  } else {
    sprintf("global_index = %s", as.character(dbQuoteString(con, global_index)))
  }

  # meta (for column echo & filenames)
  meta_sql <- sprintf("
    SELECT parameter_index, global_index
    FROM %s
    WHERE %s
    LIMIT 1
  ", qi(table_name, con), key_clause)
  meta <- dbGetQuery(con, meta_sql)
  if (!nrow(meta)) stop("No rows found for the requested index/key")

  # Check available year range to warn on out-of-range requests
  yrange_sql <- sprintf("
    SELECT
      MIN(CAST(FLOOR(timesteps / %f) AS INTEGER)) AS min_year,
      MAX(CAST(FLOOR(timesteps / %f) AS INTEGER)) AS max_year
    FROM %s
    WHERE %s
  ", days_per_year, days_per_year, qi(table_name, con), key_clause)
  yrng <- dbGetQuery(con, yrange_sql)
  if (is.finite(yrng$min_year) && is.finite(yrng$max_year)) {
    req_years <- c(baseline_year, future_years)
    if (any(req_years < yrng$min_year | req_years > yrng$max_year)) {
      warning(sprintf(
        "Requested years outside data range [%d, %d] will yield no rows if missing.",
        yrng$min_year, yrng$max_year
      ))
    }
  }

  years_all <- sort(unique(c(baseline_year, future_years)))
  min_start <- as.integer(min(years_all) * days_per_year)
  max_end   <- as.integer((max(years_all) + 1) * days_per_year)
  years_csv <- paste(years_all, collapse = ", ")

  sql <- sprintf("
    WITH base AS (
      SELECT
        simulation_index,
        CAST(FLOOR(timesteps / %f) AS INTEGER) AS year,
        CAST(cases_per_1000 AS DOUBLE) AS cpk
      FROM %s
      WHERE %s
        AND timesteps >= %d AND timesteps < %d
        AND cases_per_1000 IS NOT NULL
    )
    SELECT
      simulation_index,
      year,
      AVG(cpk) AS avg_cases_per_1000,
      COUNT(*) AS n_timesteps
    FROM base
    WHERE year IN (%s)
    GROUP BY simulation_index, year
    ORDER BY simulation_index, year
  ", days_per_year, qi(table_name, con), key_clause, min_start, max_end, years_csv)

  ann <- dbGetQuery(con, sql)
  if (!nrow(ann)) stop("No data found for requested windows/years.")

  # Split baseline vs future
  base_df <- ann[ann$year == baseline_year, c("simulation_index", "avg_cases_per_1000", "n_timesteps")]
  names(base_df) <- c("simulation_index", "baseline_cases_per_1000", "n_timesteps_baseline")

  fut_df  <- ann[ann$year %in% future_years, c("simulation_index", "year", "avg_cases_per_1000", "n_timesteps")]
  names(fut_df) <- c("simulation_index", "future_year", "future_cases_per_1000", "n_timesteps_future")

  # Join baseline to future per simulation
  merged <- merge(fut_df, base_df, by = "simulation_index", all.x = TRUE)

  # Compute deltas per simulation
  merged$delta_cases_per_1000 <- merged$future_cases_per_1000 - merged$baseline_cases_per_1000
  merged$percent_change <- ifelse(
    is.na(merged$baseline_cases_per_1000) | merged$baseline_cases_per_1000 == 0,
    NA_real_,
    100 * merged$delta_cases_per_1000 / merged$baseline_cases_per_1000
  )

  # --- aggregate or return per-sim ---
  if (isTRUE(stochastic_average)) {
    # Use only sims that have BOTH baseline and this future_year
    agg_list <- lapply(split(merged, merged$future_year), function(dfy) {
      dfy <- dfy[complete.cases(dfy[, c("future_cases_per_1000", "baseline_cases_per_1000")]), ]
      n_sims_used <- length(unique(dfy$simulation_index))
      data.frame(
        parameter_index             = meta$parameter_index[1],
        global_index                = meta$global_index[1],
        baseline_year               = baseline_year,
        future_year                 = unique(dfy$future_year)[1],
        baseline_cases_per_1000     = if (n_sims_used) mean(dfy$baseline_cases_per_1000) else NA_real_,
        future_cases_per_1000       = if (n_sims_used) mean(dfy$future_cases_per_1000) else NA_real_,
        delta_cases_per_1000        = if (n_sims_used) mean(dfy$delta_cases_per_1000) else NA_real_,
        percent_change              = if (n_sims_used) mean(dfy$percent_change, na.rm = TRUE) else NA_real_,
        n_sims_used                 = n_sims_used,
        row.names = NULL
      )
    })
    out <- do.call(rbind, agg_list)

  } else {
    # Per-simulation rows
    out <- merged[, c("simulation_index",
                      "future_year",
                      "baseline_cases_per_1000",
                      "future_cases_per_1000",
                      "delta_cases_per_1000",
                      "percent_change",
                      "n_timesteps_baseline",
                      "n_timesteps_future")]
    out$parameter_index <- meta$parameter_index[1]
    out$global_index    <- meta$global_index[1]
    out$baseline_year   <- baseline_year
    # Reorder columns
    out <- out[, c("parameter_index","global_index","simulation_index",
                   "baseline_year","future_year",
                   "baseline_cases_per_1000","future_cases_per_1000",
                   "delta_cases_per_1000","percent_change",
                   "n_timesteps_baseline","n_timesteps_future")]
    rownames(out) <- NULL
  }

  if (!is.null(output_dir) && nzchar(output_dir)) {
    if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    fname <- if (!is.null(parameter_index)) {
      sprintf("cases_summary_param_%d.csv", as.integer(parameter_index))
    } else {
      sprintf("cases_summary_%s.csv", clean_filename(meta$global_index[1]))
    }
    fpath <- file.path(output_dir, fname)
    write.csv(out, fpath, row.names = FALSE)
    message(sprintf("Summary written to: %s", fpath))
  }

  out
}