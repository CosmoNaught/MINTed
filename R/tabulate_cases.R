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
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = raw_db_path, read_only = TRUE)
    close_con <- TRUE
  } else close_con <- FALSE
  if (close_con) on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  qi <- function(x, con) DBI::dbQuoteIdentifier(con, x)

  if (!DBI::dbExistsTable(con, table_name)) {
    stop(sprintf("Table '%s' not found in database", table_name))
  }

  if (is.null(parameter_index) && is.null(global_index)) {
    stop("Provide either parameter_index OR global_index")
  }
  if (!is.null(parameter_index) && !is.null(global_index)) {
    warning("Both parameter_index and global_index supplied; using parameter_index.")
    global_index <- NULL
  }

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

  key_clause <- if (!is.null(parameter_index)) {
    sprintf("parameter_index = %d", as.integer(parameter_index))
  } else {
    sprintf("global_index = %s", as.character(DBI::dbQuoteString(con, global_index)))
  }

  # --- detect schema: raw vs precomputed ---
  cols <- DBI::dbGetQuery(
    con,
    sprintf("
      SELECT LOWER(column_name) AS name
      FROM information_schema.columns
      WHERE table_name = %s
    ", DBI::dbQuoteString(con, table_name))
  )$name

  has_raw   <- all(c("n_inc_clinical_0_36500","n_age_0_36500") %in% cols)
  has_rate  <- "cases_per_1000" %in% cols

  if (!has_raw && !has_rate) {
    stop(sprintf(
      "Table '%s' must have either (n_inc_clinical_0_36500, n_age_0_36500) or cases_per_1000.",
      table_name
    ))
  }

  # meta (for echo)
  meta_sql <- sprintf("
    SELECT parameter_index, global_index
    FROM %s
    WHERE %s
    LIMIT 1
  ", qi(table_name, con), key_clause)
  meta <- DBI::dbGetQuery(con, meta_sql)
  if (!nrow(meta)) stop("No rows found for the requested index/key")

  # year range warning
  yrange_sql <- sprintf("
    SELECT
      MIN(CAST(FLOOR(timesteps / %f) AS INTEGER)) AS min_year,
      MAX(CAST(FLOOR(timesteps / %f) AS INTEGER)) AS max_year
    FROM %s
    WHERE %s
  ", days_per_year, days_per_year, qi(table_name, con), key_clause)
  yrng <- DBI::dbGetQuery(con, yrange_sql)
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

  # --- SQL that adapts to schema ---
  if (has_raw) {
    base_measures <- "
        CAST(n_inc_clinical_0_36500 AS DOUBLE) AS inc,
        CAST(n_age_0_36500          AS DOUBLE) AS age"
    not_null_pred <- "
        AND n_inc_clinical_0_36500 IS NOT NULL
        AND n_age_0_36500          IS NOT NULL"
    annual_expr <- "
        1000.0 * SUM(inc) / NULLIF(AVG(age), 0) AS avg_cases_per_1000"
  } else {
    base_measures <- "
        CAST(cases_per_1000 AS DOUBLE) AS rate"
    not_null_pred <- "
        AND cases_per_1000 IS NOT NULL"
    annual_expr <- "
        AVG(rate) AS avg_cases_per_1000"
  }

  sql <- sprintf("
    WITH base AS (
      SELECT
        simulation_index,
        CAST(FLOOR(timesteps / %f) AS INTEGER) AS year,
        timesteps,%s
      FROM %s
      WHERE %s
        AND timesteps >= %d AND timesteps < %d%s
    ),
    annual AS (
      SELECT
        simulation_index,
        year,%s,
        COUNT(DISTINCT timesteps) AS n_timesteps
      FROM base
      WHERE year IN (%s)
      GROUP BY simulation_index, year
    )
    SELECT
      simulation_index,
      year,
      avg_cases_per_1000,
      n_timesteps
    FROM annual
    ORDER BY simulation_index, year
  ",
    days_per_year,
    base_measures,
    qi(table_name, con), key_clause,
    min_start, max_end, not_null_pred,
    annual_expr,
    years_csv
  )

  ann <- DBI::dbGetQuery(con, sql)
  if (!nrow(ann)) stop("No data found for requested windows/years.")

  base_df <- ann[ann$year == baseline_year, c("simulation_index", "avg_cases_per_1000", "n_timesteps")]
  names(base_df) <- c("simulation_index", "baseline_cases_per_1000", "n_timesteps_baseline")

  fut_df  <- ann[ann$year %in% future_years, c("simulation_index", "year", "avg_cases_per_1000", "n_timesteps")]
  names(fut_df) <- c("simulation_index", "future_year", "future_cases_per_1000", "n_timesteps_future")

  merged <- merge(fut_df, base_df, by = "simulation_index", all.x = TRUE)
  merged$delta_cases_per_1000 <- merged$future_cases_per_1000 - merged$baseline_cases_per_1000
  merged$percent_change <- ifelse(
    is.na(merged$baseline_cases_per_1000) | merged$baseline_cases_per_1000 == 0,
    NA_real_,
    100 * merged$delta_cases_per_1000 / merged$baseline_cases_per_1000
  )

  if (isTRUE(stochastic_average)) {
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
      clean_filename <- function(x) gsub("[^A-Za-z0-9_-]", "_", x)
      sprintf("cases_summary_%s.csv", clean_filename(meta$global_index[1]))
    }
    fpath <- file.path(output_dir, fname)
    utils::write.csv(out, fpath, row.names = FALSE)
    message(sprintf("Summary written to: %s", fpath))
  }

  out
}
