#' Plot cases per 1000 over time (years) from the raw DuckDB
#'
#' @param raw_db_path Path to the .duckdb file created by write_database(). 
#'   Required if con is not provided.
#' @param con Optional DuckDB connection object. If provided, raw_db_path is ignored.
#' @param parameter_index Integer. One of parameter_index OR global_index must be provided.
#' @param global_index Character. One of parameter_index OR global_index must be provided.
#' @param start_timestep Integer, optional. Include data with timesteps >= this value (days).
#' @param end_timestep Integer, optional. Include data with timesteps <= this value (days).
#' @param output_dir Optional dir to save a PDF. If NULL/"" -> plot to current device.
#' @param full_name Optional flag to print the initial parameters used within the simulation launch.
#' @param table_name Name of the base table. Default "simulation_results".
#' @param days_per_year Numeric, default 365.
#'
#' @export
plot_cases <- function(con = NULL,
                       raw_db_path = NULL,
                       parameter_index = NULL,
                       global_index = NULL,
                       start_timestep = NULL,
                       end_timestep = NULL,
                       output_dir = NULL,
                       full_name = FALSE,
                       table_name = "simulation_results",
                       days_per_year = 365) {

  # --- connection handling ---
  if (is.null(con)) {
    if (is.null(raw_db_path)) stop("Either con or raw_db_path must be provided")
    stopifnot(file.exists(raw_db_path))
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = raw_db_path, read_only = TRUE)
    close_con <- TRUE
  } else {
    close_con <- FALSE
  }
  if (close_con) on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (is.null(parameter_index) && is.null(global_index)) {
    stop("Provide either parameter_index OR global_index")
  }
  if (!is.null(parameter_index) && !is.null(global_index)) {
    warning("Both parameter_index and global_index supplied; using parameter_index.")
    global_index <- NULL
  }

  if (!DBI::dbExistsTable(con, table_name)) {
    stop(sprintf("Table '%s' not found in database", table_name))
  }

  # --- WHERE clause(s) ---
  key_filter <- if (!is.null(parameter_index)) {
    sprintf("parameter_index = %d", as.integer(parameter_index))
  } else {
    sprintf("global_index = %s", DBI::dbQuoteString(con, global_index))
  }

  range_filter <- character(0)
  if (!is.null(start_timestep))
    range_filter <- c(range_filter, sprintf("timesteps >= %d", as.integer(start_timestep)))
  if (!is.null(end_timestep))
    range_filter <- c(range_filter, sprintf("timesteps <= %d", as.integer(end_timestep)))
  where_clause <- paste(c(key_filter, range_filter), collapse = " AND ")
  if (!nzchar(where_clause)) where_clause <- "1=1"

  # --- detect schema ---
  cols <- DBI::dbGetQuery(
    con,
    sprintf("
      SELECT LOWER(column_name) AS name
      FROM information_schema.columns
      WHERE table_name = %s
    ", DBI::dbQuoteString(con, table_name))
  )$name

  has_raw  <- all(c("n_inc_clinical_0_36500","n_age_0_36500") %in% cols)
  has_rate <- "cases_per_1000" %in% cols
  if (!has_raw && !has_rate) {
    stop(sprintf(
      "Table '%s' must have either (n_inc_clinical_0_36500, n_age_0_36500) or cases_per_1000.",
      table_name
    ))
  }

  # --- base CTE providing cases_per_1000 per timestep ---
  rate_expr <- if (has_raw) {
    "
      CASE
        WHEN n_age_0_36500 IS NULL OR n_age_0_36500 = 0 THEN NULL
        ELSE 1000.0 * CAST(n_inc_clinical_0_36500 AS DOUBLE) / CAST(n_age_0_36500 AS DOUBLE)
      END AS cases_per_1000
    "
  } else {
    "
      CAST(cases_per_1000 AS DOUBLE) AS cases_per_1000
    "
  }

  not_null_pred <- if (has_raw) {
    "AND n_inc_clinical_0_36500 IS NOT NULL AND n_age_0_36500 IS NOT NULL"
  } else {
    "AND cases_per_1000 IS NOT NULL"
  }

  base_cte <- sprintf("
    WITH base AS (
      SELECT
        simulation_index,
        timesteps,
        %s
      FROM %s
      WHERE %s
        %s
    )
  ",
    rate_expr,
    DBI::dbQuoteIdentifier(con, table_name),
    where_clause,
    if (nzchar(not_null_pred)) paste0(" ", not_null_pred) else ""
  )

  # --- fetch per-simulation series ---
  sim_sql <- paste0(
    base_cte,
    "SELECT simulation_index, timesteps, cases_per_1000
     FROM base
     ORDER BY simulation_index, timesteps"
  )
  sim_df <- DBI::dbGetQuery(con, sim_sql)
  if (!nrow(sim_df)) stop("No rows matched your filter; check indices and start/end timesteps.")

  # --- mean across sims (per timestep) ---
  mean_sql <- paste0(
    base_cte,
    "SELECT timesteps, AVG(cases_per_1000) AS mean_cases_per_1000
     FROM base
     GROUP BY timesteps
     ORDER BY timesteps"
  )
  mean_df <- DBI::dbGetQuery(con, mean_sql)

  # --- metadata (for title & campaign markers) ---
  meta_key_clause <- if (!is.null(parameter_index)) {
    sprintf("parameter_index = %d", as.integer(parameter_index))
  } else {
    sprintf("global_index = %s", DBI::dbQuoteString(con, global_index))
  }
  meta_sql <- sprintf("
    SELECT *
    FROM %s
    WHERE %s
    LIMIT 1
  ", DBI::dbQuoteIdentifier(con, table_name), meta_key_clause)
  meta <- DBI::dbGetQuery(con, meta_sql)

  # --- convert x to years ---
  sim_df$year  <- sim_df$timesteps  / days_per_year
  mean_df$year <- mean_df$timesteps / days_per_year

  bed_years <- to_years_in_window(parse_ts_vec(meta$treatment_dt_bednet))
  irs_years <- to_years_in_window(parse_ts_vec(meta$treatment_dt_irs))
  lsm_years <- to_years_in_window(parse_ts_vec(meta$treatment_dt_lsm))

  # --- plotting device ---
  if (!is.null(output_dir) && nzchar(output_dir)) {
    if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    pdf_name <- if (!is.null(parameter_index)) {
      sprintf("cases_param_%d.pdf", as.integer(parameter_index))
    } else {
      sprintf("cases_%s.pdf", clean_filename(meta$global_index[1]))
    }
    pdf_path <- file.path(output_dir, pdf_name)
    pdf(pdf_path, width = 10, height = 7)
    on.exit(dev.off(), add = TRUE)
  }

  # --- plot ---
  x_min <- min(sim_df$year, na.rm = TRUE)
  x_max <- max(sim_df$year, na.rm = TRUE)
  y_min <- min(sim_df$cases_per_1000, mean_df$mean_cases_per_1000, na.rm = TRUE)
  y_max <- max(sim_df$cases_per_1000, mean_df$mean_cases_per_1000, na.rm = TRUE)
  if (!is.finite(y_min) || !is.finite(y_max)) { y_min <- 0; y_max <- 1 }

  plot(
    NA,
    xlim = c(x_min, x_max),
    ylim = c(y_min, y_max),
    xlab = "Time (years)",
    ylab = "Cases per 1000",
    main = make_main(meta, full_name),
    type = "n",
    las = 1,
    bty = "l"
  )

  # all simulations (grey)
  sims <- sort(unique(sim_df$simulation_index))
  for (s in sims) {
    sd <- sim_df[sim_df$simulation_index == s, c("year","cases_per_1000")]
    sd <- sd[complete.cases(sd), ]
    if (nrow(sd) > 1) {
      lines(sd$year, sd$cases_per_1000, col = grDevices::rgb(0.7, 0.7, 0.7, 0.5), lwd = 0.5)
    }
  }

  # mean (red)
  mdf <- mean_df[complete.cases(mean_df), ]
  if (nrow(mdf) > 1) {
    lines(mdf$year, mdf$mean_cases_per_1000, col = "red", lwd = 2)
  }

  # dashed grey line at year 9 (no legend)
  abline(v = 9, lty = 2, col = "darkgray", lwd = 1.5)

  # campaign markers
  col_bed <- "dodgerblue3"
  col_irs <- "seagreen3"
  col_lsm <- "orchid3"
  draw_vmarkers(bed_years, col_bed, "bednet campaign", 1)
  draw_vmarkers(irs_years, col_irs, "irs campaign", 2)
  draw_vmarkers(lsm_years, col_lsm, "lsm campaign", 3)

  # legend
  legend(
    "topright",
    legend = c(sprintf("Individual simulations (n=%d)", length(sims)),
               "Mean across simulations",
               "Bednet campaign", "IRS campaign", "LSM campaign"),
    col = c(grDevices::rgb(0.7,0.7,0.7), "red", col_bed, col_irs, col_lsm),
    lty = c(1, 1, 2, 2, 2),
    lwd = c(0.5, 2, 3, 3, 3),
    bty = "o"
  )

  if (!is.null(output_dir) && nzchar(output_dir)) {
    message(sprintf("Plot saved to: %s", pdf_path))
    invisible(pdf_path)
  } else {
    invisible(NULL)
  }
}
