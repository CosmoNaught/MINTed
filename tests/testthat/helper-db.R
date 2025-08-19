# Minimal DuckDB fixture with a tiny 'simulation_results' table
make_test_db <- function() {
  skip_if_no_duckdb()
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = tempfile(fileext = ".duckdb"))

  set.seed(1)

  # Build small synthetic table (2 params x 2 sims x 3 years x 3 points/year)
  years <- 0:2
  per_year_steps <- c(0L, 120L, 240L)
  rows <- lapply(c(1L, 2L), function(param_idx) {
    gidx <- if (param_idx == 1L) "A.rds" else "B.rds"
    inputs <- list(
      eir         = if (param_idx == 1L) 30 else 40,
      Q0          = if (param_idx == 1L) 0.30 else 0.40,
      phi_bednets = if (param_idx == 1L) 0.50 else 0.60,
      seasonal    = TRUE, routine = FALSE,
      dn0_use     = 0.1, dn0_future = 0.2,
      itn_use     = 0.3, irs_use = 0.4,
      itn_future  = 0.5, irs_future = 0.6,
      lsm         = 1
    )

    do.call(rbind, lapply(1:2, function(sim) {
      do.call(rbind, lapply(years, function(y) {
        ts <- as.integer(y * 365L + per_year_steps)
        data.frame(
          parameter_index = param_idx,
          global_index    = gidx,
          simulation_index = sim,
          timesteps       = ts,
          # keep cpk constant within a year so annual averages are simple:
          cases_per_1000  = 50 + 10 * y,
          prevalence      = 0.05 + 0.05 * y,
          treatment_dt_bednet = "c(365, 730)",
          treatment_dt_irs    = "365 730",
          treatment_dt_lsm    = "[365,730]",
          eir         = inputs$eir,
          Q0          = inputs$Q0,
          phi_bednets = inputs$phi_bednets,
          seasonal    = inputs$seasonal,
          routine     = inputs$routine,
          dn0_use     = inputs$dn0_use,
          dn0_future  = inputs$dn0_future,
          itn_use     = inputs$itn_use,
          irs_use     = inputs$irs_use,
          itn_future  = inputs$itn_future,
          irs_future  = inputs$irs_future,
          lsm         = inputs$lsm,
          stringsAsFactors = FALSE
        )
      }))
    }))
  })
  df <- do.call(rbind, rows)

  DBI::dbWriteTable(con, "simulation_results", df, overwrite = TRUE)
  con
}
