test_that("tabulate_cases: stochastic average and per-sim outputs are sensible", {
  con <- make_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Baseline year 0, future years 1 and 2
  out_avg <- tabulate_cases(
    con = con,
    parameter_index = 1,
    baseline_year = 0,
    future_years = c(1, 2),
    stochastic_average = TRUE
  )

  expect_s3_class(out_avg, "data.frame")
  expect_setequal(out_avg$future_year, c(1L, 2L))
  expect_true(all(out_avg$parameter_index == 1))
  expect_true(all(out_avg$baseline_year == 0))
  # With our fixture: baseline=50, future=60/70, delta=10/20, percent=20/40
  one <- out_avg[out_avg$future_year == 1L, , drop = FALSE]
  two <- out_avg[out_avg$future_year == 2L, , drop = FALSE]
  expect_equal(one$baseline_cases_per_1000, 50)
  expect_equal(one$future_cases_per_1000 , 60)
  expect_equal(one$delta_cases_per_1000  , 10)
  expect_equal(round(one$percent_change, 6), 20)

  expect_equal(two$baseline_cases_per_1000, 50)
  expect_equal(two$future_cases_per_1000 , 70)
  expect_equal(two$delta_cases_per_1000  , 20)
  expect_equal(round(two$percent_change, 6), 40)

  # Per-simulation mode returns one row per sim x future year
  out_ps <- tabulate_cases(
    con = con,
    parameter_index = 1,
    baseline_year = 0,
    future_years = c(1, 2),
    stochastic_average = FALSE
  )
  expect_true(all(c("simulation_index","future_year") %in% names(out_ps)))
  expect_equal(nrow(out_ps), 2 * length(c(1,2))) # 2 sims x 2 years
  expect_true(all(out_ps$baseline_year == 0))
})
