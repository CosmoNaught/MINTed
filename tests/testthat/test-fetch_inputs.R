test_that("fetch_inputs: returns a 1-row data.frame for a valid key (if helper funcs exist)", {
  con <- make_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Only run if your package defines get_duck_connection() and build_key_clause()
  has_gdc <- tryCatch(length(getAnywhere("get_duck_connection")$objs) > 0, error = function(e) FALSE)
  has_bkc <- tryCatch(length(getAnywhere("build_key_clause")$objs) > 0, error = function(e) FALSE)
  if (!has_gdc || !has_bkc) {
    testthat::skip("get_duck_connection()/build_key_clause() not found; skipping fetch_inputs test.")
  }

  res <- fetch_inputs(con = con, parameter_index = 1, table_name = "simulation_results")
  expect_s3_class(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_setequal(
    names(res),
    c("eir","Q0","phi_bednets","seasonal","routine",
      "dn0_use","dn0_future","itn_use","irs_use",
      "itn_future","irs_future","lsm")
  )
})
