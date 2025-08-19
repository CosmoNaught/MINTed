test_that("plot_prevalence: writes a PDF when output_dir is provided", {
  con <- make_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  tmpdir <- tempfile("plots-")
  dir.create(tmpdir)

  path <- plot_prevalence(
    con = con,
    parameter_index = 1,
    output_dir = tmpdir,
    days_per_year = 365
  )
  # function returns invisible path when output_dir is set
  expect_true(is.character(path) && length(path) == 1)
  expect_true(file.exists(path))
  # keep it tiny
  expect_gt(file.info(path)$size, 0)
})
