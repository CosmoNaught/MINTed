# Use testthat 3e
testthat::local_edition(3)

# Skip DB-dependent tests on CRAN or if duckdb isn't installed
skip_if_no_duckdb <- function() {
  testthat::skip_on_cran()
  testthat::skip_if_not_installed("duckdb")
}
