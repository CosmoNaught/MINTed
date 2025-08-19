test_that("query_database: basic equality / IN / range filters work", {
  con <- make_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # equality filter
  res1 <- query_database(con = con, filters = list(parameter_index = 1))
  expect_s3_class(res1, "data.frame")
  expect_true(all(res1$parameter_index == 1))

  # IN filter (character vector)
  res2 <- query_database(con = con, filters = list(global_index = c("A.rds","B.rds")))
  expect_true(all(res2$global_index %in% c("A.rds","B.rds")))
  expect_true(nrow(res2) >= nrow(res1))

  # numeric range filter (eir between 25 and 35 keeps only param 1)
  res3 <- query_database(con = con, filters = list(eir = c(25, 35)))
  expect_true(all(res3$eir >= 25 & res3$eir <= 35))
  expect_true(all(res3$parameter_index == 1))

  # limit
  res4 <- query_database(con = con, filters = list(parameter_index = 2), limit = 1)
  expect_equal(nrow(res4), 1)

  # columns returned
  expect_setequal(
    names(res1),
    c("parameter_index","global_index",
      "eir","Q0","phi_bednets","seasonal","routine",
      "dn0_use","dn0_future","itn_use","irs_use",
      "itn_future","irs_future","lsm")
  )
})
