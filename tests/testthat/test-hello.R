
test_that("hello works", {
  expect_type(hello(), "character")
  expect_match(hello(), "Hello")
})

