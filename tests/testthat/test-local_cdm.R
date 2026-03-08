
test_that("softValidation is passed correctly", {
  skip_if_not_installed("duckdb")
  con <- local_eunomia_con()

  # create overlapping observation periods
  op <- dplyr::tbl(con, "observation_period") |> dplyr::collect()
  DBI::dbAppendTable(con, "observation_period", op)

  expect_error(cdmFromCon(con, "main", "main", .softValidation = F),
               "overlap")

  # soft validation ignores this
  expect_no_error(cdmFromCon(con, "main", "main", .softValidation = T))
})
