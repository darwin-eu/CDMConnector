test_that("computeQuery gives warning", {
  testthat::skip_if_not_installed("duckdb")
  testthat::skip_if_not("duckdb" %in% dbToTest)
  con <- local_eunomia_con()
  cdm <- cdmFromCon(con, "main", "main")

  expect_warning({
    tbl <- cdm$person %>%
      dplyr::mutate(a = "a") %>%
      computeQuery()
  })
})
