test_that("computeQuery gives warning", {
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(con, "main", "main")

  expect_warning({
    tbl <- cdm$person %>%
      dplyr::mutate(a = "a") %>%
      computeQuery()
  })

  DBI::dbDisconnect(con, shutdown = TRUE)
})
