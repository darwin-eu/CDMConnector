test_that("validate cdm works", {
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, cdm_tables = c("person", "observation_period"))
  expect_output(validate_cdm(cdm))

  cdm <- cdm_from_con(con)
  expect_output(validate_cdm(cdm))

  DBI::dbDisconnect(con, shutdown = TRUE)
})
