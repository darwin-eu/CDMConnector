# Tests for R/copyCdmTo.R

test_that("copyCdmTo copies cdm to another duckdb connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  cdm <- cdmFromCon(con1, cdmSchema = "main", writeSchema = "main")

  newCdm <- copyCdmTo(con2, cdm, schema = c(schema = "main"), overwrite = TRUE)

  expect_s3_class(newCdm, "cdm_reference")
  expect_true("person" %in% names(newCdm))
  expect_true("observation_period" %in% names(newCdm))

  # Check data was actually copied
  person_count <- newCdm$person %>% dplyr::count() %>% dplyr::pull(n)
  expect_true(person_count > 0)
})

test_that("copyCdmTo preserves cdm name", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  cdm <- cdmFromCon(con1, cdmSchema = "main", writeSchema = "main", cdmName = "TestCDM")

  newCdm <- copyCdmTo(con2, cdm, schema = c(schema = "main"), overwrite = TRUE)
  expect_equal(omopgenerics::cdmName(newCdm), "TestCDM")
})

test_that("copyCdmTo errors on invalid connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbDisconnect(con2, shutdown = TRUE)
  on.exit(DBI::dbDisconnect(con1, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con1, cdmSchema = "main", writeSchema = "main")
  expect_error(copyCdmTo(con2, cdm, schema = c(schema = "main")))
})

test_that("copyCdmTo copies cohort tables", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  cdm <- cdmFromCon(con1, cdmSchema = "main", writeSchema = "main")
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  cdm <- generateCohortSet(cdm, cohort_set, name = "my_cohort", overwrite = TRUE)

  newCdm <- copyCdmTo(con2, cdm, schema = c(schema = "main"), overwrite = TRUE)

  expect_true("my_cohort" %in% names(newCdm))
  cohort_data <- newCdm$my_cohort %>% dplyr::collect()
  expect_true(nrow(cohort_data) >= 0)
})
