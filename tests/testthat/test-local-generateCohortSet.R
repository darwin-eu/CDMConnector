# Copyright 2025 DARWIN EU®
#
# Test generateCohortSet on a local (collected) CDM: create gibleed CDM from
# Eunomia, collect it, then run cohort generation with example cohorts.
# Local CDMs use the internal generateCohortSetLocal (in-memory DuckDB).

test_that("generateCohortSet works on local CDM (gibleed from Eunomia, collected)", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())

  # Create CDM from Eunomia (contains gibleed-relevant data, concept 192671)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
  cdm <- cdmFromCon(
    con = con,
    cdmName = "gibleed",
    cdmSchema = "main",
    writeSchema = "main"
  )

  # Collect CDM into R (local in-memory CDM)
  cdm_local <- dplyr::collect(cdm)
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Use example cohorts from the package (gibleed cohorts)
  cohort_set <- readCohortSet(
    system.file("example_cohorts", package = "CDMConnector", mustWork = TRUE)
  )
  expect_gt(nrow(cohort_set), 0)
  expect_s3_class(cohort_set, "CohortSet")

  # Generate cohort set on the local CDM (uses in-memory DuckDB internally)
  cdm_local <- CDMConnector:::generateCohortSetLocal(
    cdm = cdm_local,
    cohortSet = cohort_set,
    name = "gibleed",
    computeAttrition = TRUE,
    overwrite = TRUE
  )

  expect_true("gibleed" %in% names(cdm_local))
  expect_true(methods::is(cdm_local$gibleed, "cohort_table"))

  cohort_df <- dplyr::collect(cdm_local$gibleed)
  expect_s3_class(cohort_df, "data.frame")
  expect_true(all(c(
    "cohort_definition_id", "subject_id",
    "cohort_start_date", "cohort_end_date"
  ) %in% colnames(cohort_df)))

  expect_gt(nrow(cohort_df), 0)

  counts <- cohortCount(cdm_local$gibleed)
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), nrow(cohort_set))

  settings_df <- settings(cdm_local$gibleed)
  expect_s3_class(settings_df, "data.frame")
  expect_true("cohort_definition_id" %in% colnames(settings_df))
})
