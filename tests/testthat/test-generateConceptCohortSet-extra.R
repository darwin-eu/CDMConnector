# Additional tests for R/generateConceptCohortSet.R

# --- cohortCollapse with data.frame ---

test_that("cohortCollapse collapses overlapping intervals in data.frame", {
  df <- data.frame(
    cohort_definition_id = c(1L, 1L, 1L),
    subject_id = c(1L, 1L, 1L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-01-05", "2020-03-01")),
    cohort_end_date = as.Date(c("2020-01-10", "2020-01-15", "2020-03-10"))
  )

  result <- CDMConnector:::cohortCollapse(df)
  expect_true(is.data.frame(result))
  # First two overlap: [Jan1-Jan10] and [Jan5-Jan15] -> merged to [Jan1-Jan15]
  # Third is separate: [Mar1-Mar10]
  expect_equal(nrow(result), 2)
})

test_that("cohortCollapse handles non-overlapping intervals", {
  df <- data.frame(
    cohort_definition_id = c(1L, 1L),
    subject_id = c(1L, 1L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-06-01")),
    cohort_end_date = as.Date(c("2020-01-31", "2020-06-30"))
  )

  result <- CDMConnector:::cohortCollapse(df)
  expect_equal(nrow(result), 2)
})

test_that("cohortCollapse works with tbl_sql", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(
    cohort_definition_id = c(1L, 1L),
    subject_id = c(1L, 1L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-01-05")),
    cohort_end_date = as.Date(c("2020-01-10", "2020-01-15"))
  )

  DBI::dbWriteTable(con, "test_cohort", df)
  tbl_ref <- dplyr::tbl(con, "test_cohort")

  result <- CDMConnector:::cohortCollapse(tbl_ref) %>% dplyr::collect()
  expect_equal(nrow(result), 1)  # overlapping -> merged
})

test_that("cohortCollapse errors on non-table input", {
  expect_error(CDMConnector:::cohortCollapse(list(a = 1)))
})

# --- generateConceptCohortSet ---

test_that("generateConceptCohortSet generates cohort from concept list", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(condition_1 = c(192671))

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "concept_cohort1")
  expect_true("concept_cohort1" %in% names(cdm))

  cohort_count <- cdm$concept_cohort1 %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(cohort_count >= 0)
})

test_that("generateConceptCohortSet with limit = first", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(gi_bleed = c(192671))

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "first_cohort", limit = "first")
  expect_true("first_cohort" %in% names(cdm))

  # With limit = "first", each person should appear at most once
  cohort <- cdm$first_cohort %>% dplyr::collect()
  if (nrow(cohort) > 0) {
    n_unique <- length(unique(cohort$subject_id))
    expect_equal(nrow(cohort), n_unique)
  }
})

test_that("generateConceptCohortSet with end = event_end_date", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(gi_bleed = c(192671))

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "end_event_cohort", end = "event_end_date")
  expect_true("end_event_cohort" %in% names(cdm))
})

test_that("generateConceptCohortSet with end = numeric days", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(gi_bleed = c(192671))

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "end_days_cohort", end = 30)
  expect_true("end_days_cohort" %in% names(cdm))
})

test_that("generateConceptCohortSet with requiredObservation", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(gi_bleed = c(192671))

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "obs_cohort", requiredObservation = c(180, 30))
  expect_true("obs_cohort" %in% names(cdm))
})

test_that("generateConceptCohortSet errors when overwrite is FALSE and table exists", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(gi_bleed = c(192671))

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "existing_cohort")
  expect_error(
    generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "existing_cohort", overwrite = FALSE),
    "already exists"
  )
})

test_that("generateConceptCohortSet with multiple concept sets", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  conceptSet <- list(
    gi_bleed = c(192671),
    pharyngitis = c(4112343)
  )

  cdm <- generateConceptCohortSet(cdm, conceptSet = conceptSet, name = "multi_cohort")
  expect_true("multi_cohort" %in% names(cdm))
})
