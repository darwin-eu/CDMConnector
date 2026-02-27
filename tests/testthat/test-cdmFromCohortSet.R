# Minimal ATLAS-style cohort JSON (single drug exposure primary criterion).
# Omit EndStrategy so CirceR accepts it (empty {} requires a type id).
minimal_cohort_json <- function() {
  '{
    "ConceptSets": [{
      "id": 0,
      "name": "Aspirin",
      "expression": {
        "items": [{"concept": {"CONCEPT_ID": 1112807}}]
      }
    }],
    "PrimaryCriteria": {
      "CriteriaList": [{"DrugExposure": {"CodesetId": 0}}],
      "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
      "PrimaryCriteriaLimit": {"Type": "First"}
    },
    "QualifiedLimit": {"Type": "First"},
    "ExpressionLimit": {"Type": "First"},
    "InclusionRules": [],
    "CollapseSettings": {"CollapseType": "ERA", "EraPad": 30}
  }'
}

minimal_cohort_expression <- function() {
  jsonlite::fromJSON(minimal_cohort_json(), simplifyVector = FALSE)
}

skip_if_no_cdm_deps <- function() {
  need <- c("DBI", "duckdb", "jsonlite", "CirceR", "SqlRender")
  for (p in need) {
    if (!requireNamespace(p, quietly = TRUE)) {
      testthat::skip(paste("Required package", p, "not installed"))
    }
  }
}

skip_if_no_cdmconnector <- function() {
  skip_if_no_cdm_deps()
  if (!requireNamespace("CDMConnector", quietly = TRUE)) {
    testthat::skip("CDMConnector not installed")
  }
}

test_that("cdmFromJson works with jsonPath", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  tmp <- tempfile(fileext = ".json")
  writeLines(minimal_cohort_json(), tmp)
  on.exit(unlink(tmp))
  res <- CDMConnector:::cdmFromJson(jsonPath = tmp, n = 50, seed = 42, targetMatch = 0.9, successRate = 0.5)
  expect_type(res, "list")
  expect_true("duckdb_path" %in% names(res))
  expect_true("summary" %in% names(res))
  expect_true(file.exists(res$duckdb_path))
  expect_gte(res$summary$n_generated, 1)
  # Can query (connection is already closed by cdmFromJson)
  con <- DBI::dbConnect(duckdb::duckdb(), res$duckdb_path, read_only = TRUE)
  n_person <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM person")$n
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_equal(n_person, 50)
})

test_that("cdmFromJson works with cohortExpression", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  expr <- minimal_cohort_expression()
  res <- CDMConnector:::cdmFromJson(cohortExpression = expr, n = 30, seed = 43, targetMatch = 0.9, successRate = 0.5)
  expect_type(res, "list")
  expect_true("duckdb_path" %in% names(res))
  expect_equal(res$summary$n_generated, 30)
  con <- DBI::dbConnect(duckdb::duckdb(), res$duckdb_path, read_only = TRUE)
  n_person <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM person")$n
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_equal(n_person, 30)
})

test_that("cdmFromJson errors without jsonPath or cohortExpression", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  expect_error(
    CDMConnector:::cdmFromJson(n = 10),
    "Provide either jsonPath"
  )
})

test_that("cdmFromCohortSet returns cdm with correct person count", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  expr <- minimal_cohort_expression()
  cohortSet <- data.frame(
    cohort_definition_id = 1L,
    cohort_name = "test_cohort",
    cohort = I(list(expr)),
    stringsAsFactors = FALSE
  )
  cdm <- CDMConnector::cdmFromCohortSet(cohortSet, n = 25, seed = 44, targetMatch = 0.9, successRate = 0.5)
  expect_type(cdm, "list")
  expect_true("person" %in% names(cdm))
  n_persons <- nrow(dplyr::collect(cdm$person))
  expect_equal(n_persons, 25, info = "cdm should have exactly 25 persons")
})

test_that("cdmFromCohortSet builds condition_era, drug_era, dose_era and cdm_source", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  expr <- minimal_cohort_expression()
  cohortSet <- data.frame(
    cohort_definition_id = 1L,
    cohort_name = "test_cohort",
    cohort = I(list(expr)),
    stringsAsFactors = FALSE
  )
  cdm <- CDMConnector::cdmFromCohortSet(cohortSet, n = 25, seed = 44, targetMatch = 0.9, successRate = 0.5)
  expect_true("condition_era" %in% names(cdm))
  expect_true("drug_era" %in% names(cdm))
  expect_true("dose_era" %in% names(cdm))
  expect_true("cdm_source" %in% names(cdm))
  # Drug era should have rows when we have drug_exposure (primary criterion is DrugExposure)
  drug_era_n <- nrow(dplyr::collect(cdm$drug_era))
  expect_gte(drug_era_n, 1)
  # cdm_source should have one row
  cdm_src <- dplyr::collect(cdm$cdm_source)
  expect_equal(nrow(cdm_src), 1)
  expect_true("cdm_source_name" %in% names(cdm_src))
  expect_true("cdm_version" %in% names(cdm_src))
})

test_that("generateCohortSet on cdm yields non-zero cohort counts", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  expr <- minimal_cohort_expression()
  cohort_json <- minimal_cohort_json()
  # generateCohortSet expects cohortSet with columns: cohort_definition_id, cohort_name, cohort, json
  cohortSet <- data.frame(
    cohort_definition_id = 1L,
    cohort_name = "test_cohort",
    cohort = I(list(expr)),
    json = cohort_json,
    stringsAsFactors = FALSE
  )
  cdm <- CDMConnector::cdmFromCohortSet(cohortSet, n = 25, seed = 44, targetMatch = 0.9, successRate = 0.5)
  expect_equal(nrow(dplyr::collect(cdm$person)), 25)

  # Generate cohort using CDMConnector (runs cohort SQL on the cdm)
  cdm <- CDMConnector::generateCohortSet(cdm, cohortSet, name = "study_cohort")
  expect_true("study_cohort" %in% names(cdm))

  # At least some persons should be in the cohort; cohort counts not zero
  counts <- omopgenerics::cohortCount(cdm$study_cohort)
  counts_df <- dplyr::collect(counts)
  expect_true(nrow(counts_df) >= 1, info = "cohortCount should return at least one row")
  expect_true(
    sum(counts_df$number_records, na.rm = TRUE) > 0,
    info = "cohort should have at least one record (number_records > 0)"
  )
  expect_true(
    sum(counts_df$number_subjects, na.rm = TRUE) > 0,
    info = "cohort should have at least one subject (number_subjects > 0)"
  )
})

test_that("cdmFromCohortSet errors on invalid cohortSet", {
  skip_on_ci()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_no_cdm_deps()
  expect_error(
    CDMConnector::cdmFromCohortSet("not a data frame", n = 10),
    "non-empty data frame"
  )
  expect_error(
    CDMConnector::cdmFromCohortSet(
      data.frame(x = 1, y = 2),
      n = 10
    ),
    "cohort_definition_id|cohort_name|cohort"
  )
})

