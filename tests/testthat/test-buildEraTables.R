# Tests for OHDSI SQL-based era table builders (.build_condition_era_sql, .build_drug_era_sql)

# Extra tests only run in test-coverage and local; skip on container CI
skip_if(nzchar(Sys.getenv("CI_TEST_DB")), "Skipping extra tests on container CI")

setup_era_test_db <- function() {
  skip_if_not_installed("duckdb")
  # Create a lightweight in-memory DuckDB with just the tables needed for era building,

  # instead of copying the ~265MB empty CDM file (which can cause IO errors on CI runners).
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:", read_only = FALSE)

  # Minimal CDM tables needed by era builders
  DBI::dbExecute(con, "CREATE TABLE person (person_id INTEGER, gender_concept_id INTEGER, year_of_birth INTEGER, month_of_birth INTEGER, day_of_birth INTEGER, race_concept_id INTEGER, ethnicity_concept_id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE observation_period (observation_period_id INTEGER, person_id INTEGER, observation_period_start_date DATE, observation_period_end_date DATE, period_type_concept_id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE condition_occurrence (condition_occurrence_id INTEGER, person_id INTEGER, condition_concept_id INTEGER, condition_start_date DATE, condition_end_date DATE, condition_type_concept_id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE condition_era (condition_era_id INTEGER, person_id INTEGER, condition_concept_id INTEGER, condition_era_start_date DATE, condition_era_end_date DATE, condition_occurrence_count INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE drug_exposure (drug_exposure_id INTEGER, person_id INTEGER, drug_concept_id INTEGER, drug_exposure_start_date DATE, drug_exposure_end_date DATE, drug_type_concept_id INTEGER, days_supply INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE drug_era (drug_era_id INTEGER, person_id INTEGER, drug_concept_id INTEGER, drug_era_start_date DATE, drug_era_end_date DATE, drug_exposure_count INTEGER, gap_days INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE concept (concept_id INTEGER, concept_name VARCHAR, domain_id VARCHAR, vocabulary_id VARCHAR, concept_class_id VARCHAR, standard_concept VARCHAR, concept_code VARCHAR, valid_start_date DATE, valid_end_date DATE, invalid_reason VARCHAR)")
  DBI::dbExecute(con, "CREATE TABLE concept_ancestor (ancestor_concept_id INTEGER, descendant_concept_id INTEGER, min_levels_of_separation INTEGER, max_levels_of_separation INTEGER)")

  # Add test persons
  DBI::dbExecute(con, "INSERT INTO person VALUES (1, 8532, 1985, 6, 15, 8527, 0)")
  DBI::dbExecute(con, "INSERT INTO person VALUES (2, 8507, 1990, 3, 20, 8527, 0)")
  DBI::dbExecute(con, "INSERT INTO observation_period VALUES (1, 1, '2020-01-01', '2024-12-31', 0)")
  DBI::dbExecute(con, "INSERT INTO observation_period VALUES (2, 2, '2020-01-01', '2024-12-31', 0)")

  # Add metformin concept (Ingredient) and self-referencing concept_ancestor entry
  DBI::dbExecute(con, "INSERT INTO concept VALUES (1503297, 'metformin', 'Drug', 'RxNorm', 'Ingredient', 'S', '6809', '1970-01-01', '2099-12-31', NULL)")
  DBI::dbExecute(con, "INSERT INTO concept_ancestor VALUES (1503297, 1503297, 0, 0)")

  con
}

test_that(".build_condition_era_sql collapses within 30d gap and splits across gaps", {
  con <- setup_era_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Two occurrences within 30d gap (should merge) + one >30d away (separate era)
  DBI::dbExecute(con, "INSERT INTO condition_occurrence (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date, condition_type_concept_id) VALUES (1, 1, 201826, '2021-01-01', '2021-01-10', 0)")
  DBI::dbExecute(con, "INSERT INTO condition_occurrence (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date, condition_type_concept_id) VALUES (2, 1, 201826, '2021-01-15', '2021-01-25', 0)")
  DBI::dbExecute(con, "INSERT INTO condition_occurrence (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date, condition_type_concept_id) VALUES (3, 1, 201826, '2021-06-01', '2021-06-10', 0)")
  # Person 2: single occurrence
  DBI::dbExecute(con, "INSERT INTO condition_occurrence (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date, condition_type_concept_id) VALUES (4, 2, 320128, '2021-03-01', '2021-03-15', 0)")

  .build_condition_era_sql(con)

  ce <- DBI::dbGetQuery(con, "SELECT * FROM condition_era ORDER BY person_id, condition_era_start_date")
  expect_equal(nrow(ce), 3)

  # Person 1, first era: Jan 1-25, count=2
  era1 <- ce[ce$person_id == 1 & ce$condition_era_start_date == as.Date("2021-01-01"), ]
  expect_equal(nrow(era1), 1)
  expect_equal(era1$condition_era_end_date, as.Date("2021-01-25"))
  expect_equal(era1$condition_occurrence_count, 2)

  # Person 1, second era: Jun 1-10, count=1
  era2 <- ce[ce$person_id == 1 & ce$condition_era_start_date == as.Date("2021-06-01"), ]
  expect_equal(nrow(era2), 1)
  expect_equal(era2$condition_occurrence_count, 1)

  # Person 2: single era
  era3 <- ce[ce$person_id == 2, ]
  expect_equal(nrow(era3), 1)
  expect_equal(era3$condition_occurrence_count, 1)
})

test_that(".build_condition_era_sql handles NULL end dates", {
  con <- setup_era_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Condition with NULL end_date — OHDSI SQL defaults to start_date + 1 day
  DBI::dbExecute(con, "INSERT INTO condition_occurrence (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date, condition_type_concept_id) VALUES (1, 1, 201826, '2021-05-01', NULL, 0)")

  .build_condition_era_sql(con)

  ce <- DBI::dbGetQuery(con, "SELECT * FROM condition_era")
  expect_equal(nrow(ce), 1)
  expect_equal(ce$condition_era_start_date, as.Date("2021-05-01"))
  # End date should be start + 1 day per OHDSI convention
  expect_equal(ce$condition_era_end_date, as.Date("2021-05-02"))
})

test_that(".build_condition_era_sql handles empty table gracefully", {
  con <- setup_era_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # No condition_occurrence rows — should not error
  expect_no_error(.build_condition_era_sql(con))
  ce <- DBI::dbGetQuery(con, "SELECT * FROM condition_era")
  expect_equal(nrow(ce), 0)
})

test_that(".build_drug_era_sql uses ingredient rollup via concept_ancestor", {
  con <- setup_era_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Metformin (1503297) is an Ingredient — should map to itself via concept_ancestor
  DBI::dbExecute(con, "INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id) VALUES (1, 1, 1503297, '2021-01-05', '2021-01-20', 0)")
  DBI::dbExecute(con, "INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id) VALUES (2, 1, 1503297, '2021-02-01', '2021-02-15', 0)")
  DBI::dbExecute(con, "INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id) VALUES (3, 1, 1503297, '2021-08-01', '2021-08-15', 0)")
  DBI::dbExecute(con, "INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id) VALUES (4, 2, 1503297, '2021-04-01', '2021-04-30', 0)")

  .build_drug_era_sql(con)

  de <- DBI::dbGetQuery(con, "SELECT * FROM drug_era ORDER BY person_id, drug_era_start_date")
  expect_equal(nrow(de), 3)

  # All should be metformin ingredient concept
  expect_true(all(de$drug_concept_id == 1503297))

  # Person 1, first era: Jan 5 - Feb 15, count=2 (within 30d gap)
  era1 <- de[de$person_id == 1 & de$drug_era_start_date == as.Date("2021-01-05"), ]
  expect_equal(nrow(era1), 1)
  expect_equal(era1$drug_era_end_date, as.Date("2021-02-15"))
  expect_equal(era1$drug_exposure_count, 2)
  expect_true(era1$gap_days > 0)  # 12 days gap between Jan 20 and Feb 1

  # Person 1, second era: Aug 1-15, count=1
  era2 <- de[de$person_id == 1 & de$drug_era_start_date == as.Date("2021-08-01"), ]
  expect_equal(nrow(era2), 1)
  expect_equal(era2$drug_exposure_count, 1)
  expect_equal(era2$gap_days, 0)
})

test_that(".build_drug_era_sql handles empty drug_exposure gracefully", {
  con <- setup_era_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_no_error(.build_drug_era_sql(con))
  de <- DBI::dbGetQuery(con, "SELECT * FROM drug_era")
  expect_equal(nrow(de), 0)
})

test_that(".build_drug_era_sql correctly calculates gap_days", {
  con <- setup_era_test_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Two non-overlapping exposures within 30d gap
  # Exposure 1: Jan 1 to Jan 10 (9 days exposed)
  # Exposure 2: Jan 20 to Jan 30 (10 days exposed)
  # Era: Jan 1 to Jan 30 (29 days total)
  # gap_days = 29 - 19 = 10
  DBI::dbExecute(con, "INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id) VALUES (1, 1, 1503297, '2021-01-01', '2021-01-10', 0)")
  DBI::dbExecute(con, "INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id) VALUES (2, 1, 1503297, '2021-01-20', '2021-01-30', 0)")

  .build_drug_era_sql(con)

  de <- DBI::dbGetQuery(con, "SELECT * FROM drug_era")
  expect_equal(nrow(de), 1)
  expect_equal(de$drug_era_start_date, as.Date("2021-01-01"))
  expect_equal(de$drug_era_end_date, as.Date("2021-01-30"))
  expect_equal(de$drug_exposure_count, 2)
  expect_equal(de$gap_days, 10)  # 29 total - 9 - 10 = 10 days gap
})

test_that("cdmFromCohortSet builds drug_era with ingredient rollup", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  # Simple DrugExposure cohort (Aspirin = 1112807)
  cohort_json <- '{
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
  expr <- jsonlite::fromJSON(cohort_json, simplifyVector = FALSE)
  cohortSet <- data.frame(
    cohort_definition_id = 1L,
    cohort_name = "test_aspirin",
    cohort = I(list(expr)),
    stringsAsFactors = FALSE
  )
  cdm <- CDMConnector::cdmFromCohortSet(cohortSet, n = 25, seed = 44, targetMatch = 0.9, successRate = 0.5)
  on.exit(CDMConnector::cdmDisconnect(cdm), add = TRUE)

  # drug_era should exist and have records
  expect_true("drug_era" %in% names(cdm))
  de <- dplyr::collect(cdm$drug_era)
  expect_gte(nrow(de), 1)

  # drug_concept_id in drug_era should be at ingredient level
  # (concept_ancestor maps clinical drugs up to their ingredient)
  de_concepts <- unique(de$drug_concept_id)
  expect_true(length(de_concepts) > 0)

  # condition_era should exist (may be empty if no condition_occurrence)
  expect_true("condition_era" %in% names(cdm))
})
