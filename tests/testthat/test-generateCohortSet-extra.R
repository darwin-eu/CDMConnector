# Additional tests for R/generateCohortSet.R — coverage for readCohortSet,
# extractCodesetIds, createCodelistDataframe, getInclusionMaskId, etc.

# --- readCohortSet ---

test_that("readCohortSet reads a directory of JSON files", {
  cohort_set <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
  expect_s3_class(cohort_set, "CohortSet")
  expect_true(nrow(cohort_set) > 0)
  expect_true(all(c("cohort_definition_id", "cohort_name", "cohort", "json") %in% names(cohort_set)))
})

test_that("readCohortSet reads a single JSON file", {
  json_path <- system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector")
  cohort_set <- readCohortSet(json_path)
  expect_s3_class(cohort_set, "CohortSet")
  expect_equal(nrow(cohort_set), 1)
})

test_that("readCohortSet errors on non-existent path", {
  expect_error(readCohortSet("/nonexistent/path"), "does not exist")
})

test_that("readCohortSet errors on non-directory non-json file", {
  tmp <- tempfile(fileext = ".txt")
  writeLines("not json", tmp)
  on.exit(unlink(tmp))
  expect_error(readCohortSet(tmp), "directory or a .json")
})

test_that("readCohortSet reads directory with numeric-named JSON files", {
  cohort_set <- readCohortSet(system.file("cohorts4", package = "CDMConnector"))
  expect_s3_class(cohort_set, "CohortSet")
  # file is named "100.json", should get cohort_definition_id = 100
  # and name prefixed with "cohort_"
  expect_true(any(grepl("^cohort_", cohort_set$cohort_name)))
})

test_that("readCohortSet errors on empty directory", {
  tmp_dir <- tempdir()
  empty_dir <- file.path(tmp_dir, "empty_cohorts_test")
  dir.create(empty_dir, showWarnings = FALSE)
  on.exit(unlink(empty_dir, recursive = TRUE))
  expect_error(readCohortSet(empty_dir), "No .json files found")
})

test_that("readCohortSet reads cohorts3 directory", {
  cohort_set <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))
  expect_s3_class(cohort_set, "CohortSet")
  expect_true(nrow(cohort_set) >= 2)
})

# --- extractCodesetIds ---

test_that("extractCodesetIds extracts IDs from nested list", {
  x <- list(
    DrugExposure = list(CodesetId = 0),
    ConditionOccurrence = list(CodesetId = 1)
  )
  ids <- CDMConnector:::extractCodesetIds(x)
  expect_true(0 %in% ids)
  expect_true(1 %in% ids)
})

test_that("extractCodesetIds returns NULL for non-list input", {
  expect_null(CDMConnector:::extractCodesetIds(42))
  expect_null(CDMConnector:::extractCodesetIds("string"))
})

test_that("extractCodesetIds handles empty list", {
  ids <- CDMConnector:::extractCodesetIds(list())
  expect_null(ids)
})

# --- createCodelistDataframe ---

test_that("createCodelistDataframe creates dataframe from cohort set", {
  cohort_set <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
  df <- CDMConnector:::createCodelistDataframe(cohort_set)
  expect_s3_class(df, "data.frame")
  expect_true(all(c("cohort_definition_id", "codelist_name", "codelist_type") %in% names(df)))
})

test_that("createCodelistDataframe returns empty dataframe for empty cohort set", {
  # Create a minimal cohort set with no concept sets
  cohort_set <- dplyr::tibble(
    cohort_definition_id = 1L,
    cohort_name = "test",
    cohort = list(list(ConceptSets = list(), PrimaryCriteria = list(), InclusionRules = list())),
    json = '{"ConceptSets":[]}'
  )
  df <- CDMConnector:::createCodelistDataframe(cohort_set)
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 0)
})

# --- extractConceptsFromConceptSetList ---

test_that("extractConceptsFromConceptSetList extracts concepts", {
  conceptSets <- list(
    list(
      id = 0,
      name = "test_concept",
      expression = list(items = list(
        list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = TRUE),
        list(concept = list(CONCEPT_ID = 5678))
      ))
    )
  )
  result <- CDMConnector:::extractConceptsFromConceptSetList(conceptSets)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
  expect_true(1234 %in% result$concept_id)
  expect_true(5678 %in% result$concept_id)
  expect_true(result$include_descendants[result$concept_id == 1234])
  expect_false(result$include_descendants[result$concept_id == 5678])
})

test_that("extractConceptsFromConceptSetList handles empty items", {
  conceptSets <- list(list(id = 0, name = "empty", expression = list(items = NULL)))
  result <- CDMConnector:::extractConceptsFromConceptSetList(conceptSets)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})

# --- getInclusionMaskId ---

test_that("getInclusionMaskId returns correct mask for 1 inclusion rule", {
  result <- CDMConnector:::getInclusionMaskId(1)
  expect_type(result, "list")
  expect_equal(length(result), 2)  # x=-1 and x=0
  # For -1: all masks
  expect_equal(sort(result[[1]]), c(0, 1))
  # For 0: only masks where inclusion_0 == 1
  expect_equal(result[[2]], 1)
})

test_that("getInclusionMaskId returns correct mask for 2 inclusion rules", {
  result <- CDMConnector:::getInclusionMaskId(2)
  expect_type(result, "list")
  expect_equal(length(result), 3)  # x=-1, x=0, x=1
  # For -1: all masks (0,1,2,3)
  expect_equal(sort(result[[1]]), c(0, 1, 2, 3))
})

test_that("getInclusionMaskId returns correct mask for 3 inclusion rules", {
  result <- CDMConnector:::getInclusionMaskId(3)
  expect_type(result, "list")
  expect_equal(length(result), 4)
  # For -1: all masks (0-7)
  expect_equal(sort(result[[1]]), 0:7)
})

# --- generateCohortSet errors ---

test_that("generateCohortSet errors if cohortSet is not a dataframe", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_error(
    generateCohortSet(cdm, cohortSet = "not_a_dataframe", name = "test"),
    "dataframe"
  )
})

# --- createCohortTables helper ---

test_that("createCohortTables creates expected tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # execute_ddl to create basic CDM tables first
  CDMConnector:::createCohortTables(con, "main", "test_cohort", computeAttrition = TRUE)
  tables <- DBI::dbListTables(con)
  expect_true("test_cohort" %in% tables)
})
