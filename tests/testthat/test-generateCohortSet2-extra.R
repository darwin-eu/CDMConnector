# Integration tests for generateCohortSet2 (optimizer.R pipeline)
# This exercises: atlas_json_to_sql_batch, buildBatchCohortQuery,
# build_execution_dag, emit_dag_sql, rewrite_to_domain_caches,
# quote_cdm_table_refs, resolve_literal_conditionals, drop_prefixed_tables,
# translate_cohort_stmts, collect_batch_used_domains_from_cohorts, etc.

test_that("generateCohortSet2 generates cohorts from cohort set", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cohort_set, name = "gen2_test")

  # Should have the cohort table

  expect_true("gen2_test" %in% names(cdm))

  # Collect and check structure
  result <- dplyr::collect(cdm$gen2_test)
  expect_true(nrow(result) >= 0)
  expect_true("cohort_definition_id" %in% names(result))
  expect_true("subject_id" %in% names(result))
  expect_true("cohort_start_date" %in% names(result))
  expect_true("cohort_end_date" %in% names(result))
})

test_that("generateCohortSet2 results match generateCohortSet", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  # Generate with both methods
  cdm <- generateCohortSet(cdm, cohort_set, name = "gen1_cohort", overwrite = TRUE)
  cdm <- generateCohortSet2(cdm, cohort_set, name = "gen2_cohort")

  gen1 <- dplyr::collect(cdm$gen1_cohort) |>
    dplyr::arrange(cohort_definition_id, subject_id, cohort_start_date)
  gen2 <- dplyr::collect(cdm$gen2_cohort) |>
    dplyr::arrange(cohort_definition_id, subject_id, cohort_start_date)

  # Row counts should match per cohort
  gen1_counts <- gen1 |> dplyr::count(cohort_definition_id)
  gen2_counts <- gen2 |> dplyr::count(cohort_definition_id)

  for (cid in gen1_counts$cohort_definition_id) {
    n1 <- gen1_counts$n[gen1_counts$cohort_definition_id == cid]
    n2 <- gen2_counts$n[gen2_counts$cohort_definition_id == cid]
    expect_equal(n1, n2, info = paste("Cohort", cid, "row count mismatch"))
  }
})

test_that("generateCohortSet2 with single cohort", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  # Use just the first cohort
  single_set <- cohort_set[1, ]

  cdm <- generateCohortSet2(cdm, single_set, name = "single_gen2")
  result <- dplyr::collect(cdm$single_gen2)
  expect_true(nrow(result) >= 0)
  expect_true(all(result$cohort_definition_id == single_set$cohort_definition_id))
})

test_that("generateCohortSet2 with writePrefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = c(schema = "main", prefix = "test_"),
                     writePrefix = "test_")
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cohort_set, name = "prefix_gen2")
  result <- dplyr::collect(cdm$prefix_gen2)
  expect_true(nrow(result) >= 0)
})

test_that("generateCohortSet2 with cohorts3 (conditions with descendants)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cohort_set <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cohort_set, name = "gen2_cohorts3")
  result <- dplyr::collect(cdm$gen2_cohorts3)
  expect_true(nrow(result) >= 0)
})

# --- atlas_json_to_sql (single cohort) ---

test_that("atlas_json_to_sql generates SQL for single cohort", {
  cohort_dir <- system.file("cohorts1", package = "CDMConnector")
  json_files <- list.files(cohort_dir, pattern = "\\.json$", full.names = TRUE)
  skip_if(length(json_files) == 0)

  result <- CDMConnector:::atlas_json_to_sql(
    json_files[1],
    cohort_id = 1L,
    cdm_schema = "main",
    target_schema = "main",
    target_table = "cohort",
    target_dialect = "duckdb",
    render = TRUE
  )
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
  expect_true(grepl("cohort", result, ignore.case = TRUE))
})

test_that("atlas_json_to_sql with render=FALSE keeps parameters", {
  cohort_dir <- system.file("cohorts1", package = "CDMConnector")
  json_files <- list.files(cohort_dir, pattern = "\\.json$", full.names = TRUE)
  skip_if(length(json_files) == 0)

  result <- CDMConnector:::atlas_json_to_sql(
    json_files[1],
    cohort_id = 1L,
    render = FALSE,
    target_dialect = NULL
  )
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

# --- atlas_json_to_sql_batch ---

test_that("atlas_json_to_sql_batch non-optimized path", {
  cohort_dir <- system.file("cohorts1", package = "CDMConnector")
  json_files <- list.files(cohort_dir, pattern = "\\.json$", full.names = TRUE)
  skip_if(length(json_files) == 0)

  result <- CDMConnector:::atlas_json_to_sql_batch(
    json_inputs = as.list(json_files),
    cdm_schema = "main",
    results_schema = "main",
    target_dialect = "duckdb",
    optimize = FALSE
  )
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

test_that("atlas_json_to_sql_batch optimized path", {
  cohort_dir <- system.file("cohorts1", package = "CDMConnector")
  json_files <- list.files(cohort_dir, pattern = "\\.json$", full.names = TRUE)
  skip_if(length(json_files) == 0)

  result <- CDMConnector:::atlas_json_to_sql_batch(
    json_inputs = as.list(json_files),
    cdm_schema = "main",
    results_schema = "main",
    target_dialect = "duckdb",
    optimize = TRUE
  )
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

test_that("atlas_json_to_sql_batch with data.frame input", {
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  result <- CDMConnector:::atlas_json_to_sql_batch(
    json_inputs = cohort_set,
    cdm_schema = "main",
    results_schema = "main",
    target_dialect = "duckdb",
    optimize = TRUE
  )
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

# --- buildBatchCohortQuery ---

test_that("buildBatchCohortQuery builds SQL from cohort list", {
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  cohort_list <- lapply(cohort_set$cohort, function(c) {
    CDMConnector:::cohortExpressionFromJson(as.character(jsonlite::toJSON(c, auto_unbox = TRUE, null = "null")))
  })
  cohort_ids <- as.integer(cohort_set$cohort_definition_id)

  result <- CDMConnector:::buildBatchCohortQuery(
    cohort_list, cohort_ids,
    options = list(cdm_schema = "main", results_schema = "main")
  )
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

test_that("buildBatchCohortQuery errors on empty list", {
  expect_error(
    CDMConnector:::buildBatchCohortQuery(list(), integer(0)),
    "must not be empty"
  )
})

test_that("buildBatchCohortQuery errors on mismatched lengths", {
  expect_error(
    CDMConnector:::buildBatchCohortQuery(list(list()), c(1L, 2L)),
    "must match"
  )
})
