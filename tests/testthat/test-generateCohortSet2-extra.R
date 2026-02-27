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

# --- generateCohortSet2 argument validation ---

test_that("generateCohortSet2 validates name argument", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  expect_error(generateCohortSet2(cdm, cs, name = "UPPERCASE"), "lowercase")
  expect_error(generateCohortSet2(cdm, cs, name = "1startsnum"), "start with a letter")
  expect_error(generateCohortSet2(cdm, cs, name = "has space"), "letters, numbers, and underscores")
})

test_that("generateCohortSet2 validates cohortSet argument", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  expect_error(generateCohortSet2(cdm, "not_a_df", name = "test"), "dataframe")
  expect_error(generateCohortSet2("not_cdm", data.frame(x = 1), name = "test"), "cdm_reference")
})

# --- overwrite behavior ---

test_that("generateCohortSet2 overwrite=TRUE allows re-run", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cs, name = "ow_test", overwrite = TRUE)
  expect_true("ow_test" %in% names(cdm))

  # Running again with overwrite=TRUE should succeed
  cdm <- generateCohortSet2(cdm, cs, name = "ow_test", overwrite = TRUE)
  expect_true("ow_test" %in% names(cdm))
})

test_that("generateCohortSet2 overwrite=FALSE errors if table exists", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cs, name = "noow_test", overwrite = TRUE)
  expect_error(
    generateCohortSet2(cdm, cs, name = "noow_test", overwrite = FALSE),
    "already exists"
  )
})

# --- computeAttrition ---

test_that("generateCohortSet2 computeAttrition=TRUE with inclusion rules", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cs, name = "att_test", computeAttrition = TRUE)

  att <- omopgenerics::attrition(cdm$att_test)
  expect_true(is.data.frame(att))
  expect_true(nrow(att) > 0)
  expect_true(all(c("cohort_definition_id", "number_records", "number_subjects",
                     "reason_id", "reason", "excluded_records", "excluded_subjects") %in% names(att)))

  # GIBleed_male has 2 inclusion rules: Male, 30 days prior observation
  # Look up its ID by name rather than assuming a fixed ID
  s <- omopgenerics::settings(cdm$att_test)
  gibleed_id <- s$cohort_definition_id[s$cohort_name == "gibleed_male"]
  att_gb <- att[att$cohort_definition_id == gibleed_id, ]
  expect_true(nrow(att_gb) >= 3)  # qualifying + 2 rules
  expect_equal(att_gb$reason[1], "Qualifying initial records")
  expect_equal(att_gb$reason[2], "Male")
  expect_equal(att_gb$reason[3], "30 days prior observation")

  # Records should decrease or stay same through sequential rules
  expect_true(att_gb$number_records[1] >= att_gb$number_records[2])
  expect_true(att_gb$number_records[2] >= att_gb$number_records[3])
})

test_that("generateCohortSet2 attrition matches v1 counts", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

  cdm <- generateCohortSet(cdm, cs, name = "v1", computeAttrition = TRUE)
  cdm <- generateCohortSet2(cdm, cs, name = "v2", computeAttrition = TRUE)

  # Cohort counts should match
  c1 <- omopgenerics::cohortCount(cdm$v1)
  c2 <- omopgenerics::cohortCount(cdm$v2)
  expect_equal(nrow(c1), nrow(c2))
  for (cid in c1$cohort_definition_id) {
    expect_equal(
      c1$number_records[c1$cohort_definition_id == cid],
      c2$number_records[c2$cohort_definition_id == cid],
      info = paste("Cohort", cid, "record count mismatch")
    )
  }

  # Attrition: both should have "Qualifying initial records" as first reason
  # and matching counts for each cohort
  att1 <- omopgenerics::attrition(cdm$v1)
  att2 <- omopgenerics::attrition(cdm$v2)
  for (cid in unique(att2$cohort_definition_id)) {
    a2 <- att2[att2$cohort_definition_id == cid, ]
    expect_equal(a2$reason[1], "Qualifying initial records")
    # v1 and v2 qualifying counts should match
    a1 <- att1[att1$cohort_definition_id == cid, ]
    if (nrow(a1) > 0 && nrow(a2) > 0) {
      expect_equal(a1$number_records[1], a2$number_records[1],
                   info = paste("Cohort", cid, "qualifying records mismatch"))
    }
  }
})

test_that("generateCohortSet2 computeAttrition=FALSE skips attrition", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cs, name = "noatt_test", computeAttrition = FALSE)

  # omopgenerics provides default attrition when NULL is passed
  att <- omopgenerics::attrition(cdm$noatt_test)
  expect_true(is.data.frame(att))
  # Should NOT have detailed inclusion rule attrition (just defaults)
  att_c3 <- att[att$cohort_definition_id == 3, ]
  expect_true(nrow(att_c3) <= 1)  # Only default row, no inclusion rules
})

# --- cohort codelist ---

test_that("generateCohortSet2 includes cohort codelist", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

  cdm <- generateCohortSet2(cdm, cs, name = "cl_test")

  # Check codelist attribute exists
  cl <- attr(cdm$cl_test, "cohort_codelist")
  expect_false(is.null(cl))
})

# --- CDM table subquery support (cdmSubset, dplyr filter) ---

test_that("generateCohortSet2 works with cdmSubset (single person)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Subset to person 6 only
  cdm_sub <- cdmSubset(cdm, personId = 6L)

  # Verify subset worked — person table should have only 1 person

  expect_equal(dplyr::pull(dplyr::tally(cdm_sub$person), "n"), 1)

  # sql_render should show a JOIN (proof of modification)
  rendered <- as.character(dbplyr::sql_render(cdm_sub$condition_occurrence))
  expect_true(grepl("JOIN", rendered, ignore.case = TRUE))

  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  # This should succeed — previously would fail with "observations outside observation period"
  cdm_sub <- generateCohortSet2(cdm_sub, cs, name = "sub_cohort")

  expect_true("sub_cohort" %in% names(cdm_sub))
  result <- dplyr::collect(cdm_sub$sub_cohort)
  expect_true(is.data.frame(result))
  expect_true(all(c("cohort_definition_id", "subject_id",
                     "cohort_start_date", "cohort_end_date") %in% names(result)))

  # All cohort results should be for person 6 only
  if (nrow(result) > 0) {
    expect_true(all(result$subject_id == 6L))
  }
})

test_that("generateCohortSet2 works with cdmSample (small subset)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Sample 5 persons
  cdm_sam <- cdmSample(cdm, n = 5, seed = 42)
  sampled_persons <- sort(dplyr::pull(cdm_sam$person, "person_id"))
  expect_equal(length(sampled_persons), 5)

  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  cdm_sam <- generateCohortSet2(cdm_sam, cs, name = "sam_cohort")

  expect_true("sam_cohort" %in% names(cdm_sam))
  result <- dplyr::collect(cdm_sam$sam_cohort)
  expect_true(is.data.frame(result))

  # All results must be within the sampled person set
  if (nrow(result) > 0) {
    expect_true(all(result$subject_id %in% sampled_persons))
  }
})

test_that("generateCohortSet2 errors on missing required CDM table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # cohorts1 uses condition_occurrence; remove it from the CDM
  cdm$condition_occurrence <- NULL

  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  expect_error(
    generateCohortSet2(cdm, cs, name = "missing_test"),
    "missing table"
  )
})

test_that("generateCohortSet2 works with dplyr-filtered CDM table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Get baseline count with unfiltered CDM
  cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  cdm <- generateCohortSet2(cdm, cs, name = "baseline")
  baseline <- dplyr::collect(cdm$baseline)

  # Now apply a dplyr filter on observation_period (restrict to recent obs)
  cdm2 <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cdm2$observation_period <- cdm2$observation_period %>%
    dplyr::filter(observation_period_start_date >= as.Date("2000-01-01"))

  # Verify filter is present in SQL
  rendered <- as.character(dbplyr::sql_render(cdm2$observation_period))
  expect_true(grepl("WHERE", rendered, ignore.case = TRUE))

  cdm2 <- generateCohortSet2(cdm2, cs, name = "filtered")
  filtered <- dplyr::collect(cdm2$filtered)

  expect_true(is.data.frame(filtered))
  expect_true(all(c("cohort_definition_id", "subject_id",
                     "cohort_start_date", "cohort_end_date") %in% names(filtered)))

  # Filtered result should have <= baseline records (fewer obs periods → fewer cohort entries)
  expect_true(nrow(filtered) <= nrow(baseline))
})
