# Miscellaneous tests for various files — table_refs, cohortCollapse, Eunomia, cdmSubset, etc.

# --- table_refs (generateConceptCohortSet.R) ---

test_that("table_refs returns correct columns for condition domain", {
  result <- CDMConnector:::table_refs("condition")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$table_name, "condition_occurrence")
  expect_equal(result$concept_id, "condition_concept_id")
})

test_that("table_refs returns correct columns for drug domain", {
  result <- CDMConnector:::table_refs("drug")
  expect_equal(nrow(result), 1)
  expect_equal(result$table_name, "drug_exposure")
})

test_that("table_refs returns multiple domains", {
  result <- CDMConnector:::table_refs(c("condition", "drug", "measurement"))
  expect_equal(nrow(result), 3)
})

test_that("table_refs returns all 7 domains", {
  all_domains <- c("condition", "drug", "procedure", "observation", "measurement", "visit", "device")
  result <- CDMConnector:::table_refs(all_domains)
  expect_equal(nrow(result), 7)
})

test_that("table_refs returns empty for unknown domain", {
  result <- CDMConnector:::table_refs("unknown")
  expect_equal(nrow(result), 0)
})

# --- cohortCollapse (generateConceptCohortSet.R) ---

test_that("cohortCollapse collapses overlapping periods on data.frame", {
  df <- data.frame(
    cohort_definition_id = c(1L, 1L, 1L),
    subject_id = c(1L, 1L, 1L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-01-15", "2020-06-01")),
    cohort_end_date = as.Date(c("2020-02-01", "2020-02-15", "2020-07-01"))
  )
  result <- CDMConnector:::cohortCollapse(df)
  expect_s3_class(result, "data.frame")
  # First two overlap (Jan 15 is before Feb 1), third doesn't
  expect_equal(nrow(result), 2)
})

test_that("cohortCollapse handles non-overlapping periods", {
  df <- data.frame(
    cohort_definition_id = c(1L, 1L),
    subject_id = c(1L, 1L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-06-01")),
    cohort_end_date = as.Date(c("2020-02-01", "2020-07-01"))
  )
  result <- CDMConnector:::cohortCollapse(df)
  expect_equal(nrow(result), 2)
})

test_that("cohortCollapse handles single record", {
  df <- data.frame(
    cohort_definition_id = 1L,
    subject_id = 1L,
    cohort_start_date = as.Date("2020-01-01"),
    cohort_end_date = as.Date("2020-02-01")
  )
  result <- CDMConnector:::cohortCollapse(df)
  expect_equal(nrow(result), 1)
})

test_that("cohortCollapse errors on non-data.frame", {
  expect_error(CDMConnector:::cohortCollapse("not a df"))
})

test_that("cohortCollapse collapses on tbl_sql", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(
    cohort_definition_id = c(1L, 1L),
    subject_id = c(1L, 1L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-01-15")),
    cohort_end_date = as.Date(c("2020-02-01", "2020-02-15"))
  )
  DBI::dbWriteTable(con, "cohort_test", df)
  tbl_ref <- dplyr::tbl(con, "cohort_test")

  result <- CDMConnector:::cohortCollapse(tbl_ref) %>% dplyr::collect()
  expect_equal(nrow(result), 1)  # Overlapping, collapsed
})

# --- exampleDatasets (Eunomia.R) ---

test_that("exampleDatasets returns character vector", {
  datasets <- exampleDatasets()
  expect_type(datasets, "character")
  expect_true(length(datasets) > 0)
  expect_true("GiBleed" %in% datasets)
})

# --- eunomiaIsAvailable ---

test_that("eunomiaIsAvailable returns logical", {
  result <- eunomiaIsAvailable()
  expect_type(result, "logical")
})

# --- normalize_schema_str (optimizer.R) ---

test_that("normalize_schema_str returns string for character schema", {
  result <- CDMConnector:::normalize_schema_str("main")
  expect_equal(result, "main")
})

test_that("normalize_schema_str handles named vector schema", {
  result <- CDMConnector:::normalize_schema_str(c(schema = "main", prefix = "pre_"))
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

# --- date_string_to_sql (circe.R or execution_graph.R helpers) ---

test_that("date_string_to_sql converts date strings", {
  result <- CDMConnector:::date_string_to_sql("2020-01-15")
  expect_true(grepl("2020", result))
  expect_true(grepl("15", result))
})

# --- translate (sqlrender wrapper) ---

test_that("translate converts SQL to duckdb dialect", {
  sql <- "SELECT TOP 10 * FROM person"
  result <- translate(sql, targetDialect = "duckdb")
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("translate handles DATEADD", {
  sql <- "SELECT DATEADD(day, 30, start_date) FROM t"
  result <- translate(sql, targetDialect = "duckdb")
  expect_true(is.character(result))
})

# --- render (sqlrender wrapper) ---

test_that("render replaces parameters", {
  sql <- "SELECT * FROM @schema.person WHERE person_id = @id"
  result <- render(sql, schema = "main", id = 1)
  expect_true(grepl("main", result))
  expect_true(grepl("1", result))
})

test_that("render handles conditionals", {
  sql <- "SELECT * FROM t {@include}?{WHERE x > 0}"
  result <- render(sql, include = "true")
  expect_true(grepl("WHERE x > 0", result))
})

# --- split_sql_core (internal) ---

test_that("split_sql_core splits statements", {
  sql <- "SELECT 1; SELECT 2; SELECT 3"
  result <- CDMConnector:::split_sql_core(sql)
  expect_equal(length(result), 3)
})

# --- cdmSubset / cdmSample ---

test_that("cdmSubset works", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  person_ids <- cdm$person %>%
    dplyr::select("person_id") %>%
    head(10) %>%
    dplyr::pull("person_id")

  subset_cdm <- cdmSubset(cdm, personId = person_ids)
  person_count <- subset_cdm$person %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull("n")
  expect_equal(person_count, 10)
})

test_that("cdmSample works", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  sample_cdm <- cdmSample(cdm, n = 5)
  person_count <- sample_cdm$person %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull("n")
  expect_equal(person_count, 5)
})

# --- cdmFlatten ---

test_that("cdmFlatten works on condition domain", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  result <- cdmFlatten(cdm, domain = c("condition_occurrence"))
  # Result is a tbl_sql, collect it
  result_df <- dplyr::collect(result)
  expect_s3_class(result_df, "data.frame")
  expect_true(nrow(result_df) > 0)
})
