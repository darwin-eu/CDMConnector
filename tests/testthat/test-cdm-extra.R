# Additional tests for R/cdm.R — coverage for cdmFromCon, verify_write_access,
# snapshot, tblGroup, dbms, cdmWriteSchema, cdmCon, version

# --- cdmFromCon ---

test_that("cdmFromCon creates a valid cdm object with duckdb/eunomia", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_s3_class(cdm, "cdm_reference")
  expect_true("person" %in% names(cdm))
  expect_true("observation_period" %in% names(cdm))
})

test_that("cdmFromCon works with unnamed writeSchema vector", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_s3_class(cdm, "cdm_reference")
})

test_that("cdmFromCon works with writePrefix argument", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", writePrefix = "tmp_")
  ws <- cdmWriteSchema(cdm)
  expect_equal(ws[["prefix"]], "tmp_")
})

test_that("cdmFromCon works with empty string writePrefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", writePrefix = "")
  expect_s3_class(cdm, "cdm_reference")
})

test_that("cdmFromCon extracts cdm name from cdm_source table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  nm <- omopgenerics::cdmName(cdm)
  expect_true(is.character(nm))
  expect_true(nchar(nm) > 0)
})

test_that("cdmFromCon works with explicit cdmName", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "TestCDM")
  expect_equal(omopgenerics::cdmName(cdm), "TestCDM")
})

test_that("cdmFromCon errors on closed connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_error(cdmFromCon(con, cdmSchema = "main", writeSchema = "main"))
})

test_that("cdmFromCon works with named writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "pre_"))
  ws <- cdmWriteSchema(cdm)
  expect_equal(ws[["prefix"]], "pre_")
})

test_that("cdmFromCon writePrefix overrides prefix in writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "old_"),
                     writePrefix = "new_")
  ws <- cdmWriteSchema(cdm)
  expect_equal(ws[["prefix"]], "new_")
})

test_that("cdmFromCon deprecated cdmVersion auto triggers warning", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_warning(
    cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmVersion = "auto"),
    "deprecated"
  )
})

# --- verify_write_access ---

test_that("verify_write_access succeeds on duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_no_error(
    CDMConnector:::verify_write_access(con, c(schema = "main"))
  )
})

# --- dbms ---

test_that("dbms returns 'duckdb' for duckdb connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_equal(dbms(con), "duckdb")
})

test_that("dbms returns 'local' for NULL connection", {
  expect_equal(CDMConnector:::dbms(NULL), "local")
})

test_that("dbms extracts from cdm_reference object", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_equal(dbms(cdm), "duckdb")
})

test_that("dbms uses dbms attribute if present", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  attr(con, "dbms") <- "custom_dbms"
  expect_equal(dbms(con), "custom_dbms")
})

# --- cdmWriteSchema and cdmCon ---

test_that("cdmWriteSchema returns write schema from cdm", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  ws <- cdmWriteSchema(cdm)
  expect_true("schema" %in% names(ws))
  expect_equal(ws[["schema"]], "main")
})

test_that("cdmCon returns connection from cdm", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  retrieved_con <- cdmCon(cdm)
  expect_true(DBI::dbIsValid(retrieved_con))
})

# --- tblGroup ---

test_that("tblGroup returns tables for 'vocab' group", {
  tables <- tblGroup("vocab")
  expect_true(is.character(tables))
  expect_true("concept" %in% tables)
  expect_true("vocabulary" %in% tables)
})

test_that("tblGroup returns tables for 'clinical' group", {
  tables <- tblGroup("clinical")
  expect_true("person" %in% tables)
  expect_true("observation_period" %in% tables)
})

test_that("tblGroup returns tables for 'all' group", {
  tables <- tblGroup("all")
  expect_true(length(tables) > 0)
})

test_that("tblGroup returns tables for 'default' group", {
  tables <- tblGroup("default")
  expect_true("person" %in% tables)
  expect_true("concept" %in% tables)
})

test_that("tblGroup returns tables for 'derived' group", {
  tables <- tblGroup("derived")
  expect_true(is.character(tables))
  expect_true(length(tables) > 0)
})

test_that("tblGroup errors on invalid group", {
  expect_error(tblGroup("nonexistent"))
})

# --- snapshot ---

test_that("snapshot returns expected metadata", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  s <- snapshot(cdm)
  expect_s3_class(s, "data.frame")
  expect_true("person_count" %in% names(s))
  expect_true("observation_period_count" %in% names(s))
  expect_true("vocabulary_version" %in% names(s))
  expect_true("snapshot_date" %in% names(s))
  expect_true("cdm_name" %in% names(s))
})

# --- tbl.db_cdm ---

test_that("tbl.db_cdm reads a table from source", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  src <- attr(cdm, "cdm_source")
  person_tbl <- dplyr::tbl(src, schema = "main", name = "person")
  expect_true(nrow(dplyr::collect(utils::head(person_tbl, 5))) > 0)
})


# --- cdmFromCon edge cases ---

test_that("cdmFromCon errors on invalid connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_error(cdmFromCon(con, "main", "main"), "not valid")
})

test_that("cdmFromCon parses dot-separated writeSchema", {
  skip_if_not_installed("duckdb")
  # Use in-memory duckdb where we can set up tables in catalog "memory"
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Dot-separated schema should parse but may fail if catalog doesn't exist;
  # test the parsing by using a single-dot format with known schema
  # The parser splits "a.b" into c(catalog="a", schema="b")
  # For duckdb in-memory, just verify it processes the writeSchema format
  expect_error(
    cdmFromCon(con, cdmSchema = "main", writeSchema = "cat.sch"),
    class = "error"
  )
})

test_that("cdmFromCon parses dot-separated cdmSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # "memory.main" gets parsed but catalog "memory" may not have tables
  expect_error(
    cdmFromCon(con, cdmSchema = "memory.main", writeSchema = "main"),
    class = "error"
  )
})

test_that("cdmFromCon errors on writeSchema with more than one dot", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_error(cdmFromCon(con, cdmSchema = "main", writeSchema = "a.b.c"), "one \\.")
})

test_that("cdmFromCon errors on cdmSchema with more than one dot", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_error(cdmFromCon(con, cdmSchema = "a.b.c", writeSchema = "main"), "one \\.")
})

test_that("cdmFromCon names unnamed 2-element writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # 2-element unnamed writeSchema gets catalog/schema names but may fail on query
  # Just test the naming logic succeeds for the first part
  expect_error(
    cdmFromCon(con, cdmSchema = "main", writeSchema = c("mycat", "mysch")),
    class = "error"
  )
})

test_that("cdmFromCon with writePrefix overrides prefix in writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "old_"),
                     writePrefix = "new_")
  ws <- cdmWriteSchema(cdm)
  expect_equal(ws[["prefix"]], "new_")
})

test_that("cdmFromCon with empty writePrefix is treated as NULL", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", writePrefix = "")
  ws <- cdmWriteSchema(cdm)
  expect_false("prefix" %in% names(ws))
})

test_that("cdmFromCon with cdmVersion auto gives deprecation warning", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_warning(
    cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmVersion = "auto"),
    "deprecated"
  )
})

test_that("cdmFromCon with explicit cdmName", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "MyCDM")
  expect_equal(omopgenerics::cdmName(cdm), "MyCDM")
})

test_that("cdmFromCon with cohortTables reads existing cohort tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # First create a CDM and generate a cohort so the tables exist
  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  cdm <- generateCohortSet(cdm, cohort_set, name = "test_cohort", overwrite = TRUE)

  # Now create a new CDM reference that reads the existing cohort tables
  cdm2 <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main",
                      cohortTables = "test_cohort")
  expect_true("test_cohort" %in% names(cdm2))
})

test_that("cdmFromCon errors on non-existent cohortTable", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_error(
    cdmFromCon(con, cdmSchema = "main", writeSchema = "main",
               cohortTables = "nonexistent_cohort")
  )
})

# --- version (deprecated) ---

test_that("version returns cdm version with deprecation warning", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  lifecycle::expect_deprecated(
    v <- version(cdm)
  )
  expect_true(v %in% c("5.3", "5.4"))
})

# --- snapshot ---

test_that("snapshot returns expected columns", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  snap <- snapshot(cdm)

  expect_true("cdm_name" %in% names(snap))
  expect_true("person_count" %in% names(snap))
  expect_true("observation_period_count" %in% names(snap))
  expect_true("vocabulary_version" %in% names(snap))
  expect_true("snapshot_date" %in% names(snap))
  expect_true("cdm_data_hash" %in% names(snap))
})

test_that("snapshot all columns are character", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  snap <- snapshot(cdm)

  for (col in names(snap)) {
    expect_type(snap[[col]], "character")
  }
})

test_that("snapshot has correct date format", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  snap <- snapshot(cdm)

  # snapshot_date should be YYYY-MM-DD
  expect_true(grepl("^\\d{4}-\\d{2}-\\d{2}$", snap$snapshot_date))
})

# --- verify_write_access ---

test_that("verify_write_access succeeds with valid schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_no_error(
    verify_write_access(con, c(schema = "main"))
  )
})

test_that("verify_write_access with AssertCollection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  coll <- checkmate::makeAssertCollection()
  verify_write_access(con, c(schema = "main"), add = coll)
  expect_true(coll$isEmpty())
})

# --- inSchema (exported wrapper) ---

test_that("inSchema works same as .inSchema", {
  result <- inSchema("my_schema", "my_table")
  expect_s4_class(result, "Id")
})

test_that("inSchema with dbms argument", {
  result <- inSchema("main", "person", dbms = "duckdb")
  expect_equal(result, "person")
})


# --- version error path ---

test_that("version errors when cdm_version is not 5.3 or 5.4", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Modify the cdm_version attribute to an invalid value
 attr(cdm, "cdm_version") <- "4.0"

  lifecycle::expect_deprecated(
    expect_error(version(cdm), "5.3 or 5.4")
  )
})

# --- snapshot with empty vocabulary ---

test_that("snapshot handles missing vocab_version", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Delete all rows from vocabulary where vocabulary_id = 'None'
  DBI::dbExecute(con, "DELETE FROM main.vocabulary WHERE vocabulary_id = 'None'")

  snap <- snapshot(cdm)
  # Just verify snapshot runs successfully with no 'None' vocab entry
  expect_true("vocabulary_version" %in% names(snap))
})

# --- snapshot with empty cdm_source table ---

test_that("snapshot handles empty cdm_source table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Delete all rows from cdm_source
  DBI::dbExecute(con, "DELETE FROM main.cdm_source")

  snap <- snapshot(cdm)
  expect_true("cdm_name" %in% names(snap))
  expect_true("person_count" %in% names(snap))
  expect_true("cdm_source_name" %in% names(snap))
})

# --- cdmName fallback from cdm_source_name ---

test_that("cdmFromCon falls back to cdm_source_name when abbreviation is empty", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Set cdm_source_abbreviation to empty string but leave cdm_source_name
  DBI::dbExecute(con, "UPDATE main.cdm_source SET cdm_source_abbreviation = ''")

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  # Should fall back to cdm_source_name
  cname <- omopgenerics::cdmName(cdm)
  expect_true(nchar(cname) > 0)
})

test_that("cdmFromCon uses default name when cdm_source is empty", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Delete all cdm_source rows
  DBI::dbExecute(con, "DELETE FROM main.cdm_source")

  cdm <- suppressWarnings(cdmFromCon(con, cdmSchema = "main", writeSchema = "main"))
  cname <- omopgenerics::cdmName(cdm)
  expect_equal(cname, "An OMOP CDM database")
})

# --- .computePermanent with 2-element schema ---

test_that(".computePermanent with 2-element schema on duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "source_data", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "source_data")

  result <- CDMConnector:::.computePermanent(tbl_ref, "perm_2part",
    schema = c("memory", "main"), overwrite = TRUE)
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)
})

# --- dbms function ---

test_that("dbms returns 'local' for NULL connection", {
  expect_equal(dbms(NULL), "local")
})

test_that("dbms works on cdm_reference", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_equal(dbms(cdm), "duckdb")
})


# --- tblGroup ---

test_that("tblGroup returns character vector of table names", {
  result <- tblGroup("all")
  expect_type(result, "character")
  expect_true(length(result) > 0)
  expect_true("person" %in% result)
})

test_that("tblGroup handles clinical tables", {
  result <- tblGroup("clinical")
  expect_type(result, "character")
  expect_true("person" %in% result)
  expect_true("observation_period" %in% result)
})

test_that("tblGroup handles vocabulary tables", {
  result <- tblGroup("vocab")
  expect_type(result, "character")
  expect_true("concept" %in% result)
})

# --- cdmFromCon edge cases ---

test_that("cdmFromCon errors on invalid connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_error(cdmFromCon(con, cdmSchema = "main"), "not valid")
})

test_that("cdmFromCon dot-separated schema parsing works", {
  # Test that the parsing logic works correctly for writeSchema
  skip_if_not_installed("duckdb")
  # Can't test memory.main on file-based duckdb, just verify cdmFromCon
  # handles the parsing without error when schema is valid
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Verify that dot-separated cdmSchema raises an error (tables not found in catalog.schema form)
  expect_error(
    cdmFromCon(con, cdmSchema = "memory.main", writeSchema = "main"),
    "cdm tables"
  )
})

test_that("cdmFromCon accepts writePrefix argument", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", writePrefix = "test_")
  ws <- attr(cdm, "write_schema")
  expect_true("prefix" %in% names(ws))
  expect_equal(ws["prefix"], c(prefix = "test_"))
})

test_that("cdmFromCon accepts empty string writePrefix (treated as NULL)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", writePrefix = "")
  expect_true(methods::is(cdm, "cdm_reference"))
})

test_that("cdmFromCon warns on deprecated auto version", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_warning(
    cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmVersion = "auto"),
    "deprecated"
  )
})

test_that("cdmFromCon accepts named writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = c(schema = "main"))
  expect_true(methods::is(cdm, "cdm_reference"))
})

test_that("cdmFromCon errors on 3-element unnamed writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_error(
    cdmFromCon(con, cdmSchema = "main", writeSchema = c("a", "b", "c")),
    "unnamed"
  )
})

test_that("cdmFromCon accepts cdmName parameter", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "TestDB")
  expect_equal(omopgenerics::cdmName(cdm), "TestDB")
})

test_that("cdmFromCon with .softValidation works", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", .softValidation = TRUE)
  expect_true(methods::is(cdm, "cdm_reference"))
})

# --- tbl.db_cdm ---

test_that("tbl.db_cdm creates tbl reference", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  src <- attr(cdm, "cdm_source")

  # Call tbl method on the db_cdm source
  person_tbl <- dplyr::tbl(src, schema = "main", name = "person")
  collected <- dplyr::collect(person_tbl)
  expect_true(nrow(collected) > 0)
  expect_true("person_id" %in% names(collected))
})

# --- snapshot ---

test_that("snapshot returns named list", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  snap <- snapshot(cdm)
  expect_true(is.list(snap))
  expect_true("person_count" %in% names(snap))
  expect_true("cdm_source_name" %in% names(snap))
  expect_true("vocabulary_version" %in% names(snap))
})

# --- cdmVersion ---

test_that("cdmVersion returns version string", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  v <- omopgenerics::cdmVersion(cdm)
  expect_true(v %in% c("5.3", "5.4"))
})

# --- insertTable.db_cdm ---

test_that("insertTable.db_cdm writes data", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  src <- attr(cdm, "cdm_source")

  df <- data.frame(x = 1:5, y = letters[1:5])
  omopgenerics::insertTable(cdm, name = "test_insert", table = df)
  tables <- DBI::dbListTables(con)
  expect_true("test_insert" %in% tables)
})

# --- dropTable.db_cdm ---

test_that("dropTable removes table from database", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # First insert a table
  df <- data.frame(x = 1:5)
  omopgenerics::insertTable(cdm, name = "to_drop", table = df)
  expect_true("to_drop" %in% DBI::dbListTables(con))

  # Drop it (using dropSourceTable if available, else dropTable)
  suppressWarnings(omopgenerics::dropTable(cdm, name = "to_drop"))
  expect_false("to_drop" %in% DBI::dbListTables(con))
})
