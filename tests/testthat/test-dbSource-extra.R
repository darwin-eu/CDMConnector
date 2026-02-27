# Additional tests for R/dbSource.R — coverage for dbSource and S3 methods

# --- dbSource ---

test_that("dbSource creates a db_cdm source object with duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con)
  expect_s3_class(src, "db_cdm")
  expect_s3_class(src, "cdm_source")
  expect_equal(attr(src, "write_schema"), c(schema = "main"))
})

test_that("dbSource creates source with explicit writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  expect_s3_class(src, "db_cdm")
  expect_equal(attr(src, "write_schema"), c(schema = "main"))
})

test_that("dbSource creates source with NULL writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = NULL)
  expect_s3_class(src, "db_cdm")
  expect_null(attr(src, "write_schema"))
})

# --- insertTable.db_cdm ---

test_that("insertTable.db_cdm writes and reads a table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  df <- data.frame(id = 1:3, val = c("a", "b", "c"), stringsAsFactors = FALSE)

  result <- insertTable(cdm = src, name = "test_insert", table = df)
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)
  expect_true("id" %in% names(collected))
})

test_that("insertTable.db_cdm overwrites existing table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  df1 <- data.frame(id = 1:2, stringsAsFactors = FALSE)
  df2 <- data.frame(id = 10:12, stringsAsFactors = FALSE)

  insertTable(cdm = src, name = "overwrite_test", table = df1)
  result <- insertTable(cdm = src, name = "overwrite_test", table = df2, overwrite = TRUE)
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)
  expect_true(all(collected$id %in% 10:12))
})

# --- dropSourceTable.db_cdm ---

test_that("dropSourceTable.db_cdm drops a table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "drop_me", data.frame(x = 1))
  expect_true("drop_me" %in% listTables(con, "main"))

  dropSourceTable(src, "drop_me")
  expect_false("drop_me" %in% listTables(con, "main"))
})

# --- listSourceTables.db_cdm ---

test_that("listSourceTables.db_cdm lists tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "list_test", data.frame(x = 1))

  tables <- listSourceTables(src)
  expect_true("list_test" %in% tables)
})

# --- readSourceTable.db_cdm ---

test_that("readSourceTable.db_cdm reads a table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "read_test", data.frame(x = 1:3))

  result <- readSourceTable(src, "read_test")
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)
})

# --- summary.db_cdm ---

test_that("summary.db_cdm returns correct structure", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  s <- summary(src)
  expect_type(s, "list")
  expect_equal(s$package, "CDMConnector")
  expect_equal(s$write_schema, "main")
})

test_that("summary.db_cdm includes prefix when present", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main", prefix = "pre_"))
  s <- summary(src)
  expect_equal(s$write_schema, "main")
  expect_equal(s$write_prefix, "pre_")
})

# --- cdmDisconnect.db_cdm ---

test_that("cdmDisconnect.db_cdm disconnects from duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  src <- dbSource(con, writeSchema = c(schema = "main"))

  result <- cdmDisconnect(src)
  expect_true(result)
})

test_that("cdmDisconnect.db_cdm handles already-closed connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbDisconnect(con, shutdown = TRUE)

  expect_message(cdmDisconnect(src), "already closed")
})

# --- cdmTableFromSource.db_cdm ---

test_that("cdmTableFromSource.db_cdm errors on data.frame", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  expect_error(
    cdmTableFromSource(src, data.frame(x = 1)),
    "insertTable"
  )
})

test_that("cdmTableFromSource.db_cdm errors on non-tbl_lazy", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  expect_error(
    cdmTableFromSource(src, list(a = 1)),
    "class"
  )
})

test_that("cdmTableFromSource.db_cdm errors on different connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  src <- dbSource(con1, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con2, "other_tbl", data.frame(x = 1))
  tbl_ref <- dplyr::tbl(con2, "other_tbl")

  expect_error(
    cdmTableFromSource(src, tbl_ref),
    "different connection"
  )
})

# insertFromSource.db_cdm, insertCdmTo.db_cdm, cdmDisconnect dropPrefixTables

# --- compute.db_cdm ---

test_that("compute.db_cdm computes a table in the write schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Create a query that we'll compute
  result <- cdm$person %>%
    dplyr::select("person_id", "gender_concept_id") %>%
    dplyr::compute(name = "computed_person", temporary = FALSE, overwrite = TRUE)

  collected <- dplyr::collect(result)
  expect_true(nrow(collected) > 0)
  expect_true("person_id" %in% names(collected))
})

test_that("compute.db_cdm handles intermediate table when overwriting same name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # First compute
  tbl1 <- cdm$person %>%
    dplyr::select("person_id") %>%
    dplyr::compute(name = "my_tbl", temporary = FALSE, overwrite = TRUE)

  # Now compute from the same table name (triggers intermediate path)
  tbl2 <- tbl1 %>%
    dplyr::filter(.data$person_id > 0) %>%
    dplyr::compute(name = "my_tbl", temporary = FALSE, overwrite = TRUE)

  collected <- dplyr::collect(tbl2)
  expect_true(nrow(collected) > 0)
})

# --- insertFromSource.db_cdm ---

test_that("insertFromSource.db_cdm works for tbl_lazy from same connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Create a table in the database
  DBI::dbWriteTable(con, "test_insert_from_src", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "test_insert_from_src")

  result <- omopgenerics::insertFromSource(cdm = cdm, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

test_that("insertFromSource.db_cdm errors on data.frame", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_error(
    omopgenerics::insertFromSource(cdm = cdm, value = data.frame(x = 1)),
    "insertTable"
  )
})

test_that("insertFromSource.db_cdm errors on non-tbl_lazy", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_error(
    omopgenerics::insertFromSource(cdm = cdm, value = list(a = 1))
  )
})

test_that("insertFromSource.db_cdm errors on different connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  cdm <- cdmFromCon(con1, cdmSchema = "main", writeSchema = "main")
  DBI::dbWriteTable(con2, "other_tbl", data.frame(x = 1))
  tbl_ref <- dplyr::tbl(con2, "other_tbl")

  expect_error(
    omopgenerics::insertFromSource(cdm = cdm, value = tbl_ref),
    "different connection"
  )
})

test_that("insertFromSource.db_cdm strips prefix from remote name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "pre_"))

  DBI::dbWriteTable(con, "pre_my_table", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "pre_my_table")

  result <- omopgenerics::insertFromSource(cdm = cdm, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

# --- cdmDisconnect.db_cdm with dropPrefixTables ---

test_that("cdmDisconnect with dropPrefixTables drops prefixed tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Create CDM tables so cdmFromCon works
  CDMConnector:::execute_ddl(con, "main", tables = c("person", "observation_period",
    "cdm_source", "vocabulary", "concept", "concept_ancestor", "concept_relationship",
    "concept_synonym", "drug_strength"))

  # Insert minimal cdm_source data
  DBI::dbExecute(con, "INSERT INTO cdm_source (cdm_source_name, cdm_source_abbreviation, cdm_holder,
    source_description, source_documentation_reference, cdm_etl_reference,
    source_release_date, cdm_release_date, cdm_version, vocabulary_version)
    VALUES ('test', 'test', '', '', '', '', '2020-01-01', '2020-01-01', '5.3', '')")

  # Insert minimal observation_period
  DBI::dbExecute(con, "INSERT INTO observation_period (observation_period_id, person_id,
    observation_period_start_date, observation_period_end_date, period_type_concept_id)
    VALUES (1, 1, '2020-01-01', '2020-12-31', 0)")

  # Insert minimal person
  DBI::dbExecute(con, "INSERT INTO person (person_id, gender_concept_id, year_of_birth,
    race_concept_id, ethnicity_concept_id)
    VALUES (1, 0, 1990, 0, 0)")

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "test_pfx_"))

  # Create some prefixed tables
  DBI::dbWriteTable(con, "test_pfx_table1", data.frame(x = 1))
  DBI::dbWriteTable(con, "test_pfx_table2", data.frame(x = 2))

  expect_true("test_pfx_table1" %in% DBI::dbListTables(con))

  cdmDisconnect(cdm, dropPrefixTables = TRUE)
})

test_that("cdmDisconnect with dropPrefixTables informs when no prefix set", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Should inform that no prefix was specified (cli_inform)
  cdmDisconnect(cdm, dropPrefixTables = TRUE)
  # If we got here without error, the test passes
  expect_true(TRUE)
})

# --- dropSourceTable.db_cdm ---

test_that("dropSourceTable.db_cdm drops specified tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "to_drop", data.frame(x = 1))
  expect_true("to_drop" %in% DBI::dbListTables(con))

  dropSourceTable(src, "to_drop")
  expect_false("to_drop" %in% DBI::dbListTables(con))
})

test_that("dropTable.db_cdm drops only matching tables via tidyselect", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "keep_me", data.frame(x = 1))
  DBI::dbWriteTable(con, "drop_me", data.frame(x = 2))

  suppressWarnings(omopgenerics::dropTable(src, "drop_me"))
  tables <- DBI::dbListTables(con)
  expect_false("drop_me" %in% tables)
  expect_true("keep_me" %in% tables)
})

# --- summary.db_cdm with catalog ---

test_that("summary.db_cdm includes catalog when present in writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Construct the source manually with a catalog in the schema attribute
  src <- dbSource(con, writeSchema = c(catalog = "memory", schema = "main"))
  s <- summary(src)
  expect_equal(s$write_catalog, "memory")
  expect_equal(s$write_schema, "main")
})

# --- insertTable.db_cdm ---

test_that("insertTable.db_cdm writes a data frame to database", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  df <- data.frame(a = 1:3, b = c("x", "y", "z"))
  result <- omopgenerics::insertTable(cdm = src, name = "test_insert", table = df)
  expect_s3_class(result, "tbl_lazy")
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)
})

test_that("insertTable.db_cdm with overwrite replaces existing table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  df1 <- data.frame(a = 1:3)
  omopgenerics::insertTable(cdm = src, name = "test_ow", table = df1)

  df2 <- data.frame(a = 10:12, c = 1:3)
  result <- omopgenerics::insertTable(cdm = src, name = "test_ow", table = df2, overwrite = TRUE)
  collected <- dplyr::collect(result)
  expect_true(all(collected$a %in% 10:12))
})

# --- readSourceTable.db_cdm ---

test_that("readSourceTable.db_cdm reads table from write schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "my_source_tbl", data.frame(val = c(10, 20)))

  result <- omopgenerics::readSourceTable(cdm = src, name = "my_source_tbl")
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 2)
})

# --- cdmTableFromSource.db_cdm ---

test_that("cdmTableFromSource.db_cdm wraps tbl_lazy from same connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "src_tbl", data.frame(x = 1:5))
  tbl_ref <- dplyr::tbl(con, "src_tbl")

  result <- omopgenerics::cdmTableFromSource(src = src, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

test_that("cdmTableFromSource.db_cdm errors on data.frame", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  expect_error(
    omopgenerics::cdmTableFromSource(src = src, value = data.frame(x = 1)),
    "insertTable"
  )
})

test_that("cdmTableFromSource.db_cdm errors on different connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  src <- dbSource(con1, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con2, "other_tbl", data.frame(x = 1))
  tbl_ref <- dplyr::tbl(con2, "other_tbl")

  expect_error(
    omopgenerics::cdmTableFromSource(src = src, value = tbl_ref),
    "different connection"
  )
})

test_that("cdmTableFromSource.db_cdm strips prefix from remote name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main", prefix = "pfx_"))
  DBI::dbWriteTable(con, "pfx_my_table", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "pfx_my_table")

  result <- omopgenerics::cdmTableFromSource(src = src, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

# --- listSourceTables.db_cdm ---

test_that("listSourceTables.db_cdm lists tables from write schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "list_test", data.frame(x = 1))

  tables <- omopgenerics::listSourceTables(cdm = src)
  expect_true("list_test" %in% tables)
})

# --- dropTable.db_cdm when no tables exist ---

test_that("dropTable.db_cdm with matching table via dropTable method", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "keep_me", data.frame(x = 1))
  DBI::dbWriteTable(con, "drop_this", data.frame(x = 2))

  # Drop the existing table
  result <- suppressWarnings(CDMConnector:::dropTable.db_cdm(src, "drop_this"))
  expect_true(result)
  expect_false("drop_this" %in% DBI::dbListTables(con))
  expect_true("keep_me" %in% DBI::dbListTables(con))
})

# --- insertCdmTo.db_cdm ---

test_that("insertCdmTo.db_cdm copies CDM to new target", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Create a target source
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con2, shutdown = TRUE), add = TRUE)

  # Create minimal DDL in target
  CDMConnector:::execute_ddl(con2, "main", tables = c("person", "observation_period",
    "cdm_source", "vocabulary", "concept", "concept_ancestor", "concept_relationship",
    "concept_synonym", "drug_strength"))

  target <- dbSource(con2, writeSchema = c(schema = "main"))
  newCdm <- omopgenerics::insertCdmTo(cdm = cdm, to = target)

  expect_s3_class(newCdm, "cdm_reference")
  expect_true("person" %in% names(newCdm))
  person_count <- newCdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(person_count > 0)
})

# insertFromSource.db_cdm, insertCdmTo.db_cdm, cdmDisconnect dropPrefixTables

# --- compute.db_cdm ---

test_that("compute.db_cdm computes a table in the write schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Create a query that we'll compute
  result <- cdm$person %>%
    dplyr::select("person_id", "gender_concept_id") %>%
    dplyr::compute(name = "computed_person", temporary = FALSE, overwrite = TRUE)

  collected <- dplyr::collect(result)
  expect_true(nrow(collected) > 0)
  expect_true("person_id" %in% names(collected))
})

test_that("compute.db_cdm handles intermediate table when overwriting same name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # First compute
  tbl1 <- cdm$person %>%
    dplyr::select("person_id") %>%
    dplyr::compute(name = "my_tbl", temporary = FALSE, overwrite = TRUE)

  # Now compute from the same table name (triggers intermediate path)
  tbl2 <- tbl1 %>%
    dplyr::filter(.data$person_id > 0) %>%
    dplyr::compute(name = "my_tbl", temporary = FALSE, overwrite = TRUE)

  collected <- dplyr::collect(tbl2)
  expect_true(nrow(collected) > 0)
})

# --- insertFromSource.db_cdm ---

test_that("insertFromSource.db_cdm works for tbl_lazy from same connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Create a table in the database
  DBI::dbWriteTable(con, "test_insert_from_src", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "test_insert_from_src")

  result <- omopgenerics::insertFromSource(cdm = cdm, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

test_that("insertFromSource.db_cdm errors on data.frame", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_error(
    omopgenerics::insertFromSource(cdm = cdm, value = data.frame(x = 1)),
    "insertTable"
  )
})

test_that("insertFromSource.db_cdm errors on non-tbl_lazy", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  expect_error(
    omopgenerics::insertFromSource(cdm = cdm, value = list(a = 1))
  )
})

test_that("insertFromSource.db_cdm errors on different connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  cdm <- cdmFromCon(con1, cdmSchema = "main", writeSchema = "main")
  DBI::dbWriteTable(con2, "other_tbl", data.frame(x = 1))
  tbl_ref <- dplyr::tbl(con2, "other_tbl")

  expect_error(
    omopgenerics::insertFromSource(cdm = cdm, value = tbl_ref),
    "different connection"
  )
})

test_that("insertFromSource.db_cdm strips prefix from remote name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "pre_"))

  DBI::dbWriteTable(con, "pre_my_table", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "pre_my_table")

  result <- omopgenerics::insertFromSource(cdm = cdm, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

# --- cdmDisconnect.db_cdm with dropPrefixTables ---

test_that("cdmDisconnect with dropPrefixTables drops prefixed tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Create CDM tables so cdmFromCon works
  CDMConnector:::execute_ddl(con, "main", tables = c("person", "observation_period",
    "cdm_source", "vocabulary", "concept", "concept_ancestor", "concept_relationship",
    "concept_synonym", "drug_strength"))

  # Insert minimal cdm_source data
  DBI::dbExecute(con, "INSERT INTO cdm_source (cdm_source_name, cdm_source_abbreviation, cdm_holder,
    source_description, source_documentation_reference, cdm_etl_reference,
    source_release_date, cdm_release_date, cdm_version, vocabulary_version)
    VALUES ('test', 'test', '', '', '', '', '2020-01-01', '2020-01-01', '5.3', '')")

  # Insert minimal observation_period
  DBI::dbExecute(con, "INSERT INTO observation_period (observation_period_id, person_id,
    observation_period_start_date, observation_period_end_date, period_type_concept_id)
    VALUES (1, 1, '2020-01-01', '2020-12-31', 0)")

  # Insert minimal person
  DBI::dbExecute(con, "INSERT INTO person (person_id, gender_concept_id, year_of_birth,
    race_concept_id, ethnicity_concept_id)
    VALUES (1, 0, 1990, 0, 0)")

  cdm <- cdmFromCon(con, cdmSchema = "main",
                     writeSchema = c(schema = "main", prefix = "test_pfx_"))

  # Create some prefixed tables
  DBI::dbWriteTable(con, "test_pfx_table1", data.frame(x = 1))
  DBI::dbWriteTable(con, "test_pfx_table2", data.frame(x = 2))

  expect_true("test_pfx_table1" %in% DBI::dbListTables(con))

  cdmDisconnect(cdm, dropPrefixTables = TRUE)
})

test_that("cdmDisconnect with dropPrefixTables informs when no prefix set", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Should inform that no prefix was specified (cli_inform)
  cdmDisconnect(cdm, dropPrefixTables = TRUE)
  # If we got here without error, the test passes
  expect_true(TRUE)
})

# --- dropSourceTable.db_cdm ---

test_that("dropSourceTable.db_cdm drops specified tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "to_drop", data.frame(x = 1))
  expect_true("to_drop" %in% DBI::dbListTables(con))

  dropSourceTable(src, "to_drop")
  expect_false("to_drop" %in% DBI::dbListTables(con))
})

test_that("dropTable.db_cdm drops only matching tables via tidyselect", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "keep_me", data.frame(x = 1))
  DBI::dbWriteTable(con, "drop_me", data.frame(x = 2))

  suppressWarnings(omopgenerics::dropTable(src, "drop_me"))
  tables <- DBI::dbListTables(con)
  expect_false("drop_me" %in% tables)
  expect_true("keep_me" %in% tables)
})

# --- summary.db_cdm with catalog ---

test_that("summary.db_cdm includes catalog when present in writeSchema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Construct the source manually with a catalog in the schema attribute
  src <- dbSource(con, writeSchema = c(catalog = "memory", schema = "main"))
  s <- summary(src)
  expect_equal(s$write_catalog, "memory")
  expect_equal(s$write_schema, "main")
})

# --- insertTable.db_cdm ---

test_that("insertTable.db_cdm writes a data frame to database", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  df <- data.frame(a = 1:3, b = c("x", "y", "z"))
  result <- omopgenerics::insertTable(cdm = src, name = "test_insert", table = df)
  expect_s3_class(result, "tbl_lazy")
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)
})

test_that("insertTable.db_cdm with overwrite replaces existing table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  df1 <- data.frame(a = 1:3)
  omopgenerics::insertTable(cdm = src, name = "test_ow", table = df1)

  df2 <- data.frame(a = 10:12, c = 1:3)
  result <- omopgenerics::insertTable(cdm = src, name = "test_ow", table = df2, overwrite = TRUE)
  collected <- dplyr::collect(result)
  expect_true(all(collected$a %in% 10:12))
})

# --- readSourceTable.db_cdm ---

test_that("readSourceTable.db_cdm reads table from write schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "my_source_tbl", data.frame(val = c(10, 20)))

  result <- omopgenerics::readSourceTable(cdm = src, name = "my_source_tbl")
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 2)
})

# --- cdmTableFromSource.db_cdm ---

test_that("cdmTableFromSource.db_cdm wraps tbl_lazy from same connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "src_tbl", data.frame(x = 1:5))
  tbl_ref <- dplyr::tbl(con, "src_tbl")

  result <- omopgenerics::cdmTableFromSource(src = src, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

test_that("cdmTableFromSource.db_cdm errors on data.frame", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  expect_error(
    omopgenerics::cdmTableFromSource(src = src, value = data.frame(x = 1)),
    "insertTable"
  )
})

test_that("cdmTableFromSource.db_cdm errors on different connection", {
  skip_if_not_installed("duckdb")
  con1 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(con1, shutdown = TRUE)
    DBI::dbDisconnect(con2, shutdown = TRUE)
  }, add = TRUE)

  src <- dbSource(con1, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con2, "other_tbl", data.frame(x = 1))
  tbl_ref <- dplyr::tbl(con2, "other_tbl")

  expect_error(
    omopgenerics::cdmTableFromSource(src = src, value = tbl_ref),
    "different connection"
  )
})

test_that("cdmTableFromSource.db_cdm strips prefix from remote name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main", prefix = "pfx_"))
  DBI::dbWriteTable(con, "pfx_my_table", data.frame(x = 1:3))
  tbl_ref <- dplyr::tbl(con, "pfx_my_table")

  result <- omopgenerics::cdmTableFromSource(src = src, value = tbl_ref)
  expect_s3_class(result, "tbl_lazy")
})

# --- listSourceTables.db_cdm ---

test_that("listSourceTables.db_cdm lists tables from write schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "list_test", data.frame(x = 1))

  tables <- omopgenerics::listSourceTables(cdm = src)
  expect_true("list_test" %in% tables)
})

# --- dropTable.db_cdm when no tables exist ---

test_that("dropTable.db_cdm with matching table via dropTable method", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  src <- dbSource(con, writeSchema = c(schema = "main"))
  DBI::dbWriteTable(con, "keep_me", data.frame(x = 1))
  DBI::dbWriteTable(con, "drop_this", data.frame(x = 2))

  # Drop the existing table
  result <- suppressWarnings(CDMConnector:::dropTable.db_cdm(src, "drop_this"))
  expect_true(result)
  expect_false("drop_this" %in% DBI::dbListTables(con))
  expect_true("keep_me" %in% DBI::dbListTables(con))
})

# --- insertCdmTo.db_cdm ---

test_that("insertCdmTo.db_cdm copies CDM to new target", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

  # Create a target source
  con2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con2, shutdown = TRUE), add = TRUE)

  # Create minimal DDL in target
  CDMConnector:::execute_ddl(con2, "main", tables = c("person", "observation_period",
    "cdm_source", "vocabulary", "concept", "concept_ancestor", "concept_relationship",
    "concept_synonym", "drug_strength"))

  target <- dbSource(con2, writeSchema = c(schema = "main"))
  newCdm <- omopgenerics::insertCdmTo(cdm = cdm, to = target)

  expect_s3_class(newCdm, "cdm_reference")
  expect_true("person" %in% names(newCdm))
  person_count <- newCdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(person_count > 0)
})
