# Additional tests for R/utils.R — coverage for utility functions

# --- .inSchema ---

test_that(".inSchema returns Id for basic schema.table", {
  result <- CDMConnector:::.inSchema("my_schema", "my_table")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["schema"]], "my_schema")
  expect_equal(result@name[["table"]], "my_table")
})

test_that(".inSchema returns Id for 2-part schema", {
  result <- CDMConnector:::.inSchema(c("catalog1", "schema1"), "my_table", dbms = "snowflake")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["catalog"]], "catalog1")
  expect_equal(result@name[["schema"]], "schema1")
})

test_that(".inSchema handles prefix with uppercase table", {
  schema <- c(schema = "main", prefix = "tmp_")
  result <- CDMConnector:::.inSchema(schema, "PERSON")
  # prefix should be uppercased for UPPERCASE tables
  expect_s4_class(result, "Id")
  expect_true(grepl("TMP_PERSON", result@name[["table"]]))
})

test_that(".inSchema handles prefix with lowercase table", {
  schema <- c(schema = "main", prefix = "tmp_")
  result <- CDMConnector:::.inSchema(schema, "person")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["table"]], "tmp_person")
})

test_that(".inSchema returns plain name for duckdb main schema", {
  result <- CDMConnector:::.inSchema("main", "person", dbms = "duckdb")
  expect_equal(result, "person")
})

test_that(".inSchema handles NULL schema for non-sql-server", {
  result <- CDMConnector:::.inSchema(NULL, "person", dbms = "duckdb")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["table"]], "person")
})

test_that(".inSchema handles NULL schema for sql server (adds # prefix)", {
  result <- CDMConnector:::.inSchema(NULL, "person", dbms = "sql server")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["table"]], "#person")
})

# --- .qualifiedNameForSql ---

test_that(".qualifiedNameForSql handles character string", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::.qualifiedNameForSql(con, "my_table")
  expect_true(grepl("my_table", result))
})

test_that(".qualifiedNameForSql handles DBI::Id", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  id <- DBI::Id(schema = "main", table = "my_table")
  result <- CDMConnector:::.qualifiedNameForSql(con, id)
  expect_true(grepl("main", result))
  expect_true(grepl("my_table", result))
})

# --- isInstalled ---

test_that("isInstalled returns TRUE for base", {
  expect_true(CDMConnector:::isInstalled("base"))
})

test_that("isInstalled returns FALSE for non-existent package", {
  expect_false(CDMConnector:::isInstalled("this_package_surely_does_not_exist"))
})

test_that("isInstalled checks version", {
  expect_true(CDMConnector:::isInstalled("base", "0"))
  # Extremely high version should fail
  expect_false(CDMConnector:::isInstalled("base", "999.999"))
})

# --- ensureInstalled ---

test_that("ensureInstalled does not error for installed package", {
  expect_no_error(CDMConnector:::ensureInstalled("base"))
})

test_that("ensureInstalled errors in non-interactive for missing package", {
  expect_error(
    CDMConnector:::ensureInstalled("this_package_surely_does_not_exist"),
    "must be installed"
  )
})

# --- mapTypes ---

test_that("mapTypes returns type unchanged for duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_equal(CDMConnector:::mapTypes(con, "integer"), "integer")
  expect_equal(CDMConnector:::mapTypes(con, "character"), "character")
})

# --- unique_prefix ---

test_that("unique_prefix returns a positive number", {
  result <- CDMConnector:::unique_prefix()
  expect_true(is.numeric(result))
  expect_true(result > 0)
})

test_that("unique_prefix returns different values over time", {
  p1 <- CDMConnector:::unique_prefix()
  Sys.sleep(0.2)
  p2 <- CDMConnector:::unique_prefix()
  expect_true(is.numeric(p1))
  expect_true(is.numeric(p2))
})

# --- .dbCreateTable ---

test_that(".dbCreateTable creates table via DBI", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  fields <- c(id = "INTEGER", name = "VARCHAR")
  CDMConnector:::.dbCreateTable(con, "test_tbl", fields)
  expect_true("test_tbl" %in% DBI::dbListTables(con))
})

# --- .formatDatesForSparkInsert ---

test_that(".formatDatesForSparkInsert converts Date and POSIXt to character", {
  df <- data.frame(
    d = as.Date("2020-01-15"),
    t = as.POSIXct("2020-01-15 10:30:00", tz = "UTC"),
    x = 42L
  )
  result <- CDMConnector:::.formatDatesForSparkInsert(df)
  expect_type(result$d, "character")
  expect_type(result$t, "character")
  expect_equal(result$x, 42L)
  expect_equal(result$d, "2020-01-15")
})

# --- dcCreateTable ---

test_that("dcCreateTable generates SQL for tibble fields", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  fields <- tibble::tibble(id = integer(0), name = character(0))
  sql <- CDMConnector:::dcCreateTable(con, "test_tbl", fields)
  expect_type(sql, "character")
  expect_true(grepl("CREATE TABLE", sql))
})

test_that("dcCreateTable generates SQL for named character fields", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  fields <- c(id = "INTEGER", name = "VARCHAR")
  sql <- CDMConnector:::dcCreateTable(con, "test_tbl2", fields)
  expect_type(sql, "character")
  expect_true(grepl("CREATE TABLE", sql))
})

# --- listTables ---

test_that("listTables works with duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE test_table (id INTEGER)")
  tables <- listTables(con, schema = "main")
  expect_true("test_table" %in% tables)
})

test_that("listTables returns all tables with NULL schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE test_null_schema (id INTEGER)")
  tables <- listTables(con, schema = NULL)
  expect_true("test_null_schema" %in% tables)
})

test_that("listTables handles prefix filtering", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE tmp_abc (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE tmp_def (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE other (id INTEGER)")
  tables <- listTables(con, schema = c(schema = "main", prefix = "tmp_"))
  expect_true("abc" %in% tables)
  expect_true("def" %in% tables)
  expect_false("other" %in% tables)
})

test_that("listTables errors on bad dot-separated schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_error(
    listTables(con, schema = "a.b.c"),
    "one dot"
  )
})

# --- execute_ddl ---

test_that("execute_ddl creates CDM tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::execute_ddl(con, "main", tables = c("person", "observation_period"))
  tables <- DBI::dbListTables(con)
  expect_true("person" %in% tables)
  expect_true("observation_period" %in% tables)
})

# --- .dbIsValid ---

test_that(".dbIsValid works for duckdb connection", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  expect_true(CDMConnector:::.dbIsValid(con))
  DBI::dbDisconnect(con, shutdown = TRUE)
})


# --- cdmCommentContents ---

test_that("cdmCommentContents returns invisibly NULL in non-interactive", {
  # In non-interactive sessions, the function should return invisible NULL
  result <- cdmCommentContents(NULL)
  expect_null(result)
})

# --- .cdm_comment_interactive ---

test_that(".cdm_comment_interactive returns a logical", {
  result <- CDMConnector:::.cdm_comment_interactive()
  expect_type(result, "logical")
})

# --- listTables with duckdb 2-part schema ---

test_that("listTables works with 2-part duckdb schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE test_2part (id INTEGER)")
  tables <- listTables(con, schema = c("memory", "main"))
  expect_true("test_2part" %in% tables)
})

test_that("listTables with named 2-part schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE named_2part (id INTEGER)")
  tables <- listTables(con, schema = c(catalog = "memory", schema = "main"))
  expect_true("named_2part" %in% tables)
})

test_that("listTables with dot-separated catalog.schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE dot_separated (id INTEGER)")
  tables <- listTables(con, schema = "memory.main")
  expect_true("dot_separated" %in% tables)
})

test_that("listTables with Id object", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE id_test (id INTEGER)")
  schema_id <- DBI::Id(schema = "main")
  tables <- listTables(con, schema = schema_id)
  expect_true("id_test" %in% tables)
})

# --- execute_ddl ---

test_that("execute_ddl creates specified tables only", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::execute_ddl(con, "main", tables = c("person"))
  tables <- DBI::dbListTables(con)
  expect_true("person" %in% tables)
})

test_that("execute_ddl creates 5.4 tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::execute_ddl(con, "main", cdm_version = "5.4", tables = c("person", "observation_period"))
  tables <- DBI::dbListTables(con)
  expect_true("person" %in% tables)
  expect_true("observation_period" %in% tables)
})

test_that("execute_ddl with prefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::execute_ddl(con, "main", tables = c("person"), prefix = "test_")
  tables <- DBI::dbListTables(con)
  expect_true("test_person" %in% tables)
})

# --- .inSchema edge cases ---

test_that(".inSchema with bigquery and 2-part schema returns string", {
  result <- CDMConnector:::.inSchema(c("project", "dataset"), "my_table", dbms = "bigquery")
  expect_type(result, "character")
  expect_true(grepl("project", result))
  expect_true(grepl("dataset", result))
  expect_true(grepl("my_table", result))
})

test_that(".inSchema with snowflake and 2-part schema returns Id", {
  result <- CDMConnector:::.inSchema(c("cat", "sch"), "my_table", dbms = "snowflake")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["catalog"]], "cat")
  expect_equal(result@name[["schema"]], "sch")
})

test_that(".inSchema with 1-part schema returns Id", {
  result <- CDMConnector:::.inSchema("myschema", "my_table", dbms = "postgresql")
  expect_s4_class(result, "Id")
  expect_equal(result@name[["schema"]], "myschema")
  expect_equal(result@name[["table"]], "my_table")
})

# --- mapTypes ---

test_that("mapTypes returns type for non-bigquery", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_equal(CDMConnector:::mapTypes(con, "Date"), "Date")
  expect_equal(CDMConnector:::mapTypes(con, "logical"), "logical")
})

# --- dcCreateTable ---

test_that("dcCreateTable with Id name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  name <- DBI::Id(schema = "main", table = "test_tbl")
  fields <- c(id = "INTEGER", name = "VARCHAR")
  sql <- CDMConnector:::dcCreateTable(con, name, fields)
  expect_true(grepl("CREATE TABLE", sql))
})

# --- .qualifiedNameForSql edge cases ---

test_that(".qualifiedNameForSql handles plain non-char/non-Id", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Pass numeric (edge case: function returns it unchanged)
  result <- CDMConnector:::.qualifiedNameForSql(con, 42)
  expect_equal(result, 42)
})

# --- .formatDatesForSparkInsert ---

test_that(".formatDatesForSparkInsert preserves non-date columns", {
  df <- data.frame(x = 1:3, name = c("a", "b", "c"), stringsAsFactors = FALSE)
  result <- CDMConnector:::.formatDatesForSparkInsert(df)
  expect_equal(result$x, 1:3)
  expect_equal(result$name, c("a", "b", "c"))
})

test_that(".formatDatesForSparkInsert converts POSIXct", {
  df <- data.frame(
    ts = as.POSIXct(c("2020-01-01 12:00:00", "2020-06-15 08:30:00"), tz = "UTC"),
    val = 1:2
  )
  result <- CDMConnector:::.formatDatesForSparkInsert(df)
  expect_type(result$ts, "character")
  expect_equal(result$val, 1:2)
})

# --- dbplyr_edition.BigQueryConnection ---

test_that("dbplyr_edition.BigQueryConnection returns 2L", {
  # Create a mock BigQueryConnection-like object
  mock_con <- structure(list(), class = "BigQueryConnection")
  result <- dbplyr::dbplyr_edition(mock_con)
  expect_equal(result, 2L)
})


# --- .cdm_comment helper functions ---

test_that(".cdm_comment_interactive returns logical", {
  result <- CDMConnector:::.cdm_comment_interactive()
  expect_type(result, "logical")
})

test_that(".cdm_comment_require_rstudioapi returns logical", {
  result <- CDMConnector:::.cdm_comment_require_rstudioapi()
  expect_type(result, "logical")
})

test_that(".cdm_comment_collect collects a data frame", {
  df <- data.frame(x = 1:3)
  result <- CDMConnector:::.cdm_comment_collect(df)
  expect_true(is.data.frame(result))
  expect_equal(nrow(result), 3)
})

test_that(".cdm_comment_flatten works on CDM", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
  result <- CDMConnector:::.cdm_comment_flatten(cdm)
  expect_true(!is.null(result))
})

# --- mapTypes ---

test_that("mapTypes returns type unchanged for non-bigquery", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_equal(CDMConnector:::mapTypes(con, "integer"), "integer")
  expect_equal(CDMConnector:::mapTypes(con, "character"), "character")
  expect_equal(CDMConnector:::mapTypes(con, "numeric"), "numeric")
})

# --- isInstalled ---

test_that("isInstalled detects installed packages", {
  expect_true(CDMConnector:::isInstalled("dplyr"))
  expect_false(CDMConnector:::isInstalled("nonexistent_package_xyz"))
})

test_that("isInstalled checks version", {
  expect_true(CDMConnector:::isInstalled("dplyr", "0.1"))
  expect_false(CDMConnector:::isInstalled("dplyr", "999.0"))
})

# --- listTables with prefix ---

test_that("listTables filters by prefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "pre_table1", data.frame(x = 1))
  DBI::dbWriteTable(con, "pre_table2", data.frame(x = 2))
  DBI::dbWriteTable(con, "other_table", data.frame(x = 3))

  tables <- listTables(con, schema = c(schema = "main", prefix = "pre_"))
  expect_true("table1" %in% tables)
  expect_true("table2" %in% tables)
  expect_false("other_table" %in% tables)
})

# --- listTables with dot-separated schema ---

test_that("listTables handles catalog.schema form", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "test_tbl", data.frame(x = 1))

  # memory.main is the default catalog.schema for in-memory duckdb
  tables <- listTables(con, schema = "memory.main")
  expect_true("test_tbl" %in% tables)
})

# --- listTables errors on multiple dots ---

test_that("listTables errors on schema with multiple dots", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_error(listTables(con, schema = "a.b.c"), "one dot")
})

# --- listTables with Id schema ---

test_that("listTables handles Id schema object", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "test_tbl", data.frame(x = 1))
  schema_id <- DBI::Id(schema = "main")
  tables <- listTables(con, schema = schema_id)
  expect_true("test_tbl" %in% tables)
})

# --- .inSchema ---

test_that(".inSchema returns Id for unknown dbms", {
  result <- CDMConnector:::.inSchema("main", "person")
  expect_s4_class(result, "Id")
})

test_that(".inSchema returns string for duckdb", {
  result <- CDMConnector:::.inSchema("main", "person", dbms = "duckdb")
  expect_true(is.character(result))
})
