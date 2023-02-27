test_that("getFullTableNameQuoted", {
  skip_if_not_installed("duckdb", "0.6")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, cdm_tables = c("person"))

  result <- getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = NULL)
  expect_equal(as.character(result), "myTable")

  result <- getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = c("mySchema"))
  expect_equal(result, "mySchema.myTable")

  result <- getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = c("mySchema", "dbo"))
  expect_equal(result, "mySchema.dbo.myTable")

  # invalid input
  expect_error(getFullTableNameQuoted(x = NULL, name = "myTable", schema = c("mySchema", "dbo")))
  expect_error(getFullTableNameQuoted(x = cdm$person, name = NULL, schema = c("mySchema", "dbo")))
  expect_error(getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = -1))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that(".dbIsValid", {
  skip_if_not_installed("duckdb", "0.6")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  result <- .dbIsValid(con)
  expect_true(result)

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("inSchema", {
  result <- inSchema(table = "myTable", writeSchema = c("main"))
  expect_equal(as.character(class(result)), "Id")
  tableSchemaNames <- as.character(attr(result, "name"))
  expect_equal(tableSchemaNames[1], "main")
  expect_equal(tableSchemaNames[2], "myTable")

  result <- inSchema(table = "myTable", writeSchema = c("main", "dbo"))
  expect_equal(as.character(class(result)), "Id")
  tableSchemaNames <- as.character(attr(result, "name"))
  expect_equal(tableSchemaNames[1], "main")
  expect_equal(tableSchemaNames[2], "dbo")
  expect_equal(tableSchemaNames[3], "myTable")
})

