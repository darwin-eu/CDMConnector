test_that("getFullTableNameQuoted", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  # debugonce(cdm_from_con)
  cdm <- cdm_from_con(con, "main")

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
  result <- inSchema(schema = "main", table = "myTable")
  expect_equal(as.character(class(result)), "Id")
  tableSchemaNames <- as.character(attr(result, "name"))
  expect_equal(tableSchemaNames[1], "main")
  expect_equal(tableSchemaNames[2], "myTable")

  result <- inSchema(schema = c("main", "dbo"), table = "myTable")
  expect_equal(as.character(class(result)), "Id")
  tableSchemaNames <- as.character(attr(result, "name"))
  expect_equal(tableSchemaNames[1], "main")
  expect_equal(tableSchemaNames[2], "dbo")
  expect_equal(tableSchemaNames[3], "myTable")
})

test_that("inSchema works with duckdb", {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, "create schema scratch")
  DBI::dbWriteTable(con, DBI::Id(schema = "scratch", table = "cars"), cars)
  expect_equal(nrow(DBI::dbGetQuery(con, "select * from scratch.cars limit 5")), 5)
  df <- dplyr::tbl(con, inSchema("scratch", "cars")) %>% dplyr::collect()
  expect_equal(dplyr::tibble(cars), df)
  DBI::dbDisconnect(con, shutdown = T)
})

test_that("normalize_schema works", {
  library(zeallot)
  c(schema, prefix) %<-% normalize_schema("asdf")
  expect_true(schema == "asdf" & is.null(prefix))
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(c(schema = "asdf"))
  expect_true(schema == "asdf" & is.null(prefix))
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(DBI::Id(schema = "asdf"))
  expect_true(schema == "asdf" & is.null(prefix))
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(c(schema = "asdf", prefix = "prefix"))
  expect_true(schema == "asdf" & prefix == "prefix")
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(DBI::Id(schema = "asdf", prefix = "prefix"))
  expect_true(schema == "asdf" & prefix == "prefix")
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema("asdf.dbo")
  expect_true(all(schema == c("asdf", "dbo")) & is.null(prefix))
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(c(catalog = "asdf", schema = "dbo"))
  expect_true(all(schema == c("asdf", "dbo")) & is.null(prefix))
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(DBI::Id(catalog = "asdf", schema = "dbo"))
  expect_true(all(schema == c("asdf", "dbo")) & is.null(prefix))
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(c(catalog = "asdf", schema = "dbo", prefix = "prefix"))
  expect_true(all(schema == c("asdf", "dbo")) & prefix == "prefix")
  expect_true(is.null(names(schema)) & is.null(names(prefix)))

  c(schema, prefix) %<-% normalize_schema(DBI::Id(catalog = "asdf", schema = "dbo", prefix = "prefix"))
  expect_true(all(schema == c("asdf", "dbo")) & prefix == "prefix")
  expect_true(is.null(names(schema)) & is.null(names(prefix)))
})




