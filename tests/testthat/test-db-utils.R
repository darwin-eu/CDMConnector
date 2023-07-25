
# inSchema ----------

test_in_schema <- function(con, write_schema, cdm_schema) {
  # upload a table to the database
  if (is.null(con) || is.null(write_schema)) { return(invisible(NULL)) }

  if ("temp_test" %in% list_tables(con, schema = write_schema)) {
    DBI::dbRemoveTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)))
  }

  DBI::dbWriteTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)), cars, overwrite = TRUE)

  # <BigQueryConnection> uses an old dbplyr interface
  # i Please install a newer version of the package or contact the maintainer
  # This warning is displayed once every 8 hours.
  # Backtrace:
  #   1. test_in_schema(con, write_schema, get_cdm_schema("bigquery"))

  suppressWarnings({
    df <- dplyr::tbl(con, inSchema(write_schema, "temp_test", dbms = dbms(con))) %>%
      dplyr::mutate(a = 1) %>% # needed for duckdb for some reason??
      dplyr::collect() %>%
      dplyr::select("speed", "dist") %>%
      as.data.frame() %>%
      dplyr::arrange(.data$speed, .data$dist)
  })
  expect_equal(df, dplyr::arrange(cars, .data$speed, .data$dist))

  tables <- list_tables(con, cdm_schema) %>% tolower
  expect_true(all(c("person", "observation_period") %in% tables))

  DBI::dbRemoveTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)))
}

test_that("duckdb - inSchema", {
  con <- get_connection("duckdb")
  test_in_schema(con, "main", "main")
  disconnect(con)
})



# test_that("oracle - list_tables only", {
# only needed when the previous test was failing
#   con <- get_connection("oracle")
#   tables <- list_tables(con, get_cdm_schema("oracle")) %>% tolower
#   expect_true(all(c("person", "observation_period") %in% tables))
#   disconnect(con)
# })


test_that("getFullTableNameQuoted", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
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
  skip_if_not_installed("duckdb")
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


