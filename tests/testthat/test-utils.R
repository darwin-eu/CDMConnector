
# inSchema ----------

test_in_schema <- function(con, write_schema, cdm_schema) {
  # upload a table to the database
  if (is.null(con) || is.null(write_schema)) { return(invisible(NULL)) }

  if ("temp_test" %in% list_tables(con, schema = write_schema)) {
    DBI::dbRemoveTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)))
  }

  DBI::dbWriteTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)), cars)

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

test_that("postgres - inSchema", {
  con <- get_connection("postgres")
  test_in_schema(con, get_write_schema("postgres"), get_cdm_schema("postgres"))
  disconnect(con)
})

test_that("sqlserver - inSchema", {
  con <- get_connection("sqlserver")
  test_in_schema(con, get_write_schema("sqlserver"), get_cdm_schema("sqlserver"))
  disconnect(con)
})

test_that("redshift - inSchema", {
  con <- get_connection("redshift")
  test_in_schema(con, get_write_schema("redshift"), get_cdm_schema("redshift"))
  disconnect(con)
})

test_that("oracle - inSchema", {
  skip("failing test")
  con <- get_connection("oracle")
  write_schema <- get_write_schema("oracle")
  test_in_schema(con, write_schema, get_cdm_schema("oracle"))
  disconnect(con)
})

test_that("oracle - list_tables only", {
  con <- get_connection("oracle")
  tables <- list_tables(con, get_cdm_schema("oracle")) %>% tolower
  expect_true(all(c("person", "observation_period") %in% tables))
  disconnect(con)
})

test_that("bigquery - inSchema", {
  con <- get_connection("bigquery")
  write_schema <- get_write_schema("bigquery")
  test_in_schema(con, write_schema, get_cdm_schema("bigquery"))
  disconnect(con)
})

test_that("snowflake - inSchema", {
  con <- get_connection("snowflake")
  write_schema <- get_write_schema("snowflake")
  cdm_schema <- get_cdm_schema("snowflake")
  test_in_schema(con, write_schema, cdm_schema)
  disconnect(con)
})

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


#' List tables in a schema
#'
#' DBI::dbListTables can be used to get all tables in a database but not always in a
#' specific schema. `listTables` will list tables in a schema.
#'
#' @param con A DBI connection to a database
#' @param schema The name of a schema in a database. If NULL, returns DBI::dbListTables(con).
#'
#' @return A character vector of table names
#' @export
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' listTables(con, schema = "main")
#' }
list_tables <- function(con, schema = NULL) {
  checkmate::assert_character(schema, null.ok = TRUE, min.len = 1, max.len = 2, min.chars = 1)
  if (is.null(schema)) return(DBI::dbListTables(con))
  withr::local_options(list(arrow.pull_as_vector = TRUE))

  if (methods::is(con, "DatabaseConnectorJdbcConnection")) {
    out <- DBI::dbListTables(con, databaseSchema = paste0(schema, collapse = "."))
    return(out)
  }

  if (methods::is(con, "PqConnection") || methods::is(con, "RedshiftConnection")) {
    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema[[1]]};", .con = con)
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(.data$table_name)
    return(out)
  }

  if (methods::is(con, "duckdb_connection")) {
    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema[[1]]};", .con = con)
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(.data$table_name)
    return(out)
  }

  if (methods::is(con, "Snowflake")) {
    if (length(schema) == 2) {
      sql <- glue::glue("select table_name from {schema[1]}.information_schema.tables where table_schema = '{schema[2]}';")
    } else {
      sql <- glue::glue("select table_name from information_schema.tables where table_schema = '{schema[1]}';")
    }
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(1)
    return(out)
  }

  if (methods::is(con, "Spark SQL")) {
    # spark odbc connection
    sql <- paste("SHOW TABLES", if (!is.null(schema)) paste("IN", schema[[1]]))
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::filter(.data$isTemporary == FALSE) %>% dplyr::pull(.data$tableName)
    return(out)
  }

  if (methods::is(con, "OdbcConnection")) {
    if (length(schema) == 1) {
      out <- DBI::dbListTables(con, schema_name = schema)
    } else {
      out <- DBI::dbListTables(con, catalog_name = schema[[1]], schema_name = schema[[2]])
    }
    return(out)
  }

  if (methods::is(con, "OraConnection")) {
    checkmate::assert_character(schema, null.ok = TRUE, len = 1, min.chars = 1)
    out <- DBI::dbListTables(con, schema = schema)
    return(out)
  }

  if (methods::is(con, "BigQueryConnection")) {
    checkmate::assert_character(schema, null.ok = TRUE, len = 1, min.chars = 1)

    out <- DBI::dbGetQuery(con,
                           glue::glue("SELECT table_name
                         FROM `{schema}`.INFORMATION_SCHEMA.TABLES
                         WHERE table_schema = '{schema}'"))[[1]]
    return(out)
  }

  rlang::abort(paste(paste(class(con), collapse = ", "), "connection not supported"))
}

#' @rdname list_tables
#' @export
listTables <- list_tables

