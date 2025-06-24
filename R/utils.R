# Copyright 2025 DARWIN EU®
#
# This file is part of CDMConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Pipe operator
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom dplyr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL

# Workaround for Oracle since ROracle does not define dbIsValid
.dbIsValid <- function(dbObj, ...) {
  if (methods::is(dbObj, "OraConnection")) {
    is.character(DBI::dbListTables(dbObj))
  } else {
    DBI::dbIsValid(dbObj, ...)
  }
}

#' Helper for working with compound schemas
#'
#' This is similar to dbplyr::in_schema but has been tested across multiple
#' database platforms. It only exists to work around some of the limitations
#' of dbplyr::in_schema.
#'
#' @param schema A schema name as a character string
#' @param table A table name as character string
#' @param dbms The name of the database management system as returned
#' by `dbms(connection)`
#'
#' @return A DBI::Id that represents a qualified table and schema
#' @export
inSchema <- function(schema, table, dbms = NULL) {
  # TODO deprecate this function after removing it from all tests
  # lifecycle::deprecate_soft("1.4.1", "CDMConnector::inSchema()", "dbplyr::in_schema()")
  .inSchema(schema, table, dbms)
}

# internal function
.inSchema <- function(schema, table, dbms = NULL) {
  # lifecycle::deprecate_soft("1.4.1", "CDMConnector::inSchema()", "dbplyr::in_schema()")
  checkmate::assertCharacter(schema, min.len = 1, max.len = 3, null.ok = TRUE)
  checkmate::assertCharacter(table, len = 1, min.chars = 1)
  checkmate::assertCharacter(dbms, len = 1, null.ok = TRUE)

  if (is.null(schema)) {
    # return temp table name
    if (dbms == "sql server") {
      return(DBI::Id(table = paste0("#", table)))
    }
    return(DBI::Id(table = table))
  }

  if ("prefix" %in% names(schema)) {
    checkmate::assertCharacter(schema['prefix'], len = 1, min.chars = 1, pattern = "[a-zA-Z1-9_]+")

    # match the case of table name
    if (toupper(table) == table) {
      table <- paste0(toupper(schema['prefix']), table)
    } else {
      table <- paste0(schema['prefix'], table)
    }

    schema <- schema[!names(schema) %in% "prefix"]
    checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  }

  if (isFALSE(dbms %in% c("snowflake", "sql server", "spark"))) {
    # only a few dbms support three part names
    checkmate::assertCharacter(schema, len = 1)
  }

  schema <- unname(schema)

  if (!is.null(dbms) && dbms == "duckdb" && identical(schema, "main")) {
    out <- table
  } else {
    out <- switch(length(schema),
      DBI::Id(schema = schema, table = table),
      DBI::Id(catalog = schema[1], schema = schema[2], table = table))
  }
  return(out)
}

#' List tables in a schema
#'
#' DBI::dbListTables can be used to get all tables in a database but not always in a
#' specific schema. `listTables` will list tables in a schema.
#'
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
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#' listTables(con, schema = "main")
#' }
listTables <- function(con, schema = NULL) {

  if (methods::is(con, "Pool")) {
    if (!rlang::is_installed("pool")) {
      rlang::abort("Please install the pool package.")
    }
    con <- pool::localCheckout(con)
  }

  checkmate::assertTRUE(DBI::dbIsValid(con))

  if (methods::is(schema, "Id")) {
    schema <- schema@name
  }

  if ("prefix" %in% names(schema)) {
    prefix <- schema["prefix"]
    checkmate::assert_character(prefix, min.chars = 1, len = 1)
    schema <- schema[names(schema) != "prefix"]

    process_prefix <- function(x) {
      np <- nchar(prefix)
      x <- x[stringr::str_starts(string = x, pattern = prefix) & nchar(x) > np]
      substr(x, start = np+1, stop = nchar(x))
    }
  } else {
    process_prefix <- function(x) {x}
  }

  checkmate::assert_character(schema, null.ok = TRUE, min.len = 1, max.len = 2, min.chars = 1)

  if (is.null(schema)) {
    if (dbms(con) == "sql server") {
      # return temp tables
      # tempdb.sys.objects
      temp_tables <- DBI::dbGetQuery(con, "select * from tempdb..sysobjects")[[1]] %>%
        stringr::str_remove("_________________________.*$") %>%
        stringr::str_remove("^#+")

      return(temp_tables)

    } else if (dbms(con) == "snowflake") {
      # return all tables including temp tables
      return(DBI::dbGetQuery(con, "show terse tables;")$name)

    } else {
      return(DBI::dbListTables(con))
    }
  }

  withr::local_options(list(arrow.pull_as_vector = TRUE))

  if (methods::is(con, "DatabaseConnectorJdbcConnection")) {
    out <- DBI::dbListTables(con, databaseSchema = paste0(schema, collapse = "."))
    return(process_prefix(out))
  }

  if (methods::is(con, "PqConnection") || methods::is(con, "RedshiftConnection")) {

    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {unname(schema[1])};", .con = con)
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(.data$table_name)
    return(process_prefix(out))
  }

  if (methods::is(con, "duckdb_connection")) {
    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema[[1]]};", .con = con)
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(.data$table_name)
    return(process_prefix(out))
  }

  if (methods::is(con, "Snowflake")) {
    if (length(schema) == 2) {
      sql <- glue::glue("select table_name from {schema[1]}.information_schema.tables where table_schema = '{schema[2]}';")
    } else {
      sql <- glue::glue("select table_name from information_schema.tables where table_schema = '{schema[1]}';")
    }
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(1)
    return(process_prefix(out))
  }

  if (methods::is(con, "Spark SQL")) {
    # spark odbc connection
    sql <- paste("SHOW TABLES", if (!is.null(schema)) paste("IN", paste(schema, collapse = ".")))
    out <- DBI::dbGetQuery(con, sql) %>%
      dplyr::filter(.data$isTemporary == FALSE) %>%
      dplyr::pull(.data$tableName)

    return(process_prefix(out))
  }

  if (methods::is(con, "OdbcConnection")) {
    if (length(schema) == 1) {
      out <- DBI::dbListTables(con, schema_name = schema)
    } else if (length(schema) == 2) {
      out <- DBI::dbListTables(con, catalog_name = schema[[1]], schema_name = schema[[2]])
    } else rlang::abort("schema missing!")

    return(process_prefix(out))
  }

  if (methods::is(con, "OraConnection")) {
    checkmate::assert_character(schema, null.ok = TRUE, len = 1, min.chars = 1)
    out <- DBI::dbListTables(con, schema = schema)
    return(process_prefix(out))
  }

  if (methods::is(con, "BigQueryConnection")) {
    checkmate::assert_character(schema, null.ok = TRUE, len = 1, min.chars = 1)

    out <- DBI::dbGetQuery(con,
                           glue::glue("SELECT table_name
                         FROM `{schema}`.INFORMATION_SCHEMA.TABLES
                         WHERE table_schema = '{schema}'"))[[1]]
    return(process_prefix(out))
  }

  if (methods::is(con, "DatabaseConnectorDbiConnection")) {
    return(listTables(con@dbiConnection, schema = schema))
  }

  rlang::abort(paste(paste(class(con), collapse = ", "), "connection not supported"))
}

# To silence warning <BigQueryConnection> uses an old dbplyr interface
# https://github.com/r-dbi/bigrquery/issues/508

#' @importFrom dbplyr dbplyr_edition
#' @method dbplyr_edition BigQueryConnection
#' @export
dbplyr_edition.BigQueryConnection <- function(con) 2L

# Create the cdm tables in a database
execute_ddl <- function(con, cdm_schema, cdm_version = "5.3", dbms = "duckdb", tables = tblGroup("all"), prefix = "") {

  specs <- spec_cdm_field[[cdm_version]] %>%
    dplyr::mutate(cdmDatatype = dplyr::if_else(.data$cdmDatatype == "varchar(max)", "varchar(2000)", .data$cdmDatatype)) %>%
    dplyr::mutate(cdmFieldName = dplyr::if_else(.data$cdmFieldName == '"offset"', "offset", .data$cdmFieldName)) %>%
    dplyr::mutate(cdmDatatype = dplyr::case_when(
      dbms(con) == "postgresql" & .data$cdmDatatype == "datetime" ~ "timestamp",
      dbms(con) == "redshift" & .data$cdmDatatype == "datetime" ~ "timestamp",
      TRUE ~ cdmDatatype)) %>%
    tidyr::nest(col = -"cdmTableName") %>%
    dplyr::mutate(col = purrr::map(col, ~setNames(as.character(.$cdmDatatype), .$cdmFieldName)))

  for (i in cli::cli_progress_along(tables)) {
    fields <- specs %>%
      dplyr::filter(.data$cdmTableName == tables[i]) %>%
      dplyr::pull(.data$col) %>%
      unlist()

    DBI::dbCreateTable(con, .inSchema(cdm_schema, paste0(prefix, tables[i]), dbms = dbms(con)), fields = fields)
  }
}

# get a unique prefix based on current time. internal function.
unique_prefix <- function() {
  as.integer((as.numeric(Sys.time())*10) %% 1e6)
}


# Borrowed from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
isInstalled <- function(pkg, version = "0") {
  installedVersion <- tryCatch(utils::packageVersion(pkg),
                                error = function(e) NA
  )
  !is.na(installedVersion) && installedVersion >= version
}

# Borrowed and adapted from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensureInstalled <- function(pkg) {
  if (!isInstalled(pkg)) {
    msg <- paste0(sQuote(pkg), " must be installed for this functionality.")
    if (interactive()) {
      rlang::inform(paste(msg, "Would you like to install it?", sep = "\n"))
      if (utils::menu(c("Yes", "No")) == 1) {
        utils::install.packages(pkg)
      } else {
        stop(msg, call. = FALSE)
      }
    } else {
      stop(msg, call. = FALSE)
    }
  }
}

mapTypes <- function(conn, type) {
  # mapping types only used for some cases with bigquery (DBI) - e.g. generateCohortSet tests
  if(!(dbms(conn) %in% c("bigquery"))){
    return(type)
  }

  if (type %in% c("integer", "integer64")) {
    return("INT")
  } else if (type == "character") {
    return("STRING")
  }

  return(type)
}

# create table function adjusted to work with DatabaseConnector and bigquery
dcCreateTable <- function(conn, name, fields) {

  if (tibble::is_tibble(fields)) {
   fieldsSql <- paste(names(fields),
     sapply(fields, function(x) mapTypes(conn, class(x)[1])),
     collapse = ", "
   )
 } else {
   fields <- sapply(names(fields), function(field) {
     paste(field, fields[[field]], sep = " ")
   })
   fieldsSql <- paste(fields, collapse = ", ")
 }

  tableName <- paste(name@name, collapse = ".")

  if (!(dbms(conn) %in% c("bigquery"))){
    createTableSQL <- SqlRender::render("CREATE TABLE @a ( @b );", a = tableName, b = fieldsSql)

    createTableSQLTranslated <- SqlRender::translate(createTableSQL, dbms(conn))
  } else {
    createTableSQLTranslated <- glue::glue("CREATE TABLE `{tableName}` ({fieldsSql});")
  }

  return(createTableSQLTranslated)
}

# branching logic: which createTable function to use based on the connection type
.dbCreateTable <- function(conn, name, fields) {
  if (methods::is(conn, "DatabaseConnectorJdbcConnection") || dbms(conn)  %in% c("bigquery")) {

    createTableSQLTranslated <- dcCreateTable(conn, name, fields)
    DBI::dbExecute(conn, createTableSQLTranslated)

  } else {
  DBI::dbCreateTable(conn, name, fields)
  }
}

# build and execute the SQL query to insert data into the table
# .dbInsertData <- function(conn, name, table) {
#   columns <- colnames(table)
#   values <- apply(table, 1, function(row) paste0("(", paste(shQuote(row), collapse = ", "), ")"))
#
#   tableName <- paste(name@name, collapse = ".")
#
#   query <- paste(
#     "INSERT INTO", tableName, "(", paste(columns, collapse = ", "), ")",
#     "VALUES", paste(values, collapse = ", ")
#   )
#
#   queryTranslated <- SqlRender::translate(query, dbms(conn))
#
#   tryCatch({
#     DBI::dbExecute(conn, queryTranslated)
#     cat("Data inserted successfully.\n")
#   }, error = function(e) {
#     cat("Error inserting data:", e$message, "\n")
#   })
# }

