# Copyright 2024 DARWIN EU®
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

# Run a dplyr query and store the result in a permanent table
#
# @param x A dplyr query
# @param name Name of the table to be created
# @param schema Schema to create the new table in
# Can be a length 1 or 2 vector.
# (e.g. schema = "my_schema", schema = c("my_schema", "dbo"))
# @param overwrite If the table already exists in the remote database
# should it be overwritten? (TRUE or FALSE)
#
# @return A dplyr reference to the newly created table
#
# internal function
.computePermanent <- function(x, name, schema = NULL, overwrite = TRUE) {
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, null.ok = TRUE)
  schema <- unname(schema)
  checkmate::assertCharacter(name, len = 1)
  checkmate::assertClass(x, "tbl_sql")
  checkmate::assertLogical(overwrite, len = 1)

  fullNameQuoted <- getFullTableNameQuoted(x, name, schema)
  existingTables <- listTables(x$src$con, schema = schema)
  if (name %in% existingTables) {
    if (overwrite) {
      # DBI::dbRemoveTable(x$src$con, DBI::SQL(fullNameQuoted))
      DBI::dbRemoveTable(x$src$con, inSchema(schema, name, dbms = dbms(x$src$con)))
    } else {
      rlang::abort(paste(fullNameQuoted, "already exists.",
                         "Set overwrite = TRUE to recreate it."))
    }
  }

  if (dbms(x$src$con) %in% c("duckdb", "oracle", "snowflake", "bigquery")) {

    if (length(schema) == 2) {
      sql <- dbplyr::build_sql("CREATE TABLE ",
                               dbplyr::ident(schema[1]), dbplyr::sql("."),
                               dbplyr::ident(schema[2]), dbplyr::sql("."), dbplyr::ident(name),
                               " AS ", dbplyr::sql_render(x), con = x$src$con)
    } else {
      sql <- dbplyr::build_sql("CREATE TABLE ",
               if (!is.null(schema)) dbplyr::ident(schema),
               if (!is.null(schema)) dbplyr::sql("."), dbplyr::ident(name),
               " AS ", dbplyr::sql_render(x), con = x$src$con)
    }

  } else if (dbms(x$src$con) == "spark") {
    sql <- dbplyr::build_sql("CREATE ",
             if (overwrite) dbplyr::sql("OR REPLACE "),  "TABLE ",
             if (!is.null(schema)) dbplyr::ident(schema),
             if (!is.null(schema)) dbplyr::sql("."), dbplyr::ident(name),
             " AS ", dbplyr::sql_render(x), con = x$src$con)
  } else {
    sql <- glue::glue("SELECT * INTO {fullNameQuoted}
                      FROM ({dbplyr::sql_render(x)}) x")
  }

  DBI::dbExecute(x$src$con, sql)

  dplyr::tbl(x$src$con, inSchema(schema = schema, table = name, dbms = dbms(x$src$con)))
}

#' Run a dplyr query and add the result set to an existing
#'
#' @param x A dplyr query
#' @param name Name of the table to be appended. If it does not already exist it
#'   will be created.
#' @param schema Schema where the table exists. Can be a length 1 or 2 vector.
#'   (e.g. schema = "my_schema", schema = c("my_schema", "dbo"))
#'
#' @return A dplyr reference to the newly created table
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' concept <- dplyr::tbl(con, "concept")
#'
#' # create a table
#' rxnorm_count <- concept %>%
#'   dplyr::filter(domain_id == "Drug") %>%
#'   dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#'   dplyr::count(domain_id, isRxnorm) %>%
#'   compute("rxnorm_count")
#'
#' # append to an existing table
#' rxnorm_count <- concept %>%
#'   dplyr::filter(domain_id == "Procedure") %>%
#'   dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#'   dplyr::count(domain_id, isRxnorm) %>%
#'   appendPermanent("rxnorm_count")
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#'
#' }
appendPermanent <- function(x, name, schema = NULL) {
  checkmate::assertCharacter(schema, min.len = 1, max.len = 3, null.ok = TRUE)
  checkmate::assertCharacter(name, len = 1)
  checkmate::assertClass(x, "tbl_sql")

  # TODO try dbAppendTable
  if ("prefix" %in% names(schema)) {
    name <- paste0(schema["prefix"], name)
    schema <- schema[names(schema) != "prefix"]
  }
  fullNameQuoted <- getFullTableNameQuoted(x, name, schema)
  existingTables <- listTables(x$src$con, schema = schema)
  if (!(tolower(name) %in% tolower(existingTables))) {
    return(.computePermanent(x = x,
                             name = name,
                             schema = schema,
                             overwrite = FALSE))
  }

  if (dbms(x$src$con) == "bigquery") {
    insertStatment <- "insert into"
  } else {
    insertStatment <- "INSERT INTO"
  }
  sql <- glue::glue("{insertStatment} {fullNameQuoted} {dbplyr::sql_render(x)}")
  DBI::dbExecute(x$src$con, sql)
  dplyr::tbl(x$src$con, inSchema(schema, name, dbms = dbms(x$src$con)))
}


#' @rdname appendPermanent
#' @export
append_permanent <- appendPermanent

#' Create a unique table name for temp tables
#'
#' @return A string that can be used as a dbplyr temp table name
#' @export
uniqueTableName <- function() {
  i <- getOption("dbplyr_table_name", 0) + 1
  options(dbplyr_table_name = i)
  sprintf("dbplyr_%03i", i)
}

#' @rdname uniqueTableName
#' @export
unique_table_name <- uniqueTableName

#' Execute dplyr query and save result in remote database
#'
#' This function is a wrapper around `dplyr::compute` that is tested on several
#' database systems. It is needed to handle edge cases where `dplyr::compute`
#' does not produce correct SQL.
#'
#' @param x A dplyr query
#' @param name The name of the table to create.
#' @param temporary Should the table be temporary: TRUE (default) or FALSE
#' @param schema The schema where the table should be created. Ignored if
#'   temporary = TRUE.
#' @param overwrite Should the table be overwritten if it already exists: TRUE (default)
#'   or FALSE Ignored if temporary = TRUE.
#' @param ... Further arguments passed on the `dplyr::compute`
#'
#' @return A `dplyr::tbl()` reference to the newly created table.
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#'
#' # create a temporary table in the remote database from a dplyr query
#' drugCount <- cdm$concept %>%
#'   dplyr::count(domain_id == "Drug") %>%
#'   computeQuery()
#'
#' # create a permanent table in the remote database from a dplyr query
#' drugCount <- cdm$concept %>%
#'   dplyr::count(domain_id == "Drug") %>%
#'   computeQuery("tmp_table", temporary = FALSE, schema = "main")
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
computeQuery <- function(x,
                         name = uniqueTableName(),
                         temporary = TRUE,
                         schema = NULL,
                         overwrite = TRUE,
                         ...) {

  lifecycle::deprecate_warn("1.3", "CDMConnector::computeQuery()", with = "dplyr::compute()")

  .computeQuery(x,
                name = name,
                temporary = temporary,
                schema = schema,
                overwrite = overwrite,
                ...)
}

.computeQuery <- function(x,
                         name = uniqueTableName(),
                         temporary = TRUE,
                         schema = NULL,
                         overwrite = TRUE,
                         ...) {

  if (is.data.frame(x) || (methods::is(x, "Table") && methods::is(x, "ArrowTabular"))) {
    return(x)
  }

  if ("cdm_reference" %in% class(x)) {
    rlang::abort("You passed a cdm object into computeQuery which only accepts single tables or lazy queries!")
  }

  checkmate::assertLogical(temporary, len = 1)
  checkmate::assertLogical(overwrite, len = 1)

  if (nchar(dbplyr::sql_render(x)) > 10000) {
    rlang::warn("Your SQL query is over 10,000 characters which can cause issues on some database platforms!\nTry calling computeQuery earlier in your pipeline.")
  }

  if (isFALSE(temporary)) {
    checkmate::assertCharacter(schema, min.len = 1, max.len = 3)

    # handle prefixes
    if ("prefix" %in% names(schema)) {
      checkmate::assertCharacter(schema["prefix"], len = 1, min.chars = 1, pattern = "[a-zA-Z1-9_]+")
      name <- paste0(schema["prefix"], name)
      schema <- schema[names(schema) != "prefix"]
    }
    checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  }

  cdm_reference <- attr(x, "cdm_reference") # might be NULL
  con <- x$src$con

  if (temporary) {

    # handle overwrite for temp tables
    # TODO test overwrite of temp tables this across all dbms
    if (name %in% list_tables(con)) {
      if (isFALSE(overwrite)) {
        rlang::abort(glue::glue("table {name} already exists and overwrite is FALSE!"))
      }

      if (dbms(con) %in% c("sql server")) {
        DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS #{name};"))
      } else {
        DBI::dbRemoveTable(con, name)
      }
    }

    if (methods::is(con, "OraConnection") || methods::is(con, "Oracle")) {
      # https://github.com/tidyverse/dbplyr/issues/621#issuecomment-1362229669
      name <- paste0("ORA$PTT_", name)
      sql <- dbplyr::build_sql(
        "CREATE PRIVATE TEMPORARY TABLE \n",
        dbplyr::ident(name),
        dbplyr::sql(" ON COMMIT PRESERVE DEFINITION \n"),
        " AS\n",
        dbplyr::sql_render(x),
        con = con
      )
      DBI::dbExecute(con, sql)
      out <- dplyr::tbl(con, name)
    } else if (methods::is(con, "Spark SQL")) {
      sql <- dbplyr::build_sql(
        "CREATE ", if (overwrite) dbplyr::sql("OR REPLACE "),
        "TEMPORARY VIEW \n",
        dbplyr::ident(name), " AS\n",
        dbplyr::sql_render(x),
        con = con
      )
      DBI::dbExecute(con, sql)
      out <- dplyr::tbl(con, name)
    } else if (dbms(con) == "bigquery" && methods::is(con, "BigQueryConnection")) {
      sql <- dbplyr::build_sql(
        "CREATE TABLE ", dbplyr::ident(name), " \n",
        "OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS\n",
        dbplyr::sql_render(x),
        con = con
      )
      DBI::dbExecute(con, sql)
      out <- dplyr::tbl(con, name)
    } else if (dbms(con) == "sql server") {
      suppressMessages({ # Suppress the "Created a temporary table named" message
        out <- dplyr::compute(x, name = name, temporary = temporary, ...)
      })
    } else {
      out <- dplyr::compute(x, name = name, temporary = temporary, ...)
    }

  } else {
    # not temporary
    out <- .computePermanent(x, name = name, schema = schema, overwrite = overwrite)
  }

  # retain attributes
  for (n in names(attributes(x))) {
    if (!(n %in% names(attributes(out)))) {
      attr(out, n) <- attr(x, n)
    }
  }

  class(out) <- class(x)

  return(out)
}


#' @rdname computeQuery
#' @export
compute_query <- computeQuery

# Get the full table name consisting of the schema and table name.
#
# @param x A dplyr query
# @param name Name of the table to be created.
# @param schema Schema to create the new table in
# Can be a length 1 or 2 vector.
# (e.g. schema = "my_schema", schema = c("my_schema", "dbo"))
#
# @return the full table name
getFullTableNameQuoted <- function(x, name, schema) {
  checkmate::assertClass(x, "tbl_sql")
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, null.ok = TRUE)
  checkmate::assertCharacter(name, len = 1)

  connection <- x$src$con
  if (length(schema) == 2) {
    fullNameQuoted <- paste(DBI::dbQuoteIdentifier(connection, schema[[1]]),
                            DBI::dbQuoteIdentifier(connection, schema[[2]]),
                            DBI::dbQuoteIdentifier(connection, name),
                            sep = ".")
  } else if (length(schema) == 1) {
    fullNameQuoted <- paste(DBI::dbQuoteIdentifier(connection, schema),
                            DBI::dbQuoteIdentifier(connection, name),
                            sep = ".")
  } else {
    fullNameQuoted <- DBI::dbQuoteIdentifier(connection, name)
  }
  return(fullNameQuoted)
}

