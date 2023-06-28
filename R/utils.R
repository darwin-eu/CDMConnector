#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
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
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  checkmate::assertCharacter(table, len = 1)
  checkmate::assertCharacter(dbms, len = 1, null.ok = TRUE)

  if (isFALSE(dbms %in% c("snowflake", "sql server"))) {
    # only a few dbms support three part names
    checkmate::assertCharacter(schema, len = 1)
  }

  if (isTRUE(dbms %in% c("bigquery"))) {
    checkmate::assertCharacter(schema, len = 1)
    out <- paste(c(schema, table), collapse = ".")
  } else {
    out <- switch(length(schema),
      DBI::Id(schema = schema, table = table),
      DBI::Id(catalog = schema[1], schema = schema[2], table = table))
  }
  return(out)
}

#' @export
#' @rdname inSchema
in_schema <- inSchema

# Internal function to normalize representation of database schema
# A schema can have 3 possible components: catalog, schema, prefix
# It can be passed in using a few different representations.

# A named vector, an unnamed vector, a single string with '.' separating the components, or a DBI::Id
# This function accepts any one of these and returns a DBI::Id
normalize_schema <- function(schema) {
  # length 1, 2, or 3 vector (named or unnamed)
  # DBI::Id

  if (is.null(schema)) {
    return(list(schema = NULL, prefix = NULL))
  }

  if (methods::is(schema, "Id")) {
    # convert Id to named vector
    schema <- attr(schema, "name")
  }

  checkmate::assert_character(schema, min.len = 1, max.len = 3)

  # handle schema names like 'schema.dbo' by splitting on '.'
  if (!is.null(schema) && length(schema) == 1) {
    schema <- strsplit(schema, "\\.")[[1]]
    checkmate::assert_character(schema, min.len = 1, max.len = 2)
  }

  # check that all elements are named or none are named
  if (length(names(schema)) > 0) {
    checkmate::check_names(schema, type = "strict")
    checkmate::check_subset(names(schema), c("catalog", "schema", "prefix"))
  } else {
    # add names if they don't already exist
    switch (length(schema),
            names(schema) <- "schema",
            names(schema) <- c("catalog", "schema"),
            names(schema) <- c("catalog", "schema", "prefix")
    )
  }

  # extract out each part
  prefix <- if(is.na(schema["prefix"])) NULL else schema["prefix"] %>% unname
  checkmate::assert_character(prefix, len = 1, min.chars = 0, pattern = "[_a-z]+", null.ok = TRUE)
  catalog <- if(is.na(schema["catalog"])) NULL else schema["catalog"] %>% unname
  schema <- schema["schema"] %>% unname

  return(list(schema = c(catalog, schema), prefix = prefix))
}

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

