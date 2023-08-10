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
  checkmate::assertCharacter(schema, min.len = 1, max.len = 3)
  checkmate::assertCharacter(table, len = 1)
  checkmate::assertCharacter(dbms, len = 1, null.ok = TRUE)

  if ("prefix" %in% names(schema)) {
    checkmate::assertCharacter(schema['prefix'], len = 1, min.chars = 1, pattern = "[a-zA-Z1-9_]+")
    table <- paste0(schema['prefix'], table)
    schema <- schema[!names(schema) %in% "prefix"]
    checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  }

  if (isFALSE(dbms %in% c("snowflake", "sql server"))) {
    # only a few dbms support three part names
    checkmate::assertCharacter(schema, len = 1)
  }

  schema <- unname(schema)

  if (isTRUE(dbms %in% c("bigquery"))) { #TODO bigrquery needs to fix this
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

  if (methods::is(con, "Pool")) {
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
      stringr::str_subset(x, paste0("^", prefix)) %>% stringr::str_remove(paste0("^", prefix))
    }
  } else {
    process_prefix <- function(x) {x}
  }

  checkmate::assert_character(schema, null.ok = TRUE, min.len = 1, max.len = 2, min.chars = 1)

  if (is.null(schema)) return(DBI::dbListTables(con))
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
    sql <- paste("SHOW TABLES", if (!is.null(schema)) paste("IN", schema[[1]]))
    out <- DBI::dbGetQuery(con, sql) %>%
      dplyr::filter(.data$isTemporary == FALSE) %>%
      dplyr::pull(.data$tableName)

    return(process_prefix(out))
  }

  if (methods::is(con, "OdbcConnection")) {
    if (length(schema) == 1) {
      out <- DBI::dbListTables(con, schema_name = schema)
    } else {
      out <- DBI::dbListTables(con, catalog_name = schema[[1]], schema_name = schema[[2]])
    }

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

  rlang::abort(paste(paste(class(con), collapse = ", "), "connection not supported"))
}

#' @rdname list_tables
#' @export
listTables <- list_tables

# To silence warning <BigQueryConnection> uses an old dbplyr interface
# https://github.com/r-dbi/bigrquery/issues/508

#' @importFrom dbplyr dbplyr_edition
#' @export
dbplyr_edition.BigQueryConnection<- function(con) 2L

# Create the cdm tables in a database
execute_ddl <- function(con, cdm_schema, cdm_version = "5.3", dbms = "duckdb", tables = tbl_group("all"), prefix = "") {

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

    DBI::dbCreateTable(con, inSchema(cdm_schema, paste0(prefix, tables[i]), dbms = dbms(con)), fields = fields)
  }
}

# get a unique prefix based on current time. internal function.
unique_prefix <- function() {
  as.integer((as.numeric(Sys.time())*10) %% 1e6)
}
