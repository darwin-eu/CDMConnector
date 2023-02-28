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
  if (is(dbObj, "OraConnection")) {
    is.character(DBI::dbListTables(dbObj))
  } else {
    DBI::dbIsValid(dbObj, ...)
  }
}

# helper function that takes care of branching based on schema length
inSchema <- function(table, writeSchema) {
  if (length(writeSchema) == 2) {
    DBI::Id(catalog = writeSchema[1], schema = writeSchema[2], table = table)
  } else {
    stopifnot(length(writeSchema) == 1)
    DBI::Id(schema = writeSchema, table = table)
  }
}

#' Get the full table name consisting of the schema and table name.
#'
#' @param x A dplyr query
#' @param name Name of the table to be created.
#' @param schema Schema to create the new table in
#' Can be a length 1 or 2 vector.
#' (e.g. schema = "my_schema", schema = c("my_schema", "dbo"))
#'
#' @return the full table name
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' cdm <- cdm_from_con(con)
#' schema <- c("my_schema", "dbo")
#' name <- "myTable"
#' getFullTableNameQuoted(cdm$person, name, schema)
#' }
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

#' Drop tables starting with a prefix
#'
#' @param cdm A CDM reference object
#' @param prefix The prefix identifying tables to drop. For example,
#' a prefix of "study_results" would lead to dropping any table starting with this
#' (ie both "study_results" and "study_results_analysis_1" would be dropped if
#' they exist in the write schema). Prefix will be interpreted as a regular
#' expression.
#'
#' @return NULL
#' @export
dropTables <- function(cdm,
                       prefix,
                       verbose = FALSE) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(prefix, len = 1, min.chars = 1)
  schema <- attr(cdm, "write_schema")
  con <- attr(cdm, "dbcon")

  allTables <- CDMConnector::listTables(con, schema)
  tablesToDrop <- stringr::str_subset(allTables, paste0("^", prefix))

  for (i in seq_along(tablesToDrop)) {
    if (verbose) {
      message(paste0("Dropping", schema, ".", tablesToDrop[i]))
    }

    DBI::dbRemoveTable(con, inSchema(schema, tablesToDrop[i]))

    if (tablesToDrop[i] %in% names(cdm)) {
      cdm[[tablesToDrop[i]]] <- NULL
    }
  }
  return(invisible(cdm))
}

# Helper function to deal with compound schemas
inSchema <- function(schema, table) {
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  checkmate::assertCharacter(table, len = 1)
  if (length(schema) == 2) {
    return(DBI::Id(catalog = schema[1], schema = schema[2], table = table))
  } else {
    return(DBI::Id(schema = schema, table = table))
  }
}
