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
