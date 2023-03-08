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

# Helper function to deal with compound schemas
inSchema <- function(schema, table, dbms = NULL) {
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  checkmate::assertCharacter(table, len = 1)
  checkmate::assertCharacter(dbms, len = 1, null.ok = TRUE)

  if (!is.null(dbms) && (dbms %in% c("oracle"))) {
    # some dbms need in_schema, others DBI::Id
    switch(length(schema),
           dbplyr::in_schema(schema = schema, table = table),
           dbplyr::in_catalog(catalog = schema[1], schema = schema[2], table = table))
  } else {
    switch(length(schema),
           DBI::Id(schema = schema, table = table),
           DBI::Id(catalog = schema[1], schema = schema[2], table = table))
  }
}
