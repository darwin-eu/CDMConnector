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

  if (isTRUE(dbms == "oracle")) {
    # some dbms need in_schema, others DBI::Id
    switch(length(schema),
           dbplyr::in_schema(schema = schema, table = table),
           dbplyr::in_catalog(catalog = schema[1], schema = schema[2], table = table))
  } else if (isTRUE(dbms == "duckdb")) {
    checkmate::assertCharacter(schema, len = 1)
    paste0(schema, ".", table)
  } else {
    switch(length(schema),
           DBI::Id(schema = schema, table = table),
           DBI::Id(catalog = schema[1], schema = schema[2], table = table))
  }
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

  if (is(schema, "Id")) {
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
