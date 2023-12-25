
#' Create a source for a cdm in a database.
#'
#' @param con Connection to a database.
#' @param cdmName Name of the cdm.
#' @param cdmScehma Schema where cdm tables are. You must have read access to
#' it.
#' @param writeSchema Schema where cohort tables are. You must have read and
#' write access to it.
#' @param achillesSchema Schema where achilles tables are. You must have read
#' access to it.
#'
#' @export
#'
dbSource <- function(con,
                     cdmName,
                     cdmSchema,
                     writeSchema,
                     achillesSchema = NULL) {
  # initial checks
  if (methods::is(con, "Pool")) {
    if (!rlang::is_installed("pool")) {
      cli::cli_abort("Please install the pool package.")
    }
    con <- pool::localCheckout(con)
  }
  if (methods::is(con, "DatabaseConnectorConnection")) {
    cli::cli_warn(
      "Not all functionality is supported when DatabaseConnector as your
      database driver! Some issues may occur."
    )
  }
  checkmate::assert_true(.dbIsValid(con))
  if (dbms(con) %in% c("duckdb", "sqlite") && missing(cdmSchema)) {
    cdmSchema <- c(schema = "main")
  }
  if (dbms(con) %in% c("duckdb", "sqlite") && missing(writeSchema)) {
    writeSchema <- c(schema = "main")
  }
  checkmate::assertCharacter(cdmName, len = 1, any.missing = FALSE)
  checkmate::assert_character(cdmSchema, min.len = 1, max.len = 3)
  checkmate::assert_character(writeSchema, min.len = 1, max.len = 3)
  checkmate::assert_character(achillesSchema, min.len = 1, max.len = 3, null.ok = TRUE)

  source <- structure(
    .Data = list(),
    "dbcon" = con,
    "cdm_schema" = cdmSchema,
    "write_schema" = writeSchema,
    "achilles_schema" = achillesSchema
  )
  class(source) <- "db_cdm"
  source <- omopgenerics::cdmSource(
    src = source, sourceName = cdmName, sourceType = dbms(con)
  )
  return(source)
}

#' @export
#' @importFrom omopgenerics insertTable
insertTable.db_cdm <- function(cdm,
                               name,
                               table,
                               overwrite = TRUE) {
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE)
  con <- attr(cdm, "dbcon")
  writeSchema <- attr(cdm, "write_schema")
  fullName <- inSchema(schema = writeSchema, table = name, dbms = dbms(con))
  if (overwrite) {
    omopgenerics::dropTable(cdm = cdm, name = name)
  }
  DBI::dbWriteTable(conn = con, name = fullName, value = table)
  x <- dplyr::tbl(src = con, fullName)
  attr(x, "tbl_name") <- name
  return(x)
}

#' @export
#' @importFrom tidyselect starts_with ends_with matches
#' @importFrom omopgenerics dropTable
dropTable.db_cdm <- function(cdm, name) {
  # initial checks
  schema <- attr(cdm, "write_schema")
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  # correct names
  allTables <- listTables(con, schema = schema)
  if(length(allTables) == 0) {
    return(invisible(TRUE))
  }

  names(allTables) <- allTables
  toDrop <- names(tidyselect::eval_select(
    expr = dplyr::any_of(name), data = allTables
  ))

  # drop tables
  for (i in seq_along(toDrop)) {
    if (toDrop[i] %in% allTables) {
      DBI::dbRemoveTable(con, inSchema(schema, toDrop[i], dbms = dbms(con)))
    }
  }

  return(invisible(TRUE))
}

#' @export
#' @importFrom dplyr compute
compute.db_cdm <- function(x, name, temporary, overwrite, ...) {
  cdm <- attr(x, "cdm_reference")
  if (is.null(cdm)) {
    cli::cli_abort("x must come from a valid cdm reference")
  }
  source <- attr(cdm, "cdm_source")
  if (is.null(source)) {
    cli::cli_abort("source of the cdm can not be NULL")
  }
  if (!temporary && attr(x, "tbl_name") == name) {
    intermediate <- TRUE
  } else {
    intermediate <- FALSE
  }

  schema <- attr(source, "write_schema")
  class(x) <- class(x)[!class(x) %in% "db_cdm"]

  if (intermediate) {
    intermediateName <- paste0(
      c(sample(letters, 5), "_test_table"), collapse = ""
    )
    x <- computeQuery(
      x = x, name = intermediateName, temporary = FALSE, schema = schema,
      overwrite = TRUE
    )
  }

  x <- x |>
    computeQuery(
      name = name, temporary = temporary, schema = schema, overwrite = overwrite
    )

  if (intermediate) {
    dropTable(cdm = source, name = intermediateName)
  }

  if (temporary) {
    name <- NA_character_
  }
  attr(x, "tbl_name") <- name
  return(x)
}
