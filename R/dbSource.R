
#' Create a source for a cdm in a database.
#'
#' @param con Connection to a database.
#' @param writeSchema Schema where cohort tables are. You must have read and
#' write access to it.
#'
#' @export
#'
dbSource <- function(con, writeSchema) {
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
  if (dbms(con) %in% c("duckdb", "sqlite") && missing(writeSchema)) {
    writeSchema <- c(schema = "main")
  }
  checkmate::assert_character(writeSchema, min.len = 1, max.len = 3)

  if (length(writeSchema) != 3 & !"schema" %in% names(writeSchema)) {

    rlang::inform(c(
      "No prefix was supplied. Consider adding a prefix in your write_schema.",
      "A prefix",
      "*" = "Allows you to work in your own subschema",
      "*" = "Helps Prevent accidentally dropping tables of others using the same database schema",
      "*" = "Allows you to easily clean up intermediate tables at the end of the analysis",
      "To use a prefix you have to specify it when you create the cdm_reference:",
      "`write_schema = c(schema = \"main\", prefix = \"my_prefix_\")`"
    ),
    .frequency = "once",
    .frequency_id = "write_prefix_message")
  }

  source <- structure(
    .Data = list(),
    "dbcon" = con,
    "write_schema" = writeSchema
  )
  class(source) <- "db_cdm"
  source <- omopgenerics::newCdmSource(src = source, sourceType = dbms(con))
  return(source)
}

#' @export
insertTable.db_cdm <- function(cdm,
                               name,
                               table,
                               overwrite = TRUE) {
  src <- cdm
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE)
  con <- attr(src, "dbcon")
  writeSchema <- attr(src, "write_schema")
  fullName <- inSchema(schema = writeSchema, table = name, dbms = dbms(con))
  if (overwrite && (name %in% listTables(con, writeSchema))) {
    DBI::dbRemoveTable(con, name = fullName)
  }
  if (!inherits(table, "data.frame")) {
    table <- table |> dplyr::collect()
  }
  DBI::dbWriteTable(conn = con, name = fullName, value = table)
  x <- dplyr::tbl(src = con, fullName) |>
    omopgenerics::newCdmTable(src = src, name = name)
  return(x)
}

#' @export
#' @importFrom tidyselect starts_with ends_with matches
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
    DBI::dbRemoveTable(conn = con, name = inSchema(
      schema = schema, table = toDrop[i], dbms = dbms(con)
    ))
  }

  return(invisible(TRUE))
}

#' @export
#' @importFrom dplyr compute
compute.db_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, ...) {

  # check source and name
  source <- attr(x, "tbl_source")
  con <- attr(source, "dbcon")
  if (is.null(source)) cli::cli_abort("table source not found.")
  oldName <- attr(x, "tbl_name")
  if (is.null(oldName)) cli::cli_abort("table name not found.")

  # whether an intermediate table will be needed
  if (!temporary & !is.na(oldName)) {
    if (oldName == name) {
      intermediate <- TRUE
      intername <- paste0(c(sample(letters, 5), "_temp_table"), collapse = "")
    } else {
      intermediate <- FALSE
    }
  } else {
    intermediate <- FALSE
  }

  # get schema
  schema <- attr(source, "write_schema")
  if (is.null(schema)) cli::cli_abort("write_schema can not be NULL.")

  # remove db_cdm class to avoid recursive call
  class(x) <- class(x)[!class(x) %in% "db_cdm"]

  if (intermediate) {
    x <- x |>
      .computeQuery(
        name = intername, temporary = FALSE, schema = schema, overwrite = FALSE
      )
  }

  x <- x |>
    .computeQuery(
      name = name, temporary = temporary, schema = schema, overwrite = overwrite
    )

  if (intermediate) {
    DBI::dbRemoveTable(con, name = inSchema(schema = schema, table = intername, dbms = dbms(con)))
    if (intername %in% list_tables(con, schema)) {
      cli::cli_warn("Intermediate table `{intername}` was not dropped as expected.")
    }
  }

  class(x) <- c("db_cdm", class(x))
  return(x)
}

#' @export
#' @importFrom omopgenerics insertFromSource
insertFromSource.db_cdm <- function(cdm, value) {
  if (inherits(value, "data.frame")) {
    cli::cli_abort(
      "To insert a local table to a cdm_reference object use insertTable
      function."
    )
  }
  if (!inherits(value, "tbl_lazy")) {
    cli::cli_abort(
      "Can't assign an object of class: {paste0(class(value), collapse = ", ")}
      to a db_con cdm_reference object."
    )
  }
  con <- cdmCon(cdm)
  schema <- cdmWriteSchema(cdm)
  if (!identical(con, dbplyr::remote_con(value))) {
    cli::cli_abort(
      "The cdm object and the table have different connection sources."
    )
  }
  remoteName <- dbplyr::remote_name(value)
  if ("dbplyr" == substr(remoteName, 1, 6)) {
    remoteName <- NA_character_
  } else if ("prefix" %in% names(schema)) {
    prefix <- schema["prefix"] |> unname()
    if (substr(remoteName, 1, nchar(prefix)) == prefix) {
      remoteName <- substr(remoteName, nchar(prefix) + 1, nchar(remoteName))
    }
  }
  value <- omopgenerics::newCdmTable(
    table = value, src = attr(cdm, "cdm_source"), name = remoteName
  )
  return(value)
}

