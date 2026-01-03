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

#' Create a source for a cdm in a database.
#'
#' @param con Connection to a database.
#' @param writeSchema Schema where cohort tables are. You must have read and
#' write access to it.
#'
#' @export
dbSource <- function(con, writeSchema) {
  # initial checks
  if (methods::is(con, "Pool")) {
    if (!rlang::is_installed("pool")) {
      cli::cli_abort("Please install the pool package.")
    }
    con <- pool::localCheckout(con)
  }

  checkmate::assert_true(.dbIsValid(con))
  if (dbms(con) %in% c("duckdb", "sqlite") && missing(writeSchema)) {
    writeSchema <- c(schema = "main")
  }
  checkmate::assert_character(writeSchema, min.len = 1, max.len = 3, null.ok = TRUE)

  source <- structure(
    .Data = list(),
    "dbcon" = con,
    "write_schema" = writeSchema
  )
  class(source) <- "db_cdm"
  source <- omopgenerics::newCdmSource(src = source, sourceType = dbms(con))
  return(source)
}

#' @method insertTable db_cdm
#' @export
insertTable.db_cdm <- function(cdm,
                               name,
                               table,
                               overwrite = TRUE,
                               temporary = FALSE,
                               ...) {
  table <- dplyr::as_tibble(table)
  src <- cdm
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE)
  con <- attr(src, "dbcon")
  writeSchema <- attr(src, "write_schema")
  fullName <- .inSchema(schema = writeSchema, table = name, dbms = dbms(con))
  if (overwrite && (name %in% listTables(con, writeSchema))) {
    DBI::dbRemoveTable(con, name = fullName)
  }
  if (!inherits(table, "data.frame")) {
    table <- table |> dplyr::collect()
  }

  if (dbms(con) %in% c("bigquery") && nrow(table) == 0) {
    .dbCreateTable(con, fullName, table)
  } else {
    DBI::dbWriteTable(conn = con, name = fullName, value = table, temporary = temporary)
  }

  x <- dplyr::tbl(src = con, fullName) |>
    dplyr::rename_all(tolower) |>
    omopgenerics::newCdmTable(src = src, name = name) |>
    dplyr::select(colnames(table))
  return(x)
}

#' Drop table from a database backed cdm object
#'
#' Tables will be dropped from the write schema of the cdm.
#'
#' @param cdm a cdm_reference object
#' @param name A character vector of table names to be dropped
#'
#' @importFrom omopgenerics dropTable
#' @importFrom tidyselect starts_with ends_with matches
#'
#' @method dropTable db_cdm
#' @export
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
    DBI::dbRemoveTable(conn = con, name = .inSchema(
      schema = schema, table = toDrop[i], dbms = dbms(con)
    ))
  }

  return(invisible(TRUE))
}

#' @importFrom dplyr compute
#' @export
dplyr::compute

#' @method compute db_cdm
#' @export
#' @importFrom dplyr compute
compute.db_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, ...) {

  # check source and name
  source <- attr(x, "tbl_source")
  con <- attr(source, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  if (dbms(con) == "spark" & isTRUE(temporary)) {
    cdm <- attr(x, "cdm_reference")
    checkmate::assertClass(cdm, "cdm_reference")
    prefix <- attr(cdm, "temp_emulation_prefix")
    if (is.null(prefix)) {
      rlang::abort("temp_emulation_prefix is missing! Please open an issue at https://github.com/darwin-eu/CDMConnector")
    }
    name <- paste0(prefix, name)
    temporary <- FALSE
  }

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
    DBI::dbRemoveTable(con, name = .inSchema(schema = schema, table = intername, dbms = dbms(con)))
    if (intername %in% listTables(con, schema)) {
      cli::cli_warn("Intermediate table `{intername}` was not dropped as expected.")
    }
  }

  class(x) <- c("db_cdm", class(x))
  return(x)
}

#' @importFrom omopgenerics insertFromSource
#' @method insertFromSource db_cdm
#' @export
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

#' @method cdmTableFromSource db_cdm
#' @export
#' @importFrom omopgenerics cdmTableFromSource
cdmTableFromSource.db_cdm <- function(src, value) {
  if (inherits(value, "data.frame")) {
    cli::cli_abort(
      "To insert a local table to a cdm_reference object use insertTable
      function."
    )
  }
  if (!inherits(value, "tbl_lazy")) {
    cli::cli_abort(
      "Can't assign an object of class: {paste0(class(value), collapse = ', ')}
      to a db_con cdm_reference object."
    )
  }
  con <- attr(src, "dbcon")
  schema <- attr(src, "write_schema")
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
    table = value, src = src, name = remoteName
  )
  return(value)
}

#' @method listSourceTables db_cdm
#' @export
listSourceTables.db_cdm <- function(cdm) {
  listTables(con = attr(cdm, "dbcon"), schema = attr(cdm, "write_schema"))
}

#' @method dropSourceTable db_cdm
#' @export
dropSourceTable.db_cdm <- function(cdm, name) {
  # initial checks
  schema <- attr(cdm, "write_schema")
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  # drop tables
  for (i in seq_along(name)) {
    DBI::dbRemoveTable(conn = con, name = .inSchema(
      schema = schema, table = name[i], dbms = dbms(con)
    ))
  }

  return(cdm)
}

#' @method readSourceTable db_cdm
#' @export
readSourceTable.db_cdm <- function(cdm, name) {
  con <- attr(cdm, "dbcon")
  schema <- attr(cdm, "write_schema")
  fullName <- .inSchema(schema = schema, table = name, dbms = dbms(con))
  dplyr::tbl(src = con, fullName) |>
    dplyr::rename_all(tolower) |>
    omopgenerics::newCdmTable(src = cdm, name = tolower(name))
}

#' @method insertCdmTo db_cdm
#' @export
insertCdmTo.db_cdm <- function(cdm, to) {
  # input check
  omopgenerics::assertClass(cdm, "cdm_reference")

  con <- attr(to, "dbcon")
  writeSchema <- attr(to, "write_schema")

  achillesSchema <- NULL
  cohorts <- character()
  other <- character()
  for (nm in names(cdm)) {
    x <- dplyr::collect(cdm[[nm]])
    cl <- class(x)
    if ("achilles_table" %in% cl) {
      achilles <- writeSchema
    }
    if (!any(c("achilles_table", "omop_table", "cohort_table") %in% cl)) {
      other <- c(other, nm)
    }
    insertTable(cdm = to, name = nm, table = x, overwrite = TRUE)
    if ("cohort_table" %in% cl) {
      cohorts <- c(cohorts, nm)
      insertTable(cdm = to, name = paste0(nm, "_set"), table = attr(x, "cohort_set"), overwrite = TRUE)
      insertTable(cdm = to, name = paste0(nm, "_attrition"), table = attr(x, "cohort_attrition"), overwrite = TRUE)
      insertTable(cdm = to, name = paste0(nm, "_codelist"), table = attr(x, "cohort_codelist"), overwrite = TRUE)
    }
  }

  newCdm <- CDMConnector::cdmFromCon(
    con = attr(to, "dbcon"),
    cdmSchema = writeSchema,
    writeSchema = writeSchema,
    cohortTables = cohorts,
    achillesSchema = achillesSchema,
    cdmName = cdmName(cdm),
    cdmVersion = cdmVersion(cdm),
    .softValidation = TRUE
  )

  newCdm <- omopgenerics::readSourceTable(cdm = newCdm, name = other)

  return(newCdm)
}

#' Disconnect the connection of the cdm object
#'
#' This function will disconnect from the database as well as drop
#' "temporary" tables that were created on database systems that do not support
#' actual temporary tables. Currently temp tables are emulated on
#' Spark/Databricks systems.
#'
#' @param cdm cdm reference
#' @param dropWriteSchema Whether to drop tables in the writeSchema
#' @param ... Not used
#'
#' @method cdmDisconnect db_cdm
#' @export
cdmDisconnect.db_cdm <- function(cdm, dropWriteSchema = FALSE, ...) {
  omopgenerics::assertLogical(dropWriteSchema, length = 1)

  con <- attr(cdm, "dbcon")
  writeSchema <- attr(cdm, "write_schema")

  if (methods::is(con, "DatabaseConnectorDbiConnection")) {
    on.exit(DatabaseConnector::disconnect(con), add = TRUE)
  } else {
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  }

  # drop tables if needed
  if (dropWriteSchema) {
    dropSourceTable(cdm = cdm, name = dplyr::everything())
  }

  if (dbms(con) == "spark") {
    # TODO drop temp emulation prefix (it has to be incorporated in the dbSource object, so it is available in this function)

    # tbls <- listTables(con, schema = schema)
    # tempEmulationTablesToDrop <- stringr::str_subset(tbls, attr(cdm, "temp_emulation_prefix"))
    # # try to drop the temp emulation tables
    # purrr::walk(tempEmulationTablesToDrop,
    #             ~tryCatch(DBI::dbRemoveTable(con, .inSchema(schema, ., dbms = dbms(con))),
    #                       error = function(e) invisible(NULL)))
  }

  return(invisible(TRUE))
}

#' @export
summary.db_cdm <- function(object, ...) {
  con <- attr(object, "dbcon")
  result <- list(package = "CDMConnector")
  schema <- attr(object, "write_schema")
  if ("catalog" %in% names(schema)) {
    result$write_catalog <- schema[["catalog"]]
  }
  if ("schema" %in% names(schema)) {
    result$write_schema <- schema[["schema"]]
  }
  if ("prefix" %in% names(schema)) {
    result$write_prefix <- schema[["prefix"]]
  }
  return(result)
}
