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
#' @param writeSchema Schema where cohort tables are. If provided must have read and
#' write access to it. If NULL the cdm will be created without a write_schema.
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

  if (is.null(writeSchema)) {
    # create cdm source without a write schema
    source <- structure(.Data = source, source_type = dbms(con), class = c("cdm_source", class(source)))
  } else {
    source <- omopgenerics::newCdmSource(src = source, sourceType = dbms(con))
  }
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
  } else if (dbms(con) == "spark") {
    # Spark/Simba ODBC often fails on parameterized INSERT (UNBOUND_SQL_PARAMETER).
    # Create table then insert using literal VALUES.
    # Format Date/POSIXt as ISO strings so driver doesn't produce malformed timestamps (e.g. 00-00-00).
    table_for_insert <- .formatDatesForSparkInsert(table)
    .dbCreateTable(con, fullName, table)
    if (nrow(table_for_insert) > 0) {
      qualifiedName <- .qualifiedNameForSql(con, fullName)
      cols <- paste(DBI::dbQuoteIdentifier(con, names(table_for_insert)), collapse = ", ")
      for (i in seq_len(nrow(table_for_insert))) {
        row <- table_for_insert[i, , drop = FALSE]
        vals <- vapply(seq_len(ncol(table_for_insert)), function(j) {
          DBI::dbQuoteLiteral(con, row[[j]][[1]])
        }, character(1))
        sql <- paste0("INSERT INTO ", qualifiedName, " (", cols, ") VALUES (", paste(vals, collapse = ", "), ")")
        DBI::dbExecute(con, sql)
      }
    }
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
#' @param dropPrefixTables Whether to drop tables in the writeSchema prefixed with `writePrefix`
#' @param ... Not used. Included for compatibility with generic.
#'
#' @method cdmDisconnect db_cdm
#' @export
cdmDisconnect.db_cdm <- function(cdm, dropPrefixTables = FALSE, ...) {
  omopgenerics::assertLogical(dropPrefixTables, length = 1)
  # we are expecting a cdm source
  checkmate::assert_class(cdm, "cdm_source")
  # NOTE: what is pass in as cdm from omopgenerics is actually a cdm source
  con <- attr(cdm, "dbcon")
  if (!methods::is(con, "DatabaseConnectorConnection")) {
    checkmate::assertTRUE(DBI::dbIsValid(con))
  }

  # if the database is duckdb AND it is in the temp folder, remove the file to save temp space
  isDuckdbInTempdir <- function(con) {
    # DatabaseConnector and other wrappers don't have con@driver@dbdir; avoid slot access.
    if (methods::is(con, "DatabaseConnectorConnection")) return(FALSE)
    if (!methods::is(con, "duckdb_connection")) return(FALSE)
    tryCatch({
      dbdir <- con@driver@dbdir
      if (!is.character(dbdir) || length(dbdir) != 1L || is.na(dbdir) || dbdir == "" || dbdir == ":memory:") {
        return(FALSE)
      }
      db_path  <- normalizePath(dbdir, winslash = "/", mustWork = FALSE)
      tmp_path <- normalizePath(tempdir(), winslash = "/", mustWork = FALSE)
      # macOS often has /private prefix
      db_path  <- sub("^/private", "", db_path)
      tmp_path <- sub("^/private", "", tmp_path)
      startsWith(db_path, tmp_path)
    }, error = function(e) FALSE)
  }

  # TODO implement this cleanup for database connector duckdb connections
  if (dbms(con) == "duckdb" && isDuckdbInTempdir(con) && !methods::is(con, "DatabaseConnectorConnection")) {
    fileToRemove <- con@driver@dbdir
    DBI::dbDisconnect(con, shutdown = TRUE)
    try(unlink(fileToRemove))
    if (file.exists(fileToRemove)) {
      cli::cli_inform("Unable to remove temp duckdb file: {fileToRemove}")
    }
    return(invisible(TRUE))
  }

  if (dropPrefixTables) {
    # remove prefix tables
    writeSchema <- attr(cdm, "write_schema")
    writePrefix <- writeSchema["prefix"]

    if (is.na(writePrefix) || writePrefix == "") {
      cli::cli_inform("`dropPrefixTables = TRUE` but no writePrefix was specified for the cdm")
    } else {
      checkmate::assertCharacter(writePrefix, len = 1, any.missing = FALSE, min.chars = 1)
      # listTables returns only the prefixed tables since prefix is in the writeSchema
      tablesToDrop <- listTables(con, writeSchema)
      if (length(tablesToDrop) > 0) {
        invisible(dropSourceTable(cdm = cdm, name = tablesToDrop))
      }
    }
  }

   # if (dbms(con) == "spark") {
    # TODO drop temp emulation prefix (it has to be incorporated in the dbSource object, so it is available in this function)

    # tbls <- listTables(con, schema = schema)
    # tempEmulationTablesToDrop <- stringr::str_subset(tbls, attr(cdm, "temp_emulation_prefix"))
    # # try to drop the temp emulation tables
    # purrr::walk(tempEmulationTablesToDrop,
    #             ~tryCatch(DBI::dbRemoveTable(con, .inSchema(schema, ., dbms = dbms(con))),
    #                       error = function(e) invisible(NULL)))
   # }

  if (methods::is(con, "DatabaseConnectorDbiConnection")) {
    DatabaseConnector::disconnect(con)
  } else if (dbms(con) == "duckdb") {
    DBI::dbDisconnect(con, shutdown = TRUE)
  } else {
    DBI::dbDisconnect(con)
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
