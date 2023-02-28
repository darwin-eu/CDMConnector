#' Run a dplyr query and store the result in a permanent table
#'
#' This function has been superceded by `computeQuery` which should be used
#' instead of `computePermanent`.
#'
#' @param x A dplyr query
#' @param name Name of the table to be created
#' @param schema Schema to create the new table in
#' Can be a length 1 or 2 vector.
#' (e.g. schema = "my_schema", schema = c("my_schema", "dbo"))
#' @param overwrite If the table already exists in the remote database
#' should it be overwritten? (TRUE or FALSE)
#'
#' @return A dplyr reference to the newly created table
#' @export
#'
#' `r lifecycle::badge("superseded")`
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' concept <- dplyr::tbl(con, "concept")
#'
#' rxnorm_count <- concept %>%
#'   dplyr::filter(domain_id == "Drug") %>%
#'   dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#'   dplyr::count(isRxnorm) %>%
#'   computePermanent("rxnorm_count")
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
computePermanent <- function(x, name, schema = NULL, overwrite = FALSE) {
  rlang::inform(paste("`computePermanent` has been superseded. Use `computeQuery` instead."),
                .frequency = "once",
                .frequency_id = "computePermanent")

  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, null.ok = TRUE)
  checkmate::assertCharacter(name, len = 1)
  checkmate::assertClass(x, "tbl_sql")
  checkmate::assertLogical(overwrite, len = 1)

  fullNameQuoted <- getFullTableNameQuoted(x, name, schema)
  existingTables <- CDMConnector::listTables(x$src$con, schema = schema)
  if (name %in% existingTables) {
    if (overwrite) {
      DBI::dbRemoveTable(x$src$con, DBI::SQL(fullNameQuoted))
    } else {
      rlang::abort(paste(fullNameQuoted, "already exists.",
                         "Set overwrite = TRUE to recreate it."))
    }
  }

  if (CDMConnector::dbms(x$src$con) == "spark" &&
      !rlang::is_installed("SqlRender", version = "1.8.0")) {
    rlang::abort("SqlRender version 1.8.0 or later is required
                 to use computePermanent with spark.")
  }

  if (CDMConnector::dbms(x$src$con) %in% c("duckdb", "oracle")) {
    sql <- dbplyr::build_sql("CREATE TABLE ",
             if (!is.null(schema)) dbplyr::ident(schema),
             if (!is.null(schema)) dbplyr::sql("."), dbplyr::ident(name),
             " AS ", dbplyr::sql_render(x), con = x$src$con)

  } else if (CDMConnector::dbms(x$src$con) == "spark") {
    sql <- dbplyr::build_sql("CREATE ",
             if (overwrite) dbplyr::sql("OR REPLACE "),  "TABLE ",
             if (!is.null(schema)) dbplyr::ident(schema),
             if (!is.null(schema)) dbplyr::sql("."), dbplyr::ident(name),
             " AS ", dbplyr::sql_render(x), con = x$src$con)
  } else {
    sql <- glue::glue("SELECT * INTO {fullNameQuoted}
                      FROM ({dbplyr::sql_render(x)}) x")
    sql <- SqlRender::translate(sql,
                                targetDialect = CDMConnector::dbms(x$src$con))
  }

  DBI::dbExecute(x$src$con, sql)

  if (is(x$src$con, "duckdb_connection")) {
    ref <- dplyr::tbl(x$src$con, paste(c(schema, name), collapse = "."))
  } else if (length(schema) == 2) {
    ref <- dplyr::tbl(x$src$con,
                      dbplyr::in_catalog(schema[[1]], schema[[2]], name))
  } else if (length(schema) == 1) {
    ref <- dplyr::tbl(x$src$con, dbplyr::in_schema(schema, name))
  } else {
    ref <- dplyr::tbl(x$src$con, name)
  }
  return(ref)
}

#' Run a dplyr query and add the result set to an existing
#'
#' @param x A dplyr query
#' @param name Name of the table to be appended. If it does not already exist it
#'   will be created.
#' @param schema Schema where the table exists. Can be a length 1 or 2 vector.
#'   (e.g. schema = "my_schema", schema = c("my_schema", "dbo"))
#'
#' @return A dplyr reference to the newly created table
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' concept <- dplyr::tbl(con, "concept")
#'
#' # create a table
#' rxnorm_count <- concept %>%
#'   dplyr::filter(domain_id == "Drug") %>%
#'   dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#'   dplyr::count(domain_id, isRxnorm) %>%
#'   computePermanent("rxnorm_count")
#'
#' # append to an existing table
#' rxnorm_count <- concept %>%
#'   dplyr::filter(domain_id == "Procedure") %>%
#'   dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#'   dplyr::count(domain_id, isRxnorm) %>%
#'   appendPermanent("rxnorm_count")
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#'
#' }
appendPermanent <- function(x, name, schema = NULL) {
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, null.ok = TRUE)
  checkmate::assertCharacter(name, len = 1)
  checkmate::assertClass(x, "tbl_sql")

  fullNameQuoted <- getFullTableNameQuoted(x, name, schema)
  existingTables <- CDMConnector::listTables(x$src$con, schema = schema)
  if (!(tolower(name) %in% tolower(existingTables))) {
    return(computePermanent(x = x,
                            name = name,
                            schema = schema,
                            overwrite = FALSE))
  }

  sql <- glue::glue("INSERT INTO {fullNameQuoted} {dbplyr::sql_render(x)}")
  sql <- SqlRender::translate(sql,
                              targetDialect = CDMConnector::dbms(x$src$con))

  DBI::dbExecute(x$src$con, sql)

  if (length(schema) == 2) {
    ref <- dplyr::tbl(x$src$con,
                      dbplyr::in_catalog(schema[[1]], schema[[2]], name))
  } else if (length(schema) == 1) {
    ref <- dplyr::tbl(x$src$con, dbplyr::in_schema(schema, name))
  } else {
    ref <- dplyr::tbl(x$src$con, name)
  }
  return(ref)
}

uniqueTableName <- function() {
  i <- getOption("dbplyr_table_name", 0) + 1
  options(dbplyr_table_name = i)
  sprintf("dbplyr_%03i", i)
}

#' Execute dplyr query and save result in remote database
#'
#' This function is a wrapper around `dplyr::compute` that is tested on several
#' database systems. It is needed to handle edge cases where `dplyr::compute`
#' does not produce correct SQL.
#'
#' @param x A dplyr query
#' @param name The name of the table to create.
#' @param temporary Should the table be temporary: TRUE (default) or FALSE
#' @param schema The schema where the table should be created. Ignored if
#'   temporary = TRUE.
#' @param overwrite Should the table be overwritten if it already exists: TRUE
#'   or FALSE (default) Ignored if temporary = TRUE.
#' @param ... Further arguments passed on the `dplyr::compute`
#'
#' @return A `dplyr::tbl()` reference to the newly created table.
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#'
#' # create a temporary table in the remote database from a dplyr query
#' drugCount <- cdm$concept %>%
#'   dplyr::count(domain_id == "Drug") %>%
#'   computeQuery()
#'
#' # create a permanent table in the remote database from a dplyr query
#' drugCount <- cdm$concept %>%
#'   dplyr::count(domain_id == "Drug") %>%
#'   computeQuery("tmp_table", temporary = FALSE, schema = "main")
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
computeQuery <- function(x,
                         name = uniqueTableName(),
                         temporary = TRUE,
                         schema = NULL,
                         overwrite = FALSE,
                         ...) {

  checkmate::assertLogical(temporary, len = 1)

  con <- x$src$con

  if (temporary) {
    if (is(con, "OraConnection") || is(con, "Oracle")) {
      # https://github.com/tidyverse/dbplyr/issues/621#issuecomment-1362229669
      name <- paste0("ORA$PTT_", name)
      sql <- dbplyr::build_sql(
        "CREATE PRIVATE TEMPORARY TABLE \n",
        dbplyr::ident(name),
        dbplyr::sql(" ON COMMIT PRESERVE DEFINITION \n"),
        " AS\n",
        dbplyr::sql_render(x),
        con = con
      )
      DBI::dbExecute(con, sql)
      return(dplyr::tbl(con, name))
    } else if (is(con, "Spark SQL")) {
      sql <- dbplyr::build_sql(
        "CREATE ", if (overwrite) dbplyr::sql("OR REPLACE "),
        "TEMPORARY VIEW \n",
        dbplyr::ident(name), " AS\n",
        dbplyr::sql_render(x),
        con = con
      )
      DBI::dbExecute(con, sql)
      return(dplyr::tbl(con, name))
    } else {
      return(dplyr::compute(x, name = name, temporary = temporary, ...))
    }
  } else {
    computePermanent(x, name = name, schema = schema, overwrite = overwrite)
  }
}

#' Drop tables from write_schema of a cdm object
#'
#' cdm objects can have zero or more cohort tables stored in a special schema
#' where the user has write access. This function removes tables from a cdm's
#' write_schema
#'
#'
#' @param cdm A cdm reference
#' @param name A character vector of tables in the cdm's write_schema
#' @param verbose Print a message when dropping a table? TRUE or FALSE (default)
#'
#' @return Invisibly returns the cdm object
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#'
#' # create a temporary table in the remote database from a query
#' cdm$tmp_table <- cdm$concept %>%
#'   dplyr::count(domain_id == "Drug") %>%
#'   computeQuery("tmp_table", temporary = FALSE, schema = "main")
#'
#' "tmp_table" %in% DBI::dbListTables(con)
#' # [1] TRUE
#' "tmp_table" %in% names(cdm)
#' # [1] TRUE
#'
#' cdm <- dropTable(cdm, "tmp_table")
#'
#' "tmp_table" %in% DBI::dbListTables(con)
#' # [1] FALSE
#' "tmp_table" %in% names(cdm)
#' # [1] FALSE
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
dropTable <- function(cdm, name, verbose = FALSE) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(name, min.chars = 1, min.len = 1,  max.len = 1e5)
  schema <- attr(cdm, "write_schema")
  checkmate::assertCharacter(schema, min.chars = 1, min.len = 1, max.len = 2)
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  allTables <- CDMConnector::listTables(con, schema = schema)

  for (i in seq_along(name)) {

    if (name[i] %in% allTables) {
      if (verbose) {
        message(paste0("Dropping", schema, ".", name[i]))
      }
      DBI::dbRemoveTable(con, inSchema(schema, name[i]))
    }

    if (name[i] %in% names(cdm)) {
      cdm[[name[i]]] <- NULL
    }
  }

  return(invisible(cdm))
}

#' Drop tables starting with a prefix/stem
#'
#' @param cdm A CDM reference object
#' @param stem The prefix/stem identifying tables to drop. For example,
#' a stem of "study_results" would lead to dropping any table starting with this
#' (ie both "study_results" and "study_results_analysis_1" would be dropped if
#' they exist in the write schema). Prefix will be interpreted as a regular
#' expression.
#' @param verbose Print a message when dropping a table? TRUE or FALSE (default)
#'
#' @return Invisibly returns the cdm object
#' @export
dropStemTables <- function(cdm, stem, verbose = FALSE) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(stem, len = 1, min.chars = 1)
  schema <- attr(cdm, "write_schema")
  checkmate::assertCharacter(schema, min.chars = 1, min.len = 1, max.len = 2)
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  allTables <- CDMConnector::listTables(con, schema)
  tablesToDrop <- stringr::str_subset(allTables, paste0("^", stem))

  cdm <- dropTable(cdm, tablesToDrop, verbose)
  return(invisible(cdm))
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

