#' Run a dplyr query and store the result in a permanent table
#'
#' @param x A dplyr query
#' @param name Name of the table to be created
#' @param schema Schema to create the new table in
#' Can be a length 1 or 2 vector. (e.g. schema = "my_schema", schema = c("my_schema", "dbo))
#' @param overwrite If the table already exists in the remote database should it be overwritten? (TRUE or FALSE)
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
#' rxnorm_count <- concept %>%
#'   dplyr::filter(domain_id == "Drug") %>%
#'   dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#'   dplyr::count(isRxnorm) %>%
#'   computePermanent("rxnorm_count")
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
computePermanent <- function(x, name, schema = NULL, overwrite = FALSE) {
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, null.ok = TRUE)
  checkmate::assertCharacter(name, len = 1)
  checkmate::assertClass(x, "tbl_sql")
  checkmate::assertLogical(overwrite, len = 1)

  if (length(schema) == 2) {
    fullName <- paste(schema[[1]], schema[[2]], name, sep = ".")
  } else if (length(schema) == 1) {
    fullName <- paste(schema, name, sep = ".")
  } else {
    fullName <- name
  }

  existingTables <- CDMConnector::listTables(x$src$con, schema = schema)
  if (name %in% existingTables) {
    if (overwrite) {
      DBI::dbRemoveTable(x$src$con, DBI::SQL(fullName))
    } else {
      rlang::abort(paste(fullName, "already exists. Set overwrite = TRUE to recreate it."))
    }
  }

  if (CDMConnector::dbms(x$src$con) == "spark" && !rlang::is_installed("SqlRender", version = "1.8.0")) {
    rlang::abort("SqlRender version 1.8.0 or later is required to use computePermanent with spark.")
  }

  # TODO fix select * INTO translation for duckdb in SqlRender
  if (CDMConnector::dbms(x$src$con) %in% c("duckdb", "spark")) {
    sql <- glue::glue("CREATE TABLE {fullName} AS {dbplyr::sql_render(x)}")
  } else {
    sql <- glue::glue("SELECT * INTO {fullName} FROM ({dbplyr::sql_render(x)}) x")
    sql <- SqlRender::translate(sql, targetDialect = CDMConnector::dbms(x$src$con))
  }

  DBI::dbExecute(x$src$con, sql)

  if (length(schema) == 2) {
    ref <- dplyr::tbl(x$src$con, dbplyr::in_catalog(schema[[1]], schema[[2]], name))
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
#' @param name Name of the table to be appended. If it does not already exist it will be created.
#' @param schema Schema where the table exists.
#' Can be a length 1 or 2 vector. (e.g. schema = "my_schema", schema = c("my_schema", "dbo))
#'
#' @return A dplyr reference to the newly created table
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(SqlUtilities)
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

  if (length(schema) == 2) {
    fullName <- paste(schema[[1]], schema[[2]], name, sep = ".")
  } else if (length(schema) == 1) {
    fullName <- paste(schema, name, sep = ".")
  } else {
    fullName <- name
  }

  existingTables <- CDMConnector::listTables(x$src$con, schema = schema)
  if (!(name %in% existingTables)) {
    return(computePermanent(x = x, name = name, schema = schema, overwrite = FALSE))
  }

  sql <- glue::glue("INSERT INTO {fullName} {dbplyr::sql_render(x)}")
  sql <- SqlRender::translate(sql, targetDialect = CDMConnector::dbms(x$src$con))

  DBI::dbExecute(x$src$con, sql)

  if (length(schema) == 2) {
    ref <- dplyr::tbl(x$src$con, dbplyr::in_catalog(schema[[1]], schema[[2]], name))
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
#' This function is a wrapper around `dplyr::compute` that is tested on several database systems.
#' It is needed to handle edge cases where `dplyr::compute` does not produce correct SQL.
#'
#' @param x A dplyr query
#' @param name The name of the table to create.
#' @param temporary Should the table be temporary: TRUE (default) or FALSE
#' @param schema The schema where the table should be created. Ignored if temporary = TRUE.
#' @param overwrite Should the table be overwritten if it already exists: TRUE or FALSE (default)
#'       Ignored if temporary = TRUE.
#' @param ... Further arguments passed on the `dplyr::compute`
#'
#' @return A `dplyr::tbl()` reference to the newly created table.
#' @export
computeQuery <- function(x, name = uniqueTableName(), temporary = TRUE, schema = NULL, overwrite = FALSE, ...) {

  checkmate::assertLogical(temporary, len = 1)

  con <- x$src$con

  if (temporary) {
    if(is(con, "OraConnection")) {
      name <- paste0("ORA$PTT_", name)
      sql <- dbplyr::build_sql(
        "CREATE ", dbplyr::sql("PRIVATE TEMPORARY "), "TABLE \n",
        dbplyr::as.sql(name, con), dbplyr::sql(" ON COMMIT PRESERVE DEFINITION \n"), " AS\n",
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


# possible dbplyr solution from https://github.com/tidyverse/dbplyr/issues/621#issuecomment-1362229669
# sql_query_save.OraConnection <- function(con, sql, name, temporary = TRUE, ...) {
#   print(name)
#   if (temporary) {
#     assign(analyze, FALSE, parent.frame())
#     assign("name", paste0("ORA$PTT_", name), parent.frame())
#     name <- paste0("ORA$PTT_", name)
#   }
#   s <- dbplyr::build_sql(
#     "CREATE ", if (temporary) dbplyr::sql("PRIVATE TEMPORARY "), "TABLE \n",
#     dbplyr::as.sql(name, con), if (temporary) dbplyr::sql(" ON COMMIT PRESERVE DEFINITION \n"), " AS\n", sql,
#     con = con
#   )
#   print(s)
#   s
# }

