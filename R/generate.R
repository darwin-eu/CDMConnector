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


#' Add a cohort table to a cdm object
#'
#' @description
#' This function creates an empty cohort table in a cdm and returns the cdm with the cohort table added.
#'
#' @param cdm A cdm reference created by CDMConnector. write_schema must be specified.
#' @param name Name of the cohort table to be created.
#' @param overwrite Should the cohort table be overwritten if it already exists? (TRUE or FALSE)
addCohortTable <- function(cdm,
                           name = "cohort",
                           overwrite = FALSE) {

  if(is.null(attr(cdm, "write_schema"))) stop("write_schema must be set in the cdm object")

  con <- attr(cdm, "dbcon")
  schema <- attr(cdm, "write_schema")

  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, min.chars = 1, null.ok = FALSE)
  checkmate::assertCharacter(name, len = 1, min.chars = 1, null.ok = FALSE)
  if (name != tolower(name)) rlang::abort("Cohort table names must be lowercase.")

  tables <- CDMConnector::listTables(con, schema = schema)

  if ((name %in% tables) && !overwrite) {
    rlang::abort(paste0("Table \"", name, "\" already exists. Set overwrite = TRUE to recreate it."))
  }

  sql <- "
      IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
      	DROP TABLE @cohort_database_schema.@cohort_table;

      CREATE TABLE @cohort_database_schema.@cohort_table (
      	cohort_definition_id BIGINT,
      	subject_id BIGINT,
      	cohort_start_date DATE,
      	cohort_end_date DATE
      );
  "
  cohort_database_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "write_schema")), sep = ".")
  sql <- SqlRender::render(sql = sql,
                           cohort_database_schema = cohort_database_schema,
                           cohort_table = name,
                           warnOnMissingParameters = TRUE
  )

  sql <- SqlRender::translate(sql = sql, targetDialect = CDMConnector::dbms(con))

  sqlStatements <- SqlRender::splitSql(sql)

  purrr::walk(sqlStatements, ~suppressMessages(DBI::dbExecute(attr(cdm, "dbcon"), .x, immediate = TRUE)))

  if (dbms(con) == "duckdb") {
    cdm[[name]] <- dplyr::tbl(con, paste(c(schema, name), collapse = "."))
  } else if (length(schema) == 2) {
    cdm[[name]] <- dplyr::tbl(con, dbplyr::in_catalog(schema[[1]], schema[[2]], name))
  } else if (length(schema) == 1) {
    cdm[[name]] <- dplyr::tbl(con, dbplyr::in_schema(schema, name))
  }

  invisible(cdm)
}


#' Generate a set of cohorts
#'
#' @description
#' This function generates a set of cohorts in the cohort table.
#' @param cdm cdm reference object
#'
#' @param cohortSet A cohort definition set dataframe
#' @param cohortTableName The name of the cohort table in the cdm. Defaults to 'cohort'.
#' @param overwrite Should the cohort table be overwritten if it already exists? TRUE or FALSE (default).
#'
#' @returns cdm reference object with the added cohort table containing generated cohorts
#'
#' @export
generateCohortSet <- function(cdm, cohortSet, cohortTableName = "cohort", overwrite = FALSE) {

  checkmate::assertDataFrame(cohortSet, min.rows = 0, col.names = "named")
  checkmate::assertNames(colnames(cohortSet), must.include = c("cohortId", "cohortName", "sql"))
  checkmate::assert_character(attr(cdm, "write_schema"), min.chars = 1, min.len = 1, max.len = 2)

  if (nrow(cohortSet) == 0) return(cdm)

  con <- attr(cdm, "dbcon")

  if (cohortTableName %in% CDMConnector::listTables(con, attr(cdm, "write_schema")) && !overwrite) {
    # ids <- as.numeric(dplyr::pull(cdm[[cohortTableName]], "cohort_definition_id"))
    # overlap <- dplyr::intersect(ids, cohortSet$cohortId)
    # if (length(overlap) > 0) {
    #   ids_chr <- paste(overlap, collapse = ", ")
    #   rlang::abort(glue::glue("Cohort definition IDs {ids_chr} already exist in {cohortTableName} table."))
    #   # add overwrite option?
    # }
    rlang::abort(glue::glue("The cohort table {cohortTableName} already exists.\nSpecify overwrite = TRUE to overwrite it."))
  } else {
    cdm <- addCohortTable(cdm, name = cohortTableName, overwrite = overwrite)
  }

  cli::cli_progress_bar(total = nrow(cohortSet),
                        format = "Generating cohorts {cli::pb_bar} {cli::pb_current}/{cli::pb_total}")

  cdm_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "cdm_schema")), sep = ".")
  write_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "write_schema")), sep = ".")
  target_cohort_table <- glue::glue_sql(DBI::dbQuoteIdentifier(con, cohortTableName))

  for (i in 1:nrow(cohortSet)) {

    sql <- cohortSet$sql[i] %>%
      SqlRender::render(
        cdm_database_schema = cdm_schema,
        vocabulary_database_schema = cdm_schema,
        target_database_schema = write_schema,
        results_database_schema = write_schema,
        target_cohort_table = target_cohort_table,
        target_cohort_id = cohortSet$cohortId[i],
        warnOnMissingParameters = FALSE
      ) %>%
      SqlRender::translate(
        targetDialect = CDMConnector::dbms(con),
        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
      ) %>%
      SqlRender::splitSql()

    purrr::walk(sql, ~DBI::dbExecute(con, .x, immediate = TRUE))
    cli::cli_progress_update()
  }
  cli::cli_progress_done()

  schema <- attr(cdm, "write_schema")

  if (dbms(con) == "duckdb") {
    cdm[[cohortTableName]] <- dplyr::tbl(con, paste(c(write_schema, cohortTableName), collapse = "."))
  } else if (length(schema) == 2) {
    cdm[[cohortTableName]] <- dplyr::tbl(con, dbplyr::in_catalog(schema[[1]], schema[[2]], cohortTableName))
  } else if (length(schema) == 1) {
    cdm[[cohortTableName]] <- dplyr::tbl(con, dbplyr::in_schema(schema, cohortTableName))
  }
  return(cdm)
}
