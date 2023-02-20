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


#' Generate a cohort set on a cdm object
#'
#' @description
#' A "GeneratedCohortSet" object consists of several components
#' \itemize{
#'   \item{A remote table reference to an OHDSI cohort table with at least
#'         the columns: cohort_definition_id, subject_id, cohort_start_date,
#'         cohort_end_date. Additional columns are optional and some analytic
#'         packages define additional columns specific to certain analytic
#'         cohorts.}
#'   \item{A **settings attribute** which points to a remote table containing
#'         cohort settings including the names of the cohorts.}
#'   \item{An **attrition attribute** which points to a remote table with
#'         attrition information recorded during generation. This attribute is
#'         optional. Since calculating attrition takes additional compute it
#'         can be skipped resulting in a NULL attrition attribute.}
#'   \item{A **cohortCounts attribute** which points to a remote table
#'         containing cohort counts}
#' }
#'
#' Each of the three attributes are tidy tables. The implementation of this
#' object is experimental and user feedback is welcome.
#'
#' `r lifecycle::badge("experimental")`
#'
#' One key design principle is that GeneratedCohortSet objects are created once
#' and can persist across analysis execution but should not be modified after
#' creation. While it is possible to modify a GeneratedCohortSet object doing
#' so will invalidate it and it's attributes may no longer be accurate.
#'
#' @param cdm A cdm reference created by CDMConnector. write_schema must be
#'   specified.
#' @param name Name of the cohort table to be created. This will also be used
#' as a prefix for the cohort attribute tables.
#' @param cohortSet A cohortSet object created with `readCohortSet()`.
#' @param computeAttrition Should attrition be computed? TRUE or FALSE.
#' @export
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con,
#'                     cdm_schema = "main",
#'                     cdm_tables = c(tbl_group("default")),
#'                     write_schema = "main")
#'
#' cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
#' cdm <- generateCohortSet(cdm, cohortSet, name = "cohort")
#'
#' print(cdm$cohort)
#'
#' attrition(cdm$cohort)
#' settings(cdm$cohort)
#' cohortCounts(cdm$cohort)
#' }
generateCohortSet <- function(cdm,
                                  cohortSet,
                                  name = "cohort",
                                  computeAttrition = FALSE,
                                  overwrite = TRUE) {

  rlang::check_installed("CirceR")
  rlang::check_installed("SqlRender")

  # test
  # devtools::load_all()
  # library(CDMConnector)
  # con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  # cdm <- cdm_from_con(con,
  #                     cdm_schema = "main",
  #                     cdm_tables = c(tbl_group("default")),
  #                     write_schema = "main")
  #
  # cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  # name = "cohort"
  # computeAttrition = T
  # overwrite = T

  # check inputs
  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assert_character(attr(cdm, "write_schema"),
                              min.chars = 1,
                              min.len = 1,
                              max.len = 2,
                              null.ok = FALSE)
  checkmate::assertDataFrame(cohortSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortSet),
                         must.include = c("cohortId", "cohortName", "cohort"))
  checkmate::assertCharacter(name, len = 1, min.chars = 1, null.ok = FALSE)
  checkmate::assertLogical(computeAttrition, len = 1)
  checkmate::assertLogical(overwrite, len = 1)
  checkmate::assert_true(DBI::dbIsValid(attr(cdm, "dbcon")))


  if (name != tolower(name)) {
    rlang::abort("Cohort table names must be lowercase.")
  }

  writeSchema <- attr(cdm, "write_schema")
  con <- attr(cdm, "dbcon")

  existingTables <- CDMConnector::listTables(con, writeSchema)

  if ((name %in% existingTables) && (!overwrite)) {
    rlang::abort(glue::glue("The cohort table {cohortTableName} already exists.
                            \nSpecify overwrite = TRUE to overwrite it."))
  }


  # Create the OHDSI-SQL for each cohort ----

  cohortSet$sql <- character(nrow(cohortSet))

  for (i in 1:nrow(cohortSet)) {
    cohortJson <- as.character(jsonlite::toJSON(cohortSet$cohort[[i]], auto_unbox = TRUE))
    cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
    cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                          options = CirceR::createGenerateOptions(
                                            cohortId = cohortSet$cohortId[i],
                                            cdmSchema =  glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "cdm_schema")), sep = "."),
                                            targetTable = name,
                                            resultSchema = glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "write_schema")), sep = "."),
                                            generateStats = computeAttrition))
    cohortSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
  }

  params <- unique(stringr::str_extract_all(cohortSet$sql, "@\\w+")[[1]])
  stopifnot(length(params) == 0)

  # Create the cohort tables ----

  # helper function that takes care of branching based on schema length
  # catalog is needed for sql server
  inSchema <- function(table) {
    if (length(writeSchema) == 2) {
      DBI::Id(catalog = writeSchema[1], schema = writeSchema[2], table = table)
    } else {
      stopifnot(length(writeSchema) == 1)
      DBI::Id(schema = writeSchema, table = table)
    }
  }

  if (name %in% existingTables) {
    DBI::dbRemoveTable(con, inSchema(name))
  }

  DBI::dbCreateTable(con,
                     name = inSchema(name),
                     fields = c(
                       cohort_definition_id = "INT",
                       subject_id = "INT",
                       cohort_start_date = "DATE",
                       cohort_end_date = "DATE"
                     ))

  stopifnot(name %in% listTables(con, writeSchema))

  if (computeAttrition) {

    nm <- paste0(name, "_inclusion")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         rule_sequence = "INT",
                         name = "VARCHAR(255)",
                         description = "VARCHAR(1000)")
    )

    nm <- paste0(name, "_inclusion_result")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         inclusion_rule_mask = "INT",
                         person_count = "INT",
                         mode_id = "INT")
    )

    nm <- paste0(name, "_inclusion_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         rule_sequence = "INT",
                         person_count = "INT",
                         gain_count = "INT",
                         person_total = "INT",
                         mode_id = "INT")
    )


    nm <- paste0(name, "_summary_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         base_count = "INT",
                         final_count = "INT",
                         mode_id = "INT")
    )

    nm <- paste0(name, "_censor_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         lost_count = "INT")
    )
  }

  # Run OHDSI-SQL ----

  cli::cli_progress_bar(total = nrow(cohortSet),
                        format = "Generating cohorts {cli::pb_bar} {cli::pb_current}/{cli::pb_total}")

  # cdm_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "cdm_schema")), sep = ".")
  # write_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "write_schema")), sep = ".")
  # target_cohort_table <- glue::glue_sql(DBI::dbQuoteIdentifier(con, cohortTableName))

  for (i in seq_len(nrow(cohortSet))) {

    sql <- cohortSet$sql[i] %>%
      # SqlRender::render(
      #   cdm_database_schema = cdm_schema,
      #   vocabulary_database_schema = cdm_schema,
      #   target_database_schema = write_schema,
      #   results_database_schema = write_schema,
      #   target_cohort_table = target_cohort_table,
      #   target_cohort_id = cohortSet$cohortId[i],
      #   warnOnMissingParameters = FALSE
      # ) %>%
      SqlRender::translate(
        targetDialect = CDMConnector::dbms(con),
        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
      ) %>%
      SqlRender::splitSql()

    purrr::walk(sql, ~DBI::dbExecute(con, .x, immediate = TRUE))
    cli::cli_progress_update()
  }
  cli::cli_progress_done()

  # Create the return object -----
  # if (dbms(con) == "duckdb") {
  # genearatedCohortSet <- dplyr::tbl(con, paste(c(writeSchema, name), collapse = "."))
  # } else if (length(schema) == 2) {
  #   genearatedCohortSet <- dplyr::tbl(con, dbplyr::in_catalog(schema[[1]],
  #                                                             schema[[2]],
  #                                                             name))
  # } else if (length(schema) == 1) {
  #   genearatedCohortSet <- dplyr::tbl(con, dbplyr::in_schema(schema, name))
  # } else {
  #   generatedCohortSet <- dplyr::tbl(con, inSchema(name)) # could this be used in all cases?
  # }

  generatedCohortSet <- dplyr::tbl(con, inSchema(name)) # could this be used in all cases?
  class(generatedCohortSet) <- c("generatedCohortSet", class(generatedCohortSet))

  # Create attrition attribute ----
  # TODO insert attrition code
  if (computeAttrition) {
    attr(generatedCohortSet, "attrition") <- dplyr::tbl(con, inSchema(paste0(name, "_inclusion_result")))
  } else {
    attr(generatedCohortSet, "attrition") <- NULL
  }

  # Create settings attribute -----
  DBI::dbWriteTable(con,
                    name = inSchema(paste0(name, "_settings")),
                    value = as.data.frame(cohortSet[,c("cohortId", "cohortName")]),
                    overwrite = TRUE)

  attr(generatedCohortSet, "settings") <- dplyr::tbl(con, inSchema(paste0(name, "_settings")))


  # Create CohortCounts attribute ----
  attr(generatedCohortSet, "cohortCounts") <- generatedCohortSet %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(cohort_entries = dplyr::n(),
                     cohort_subjects = dplyr::n_distinct(.data$subject_id)) %>%
    # TODO join with settings table to fill in zero counts
    # Can we just put counts in the settings table?
    computeQuery(name = paste0(name, "_count"),
                 schema = writeSchema,
                 temporary = FALSE,
                 overwrite = TRUE)

  cdm[[name]] <- generatedCohortSet

  cdm
}

#' Return attrition table from a generated cohort set object
#'
#' @export
attrition <- function(x) { UseMethod("attrition") }

#' @export
attrition.generatedCohortSet <- function(x) {
  attr(x, "attrition")
}


#' Return settings table
#'
#' @export
settings <- function(x) { UseMethod("settings") }

#' @export
settings.generatedCohortSet <- function(x) {
  attr(x, "settings")
}

#' Return cohortCounts table
#'
#' @export
cohortCounts <- function(x) { UseMethod("cohortCounts") }

#' @export
cohortCounts.generatedCohortSet <- function(x) {
  attr(x, "cohortCounts")
}

