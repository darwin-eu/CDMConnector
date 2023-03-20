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

#' Read a set of cohort definitions into R
#'
#' A "cohort set" is a collection of cohort definitions. In R this is stored in
#' a dataframe with cohort_definition_id, cohort_name, and cohort columns.
#' On disk this is stored as a folder with a CohortsToCreate.csv file and
#' one or more json files.
#' If the CohortsToCreate.csv file is missing then all of the json files in the
#' folder will be used, cohort_definition_id will be automatically assigned
#' in alphabetical order, and cohort_name will match the file names.
#'
#' @param path The path to a folder containing Circe cohort definition
#' json files and optionally a csv file named CohortsToCreate.csv with columns
#' cohortId, cohortName, and jsonPath.
#' @importFrom jsonlite read_json
#' @importFrom dplyr tibble
#' @export
read_cohort_set <- function(path) {
  checkmate::checkCharacter(path, len = 1, min.chars = 1)

  if (!fs::is_dir(path)) {
    rlang::abort(glue::glue("{path} is not a directory!"))
  }

  if (!dir.exists(path)) {
    rlang::abort(glue::glue("The directory {path} does not exist!"))
  }

  if (file.exists(file.path(path, "CohortsToCreate.csv"))) {
    cohortsToCreate <- readr::read_csv(file.path(path, "CohortsToCreate.csv"), show_col_types = FALSE) %>%
      dplyr::mutate(cohort = purrr::map(.data$jsonPath, jsonlite::read_json)) %>%
      dplyr::mutate(json = purrr::map(.data$jsonPath, readr::read_file)) %>%
      dplyr::mutate(cohort_definition_id = .data$cohortId, cohort_name = .data$cohortName)
  } else {
    jsonFiles <- sort(list.files(path, pattern = "\\.json$", full.names = TRUE))
    cohortsToCreate <- dplyr::tibble(
      cohort_definition_id = seq_along(jsonFiles),
      cohort_name = tools::file_path_sans_ext(basename(jsonFiles)),
      json_path = jsonFiles) %>%
      dplyr::mutate(cohort = purrr::map(.data$json_path, jsonlite::read_json)) %>%
      dplyr::mutate(json = purrr::map(.data$json_path, readr::read_file))
  }

  cohortsToCreate <- dplyr::select(cohortsToCreate, "cohort_definition_id", "cohort_name", "cohort", "json")
  class(cohortsToCreate) <- c("CohortSet", class(cohortsToCreate))
  return(cohortsToCreate)
}


#' @export
#' @rdname read_cohort_set
readCohortSet <- read_cohort_set


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
#' @param cohort_set,cohortSet A cohortSet object created with `readCohortSet()`.
#' @param compute_attrition,computeAttrition Should attrition be computed? TRUE or FALSE (default)
#' @param overwrite Should the cohort table be overwritten if it already
#' exists? TRUE or FALSE (default)
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
                              overwrite = FALSE) {
  rlang::check_installed("CirceR")
  rlang::check_installed("SqlRender")

  # check inputs ----
  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assert_character(attr(cdm, "write_schema"),
                              min.chars = 1,
                              min.len = 1,
                              max.len = 2,
                              null.ok = FALSE)
  checkmate::assertDataFrame(cohortSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortSet),
                         must.include = c("cohort_definition_id", "cohort_name", "cohort"))
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

  if ((name %in% existingTables) && isFALSE(overwrite)) {
    rlang::abort(glue::glue("The cohort table {name} already exists.
                            \nSpecify overwrite = TRUE to overwrite it."))
  }


  # Create the OHDSI-SQL for each cohort ----

  cohortSet$sql <- character(nrow(cohortSet))

  for (i in seq_len(nrow(cohortSet))) {
    # cohortJson <- as.character(jsonlite::toJSON(cohortSet$cohort[[i]], auto_unbox = TRUE))
    cohortJson <- cohortSet$json[[i]]
    cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
    cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                          options = CirceR::createGenerateOptions(
                                            generateStats = computeAttrition))
    cohortSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
  }

  # Create the cohort tables ----

  if (name %in% existingTables) {
    DBI::dbRemoveTable(con, inSchema(writeSchema, name))
  }

  DBI::dbCreateTable(con,
                     name = inSchema(writeSchema, name),
                     fields = c(
                       cohort_definition_id = "INT",
                       subject_id = "BIGINT",
                       cohort_start_date = "DATE",
                       cohort_end_date = "DATE"
                     ))

  stopifnot(name %in% listTables(con, writeSchema))

  if (computeAttrition) {

    nm <- paste0(name, "_inclusion")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(writeSchema, nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         rule_sequence = "INT",
                         name = "VARCHAR(255)",
                         description = "VARCHAR(1000)")
    )

    nm <- paste0(name, "_inclusion_result") # used for attrition

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(writeSchema, nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         inclusion_rule_mask = "INT",
                         person_count = "INT",
                         mode_id = "INT")
    )

    nm <- paste0(name, "_inclusion_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(writeSchema, nm),
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
      DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(writeSchema, nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         base_count = "INT",
                         final_count = "INT",
                         mode_id = "INT")
    )

    nm <- paste0(name, "_censor_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(writeSchema, nm),
                       fields = c(
                         cohort_definition_id = "INT",
                         lost_count = "INT")
    )
  }

  # Run the OHDSI-SQL ----

  cli::cli_progress_bar(
    total = nrow(cohortSet),
    format = "Generating cohorts {cli::pb_bar} {cli::pb_current}/{cli::pb_total}")

  cdm_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "cdm_schema")), sep = ".")
  write_schema <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, attr(cdm, "write_schema")), sep = ".")
  target_cohort_table <- glue::glue_sql(DBI::dbQuoteIdentifier(con, name))

  for (i in seq_len(nrow(cohortSet))) {

    sql <- cohortSet$sql[i] %>%
      SqlRender::render(
        cdm_database_schema = cdm_schema,
        vocabulary_database_schema = cdm_schema,
        target_database_schema = write_schema,
        results_database_schema.cohort_inclusion = paste0(write_schema, ".", name, "_inclusion"),
        results_database_schema.cohort_inclusion_result = paste0(write_schema, ".", name, "_inclusion_result"),
        results_database_schema.cohort_summary_stats = paste0(write_schema, ".", name, "_summary_stats"),
        results_database_schema.cohort_censor_stats = paste0(write_schema, ".", name, "_censor_stats"),
        results_database_schema.cohort_inclusion = paste0(write_schema, ".", name, "_inclusion"),
        target_cohort_table = target_cohort_table,
        target_cohort_id = cohortSet$cohort_definition_id[i],
        warnOnMissingParameters = FALSE
      )

    stopifnot(length(unique(stringr::str_extract_all(sql, "@\\w+"))[[1]]) == 0)

    sql <- SqlRender::translate(sql, targetDialect = CDMConnector::dbms(con)) %>%
      SqlRender::splitSql()

    purrr::walk(sql, ~DBI::dbExecute(con, .x, immediate = TRUE))
    cli::cli_progress_update()
  }
  cli::cli_progress_done()

  cohort_ref <- dplyr::tbl(con, inSchema(writeSchema, name))

  # Create attrition attribute ----
  if (computeAttrition) {
    cohort_attrition_ref <- computeAttritionTable(cdm,
                                                  cohortStem = name,
                                                  cohortSet = cohortSet,
                                                  overwrite = overwrite)
  } else {
    cohort_attrition_ref <- NULL
  }

  # Create cohort_set attribute -----
  DBI::dbWriteTable(con,
                    name = inSchema(writeSchema, paste0(name, "_set")),
                    value = as.data.frame(cohortSet[,c("cohort_definition_id", "cohort_name")]),
                    overwrite = TRUE)

  cohort_set_ref <- dplyr::tbl(con, inSchema(writeSchema, paste0(name, "_set")))

  # Create cohort_count attribute ----
  cohort_count_ref <- cohort_ref %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(number_records = dplyr::n(),
                     number_subjects = dplyr::n_distinct(.data$subject_id)) %>%
    {dplyr::left_join(cohort_set_ref, ., by = "cohort_definition_id")} %>%
    dplyr::mutate(number_records  = dplyr::coalesce(.data$number_records, 0L),
                  number_subjects = dplyr::coalesce(.data$number_subjects, 0L)) %>%
    dplyr::select("cohort_definition_id",
                  "number_records",
                  "number_subjects") %>%
    computeQuery(name = paste0(name, "_count"),
                 schema = attr(cdm, "write_schema"),
                 temporary = FALSE,
                 overwrite = TRUE)

  # Clean up tables ---- TODO decide how to handle this
  if (FALSE) {
    DBI::dbRemoveTable(con, inSchema(writeSchema, paste0(name, "_inclusion")))
    DBI::dbRemoveTable(con, inSchema(writeSchema, paste0(name, "_inclusion_result")))
    DBI::dbRemoveTable(con, inSchema(writeSchema, paste0(name, "_inclusion_stats")))
    DBI::dbRemoveTable(con, inSchema(writeSchema, paste0(name, "_summary_stats")))
    DBI::dbRemoveTable(con, inSchema(writeSchema, paste0(name, "_censor_stats")))
  }

  # Create the object. Let the constructor handle getting the counts.----
  cdm[[name]] <- new_generated_cohort_set(
    cohort_ref = cohort_ref,
    cohort_set_ref = cohort_set_ref,
    cohort_attrition_ref = cohort_attrition_ref,
    cohort_count_ref = cohort_count_ref)

  return(cdm)
}


#' @rdname generateCohortSet
#' @export
generate_cohort_set <- function(cdm,
                                cohort_set,
                                name = "cohort",
                                compute_attrition = FALSE,
                                overwrite = FALSE) {
  generateCohortSet(cdm = cdm,
                    cohortSet = cohort_set,
                    name = name,
                    computeAttrition = compute_attrition,
                    overwrite = overwrite)

}


#' Low level constructor for GeneratedCohortSet objects for package developers
#'
#' This constructor function is to be used by analytic package developers to
#' create `generatedCohortSet` objects. Users should never need to call this
#' function. The use of this function ensures that all `generatedCohortSet`
#' have a valid structure.
#'
#' @details
#' A `generatedCohort` is a set of person-time from an OMOP CDM database.
#' A `generatedCohort` can be represented by a table with three columns:
#' subject_id, cohort_start_date, cohort_end_date. Subject_id is the same as
#' person_id in the OMOP CDM. A `generatedCohortSet` is a collection of one
#' or more `generatedCohorts` and can be represented as a table with four
#' columns: cohort_definition_id, subject_id, cohort_start_date,
#' cohort_end_date.
#'
#' This constructor function defines the `generatedCohortSet` object in R.
#'
#' The object is an extension of a `tbl_sql` object defined in dplyr. This is
#' a lazy database query that points to a cohort table in the database with
#' at least the columns cohort_definition_id, subject_id, cohort_start_date,
#' cohort_end_date. The table could optionally have more columns as well.
#'
#' In addition the `generatedCohortSet` object has three optional attributes.
#' These are: cohort_set, cohort_attrition, cohort_count.
#' Each of these attributes is also a lazy SQL query (`tbl_sql`) that points
#' to a table in a database and is described below.
#'
#' ## cohort_set
#'
#' cohort_set is a table with one row per cohort_definition_id. The first
#' two columns of the cohort_set table are: cohort_definition_id, and
#' cohort_name. Additional columns can be added. The cohort_set table is meant
#' to store metadata about the cohort definition.
#'
#' ## cohort_attrition
#'
#' cohort_attrition is an optional table that stores attrition information
#' recorded during the cohort generation process such as how many persons were
#' dropped at each step of inclusion rule application. The first column of this
#' table should be `cohort_definition_id` but all other columns currently
#' have no constraints.
#'
#' ## cohort_count
#'
#' cohort_count is a option attribute table that records the number of records
#' and the number of unique persons in each cohort in a `generatedCohortSet`.
#' It is derived metadata that can be re-derived as long as cohort_set,
#' the complete list of cohorts in the set, is available. Column names of
#' cohort_count are: cohort_definition_id, number_records,
#' number_subjects.
#'
#' @param cohort_ref,cohortRef A `tbl_sql` object that points to a remote cohort table
#' with the following first four columns: cohort_definition_id,
#' subject_id, cohort_start_date, cohort_end_date. Additional columns are
#' optional.
#' @param cohort_set_ref,cohortSetRef A `tbl_sql` object that points to a remote table
#' with the following first two columns: cohort_definition_id, cohort_name.
#' Additional columns are optional. cohort_definition_id should be a primary
#' key on this table and uniquely identify rows.
#' @param cohort_attrition_ref,cohortAttritionRef A `tbl_sql` object that points to an attrition
#' table in a remote database with the first column being cohort_definition_id.
#' @param cohort_count_ref,cohortCountRef A `tbl_sql` object that points to a cohort_count
#' table in a remote database with columns cohort_definition_id, cohort_entries,
#' cohort_subjects.
#'
#' @return A `generatedCohortSet` object that is a `tbl_sql` reference
#' to a cohort table in the write_schema of an OMOP CDM
#' @export
#'
#' @examples
#' \dontrun{
#'  # This function is for developers who are creating generatedCohortSet
#'  # objects in their packages. The function should accept a cdm_reference
#'  # object as the first argument and return a cdm_reference object with the
#'  # cohort table added. The second argument should be `name` which will be
#'  # the prefix for the database tables, the name of the cohort table in the
#'  # database and the name of the cohort table in the cdm object.
#'  # Other optional arguments can be added after the first two.
#'
#'  generateCustomCohort <- function(cdm, name, ...) {
#'
#'    # accept a cdm_reference object as input
#'    checkmate::assertClass(cdm, "cdm_reference")
#'    con <- attr(cdm, "dbcon")
#'
#'    # Create the tables in the database however you like
#'    # All the tables should be prefixed with `name`
#'    # The cohort table should be called `name` in the database
#'
#'    # Create the dplyr table references
#'    cohort_ref <- dplyr::tbl(con, name)
#'    cohort_set <- dplyr::tbl(con, paste0(name, "_set"))
#'    cohort_attrition_ref <- dplyr::tbl(con, paste0(name, "_attrition"))
#'    cohort_count_ref <- dplyr::tbl(con, paste0(name, "_count"))
#'
#'    # create the generated cohort set object using the constructor
#'    generatedCohortSet <- new_generated_cohort_set(
#'       cohort_ref,
#'       cohort_set_ref = cohort_set_ref,
#'       cohort_attrition_ref = cohort_attrition_ref,
#'       cohort_count_ref = cohort_count_ref)
#'
#'.   # Add the generatedCohortSet to the cdm and return the cdm
#'    cdm[[name]] <- generatedCohortSet
#'    return(cdm)
#'  }
#' }
new_generated_cohort_set <- function(cohort_ref,
                                     cohort_set_ref = NULL,
                                     cohort_attrition_ref = NULL,
                                     cohort_count_ref = NULL) {

  checkmate::assertClass(cohort_ref, classes = c("tbl_sql"), null.ok = FALSE)
  checkmate::assertClass(cohort_set_ref, classes = c("tbl_sql"), null.ok = TRUE)
  checkmate::assertClass(cohort_attrition_ref, classes = c("tbl_sql"), null.ok = TRUE)
  checkmate::assertClass(cohort_count_ref, classes = c("tbl_sql"), null.ok = TRUE)

  checkmate::assertSubset(c("cohort_definition_id", "subject_id",
                            "cohort_start_date", "cohort_end_date"),
                          choices = names(cohort_ref))

  if (!is.null(cohort_set_ref)) {
    stopifnot(colnames(cohort_set_ref)[1] == "cohort_definition_id",
              colnames(cohort_set_ref)[2] == "cohort_name")

    # primary key check
    one <- cohort_set_ref %>%
      dplyr::count(.data$cohort_definition_id, name = "one") %>%
      dplyr::count(.data$one) %>%
      dplyr::pull("one")

    if (!(length(one) == 1 && one == 1)) {
      rlang::abort("cohort_definition_id is not a primary key on cohort_set_ref")
    }
  }

  if (!is.null(cohort_attrition_ref)) {
    checkmate::assertSubset(c("cohort_definition_id"),
                            choices = names(cohort_attrition_ref))
  }

  if (!is.null(cohort_count_ref)) {
    checkmate::assertSubset(c("cohort_definition_id",
                              "number_records",
                              "number_subjects"),
                            choices = names(cohort_count_ref))
  }

  # create the object
  class(cohort_ref) <- c("GeneratedCohortSet", class(cohort_ref))
  attr(cohort_ref, "cohort_set") <- cohort_set_ref
  attr(cohort_ref, "cohort_attrition") <- cohort_attrition_ref
  attr(cohort_ref, "cohort_count") <- cohort_count_ref

  return(cohort_ref)
}


#' @rdname new_generated_cohort_set
#' @export
newGeneratedCohortSet <- function(cohortRef,
                                  cohortSetRef = NULL,
                                  cohortAttritionRef = NULL,
                                  cohortCountRef = NULL) {
  new_generated_cohort_set(cohort_ref = cohortRef,
                           cohort_set_ref = cohortSetRef,
                           cohort_attrition_ref = cohortAttritionRef,
                           cohort_count_ref = cohortCountRef
                           )
}


#' Get attrition table from a GeneratedCohortSet object
#'
#' @param x A generatedCohortSet object
#'
#' @export
cohortAttrition <- function(x) { UseMethod("cohortAttrition") }


#' @rdname cohortAttrition
#' @export
cohort_attrition <- cohortAttrition


#' @export
cohortAttrition.GeneratedCohortSet <- function(x) {
  attr(x, "cohort_attrition")
}

#' Get cohort settings from a GeneratedCohortSet object
#'
#' @param x A generatedCohortSet object
#'
#' @export
cohortSet <- function(x) { UseMethod("cohortSet") }


#' @rdname cohortSet
#' @export
cohort_set <- cohortSet


#' @export
cohortSet.GeneratedCohortSet <- function(x) {
  attr(x, "cohort_set")
}

#' Get cohort counts from a GeneratedCohortSet object
#'
#' @param x A generatedCohortSet object
#'
#' @export
cohortCount <- function(x) { UseMethod("cohortCount") }


#' @rdname cohortCount
#' @export
cohort_count <- cohortCount


#' @export
cohortCount.GeneratedCohortSet <- function(x) {
  attr(x, "cohort_count")
}


# Compute the attrition for a set of cohorts (internal function)
#
# @description This function computes the attrition for a set of cohorts. It
# uses the inclusion_result table so the cohort should be previously generated
# using stats = TRUE.
#
# @param cdm A cdm reference created by CDMConnector.
# @param cohortStem Stem for the cohort tables.
# @param cohortSet Cohort set of the generated tables.
# @param cohortId Cohort definition id of the cohorts that we want to generate
# the attrition. If NULL all cohorts from cohort set will be used.
# @param overwrite Should the attrition table be overwritten if it already exists? TRUE or FALSE
#
# @importFrom rlang :=
# @return the attrition as a data.frame
computeAttritionTable <- function(cdm,
                                  cohortStem,
                                  cohortSet,
                                  cohortId = NULL,
                                  overwrite = FALSE) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(cohortStem, len = 1, min.chars = 1)
  checkmate::assertLogical(overwrite, len = 1)
  checkmate::assertDataFrame(cohortSet, min.rows = 0, col.names = "named")
  checkmate::assertNames(colnames(cohortSet),
    must.include = c("cohort_definition_id", "cohort")
  )
  if (is.null(cohortId)) {
    cohortId <- cohortSet$cohort_definition_id
  }
  checkmate::assertNumeric(cohortId, any.missing = FALSE, min.len = 1)
  checkmate::assertTRUE(all(cohortId %in% cohortSet$cohort_definition_id))

  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  inclusionResultTableName <- paste0(cohortStem, "_inclusion_result")
  schema <- attr(cdm, "write_schema")
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, min.chars = 1)

  if (paste0(cohortStem, "_attrition") %in% listTables(con, schema = schema)) {
    if (overwrite) {
      DBI::dbRemoveTable(con, inSchema(schema, paste0(cohortStem, "_attrition")))
    } else {
      rlang::abort(paste0(cohortStem, "_attrition already exists in the database. Set overwrite = TRUE."))
    }
  }

  # Bring the inclusion result table to R memory
  inclusionResult <- dplyr::tbl(con, inSchema(schema, inclusionResultTableName, dbms(con))) %>%
    dplyr::collect() %>%
    dplyr::mutate(inclusion_rule_mask = as.numeric(.data$inclusion_rule_mask))

  attritionList <- list()

  for (i in seq_along(cohortId)) {

    id <- cohortId[i]
    inclusionName <-  purrr::map_chr(cohortSet$cohort[i]$InclusionRules, "name")

    numberInclusion <- length(inclusionName)
    if (numberInclusion == 0) {
      #cohortTableName <- paste0(cohortStem, "_cohort")
      cohortTableName <- cohortStem
      attrition <- dplyr::tibble(
        cohort_definition_id = id,
        number_records = dplyr::tbl(con, inSchema(schema, cohortTableName, dbms(con))) %>%
          dplyr::filter(.data$cohort_definition_id == id) %>%
          dplyr::tally() %>%
          dplyr::pull("n") %>%
          as.numeric(),
        number_subjects = dplyr::tbl(con, inSchema(schema, cohortTableName, dbms(con))) %>%
          dplyr::filter(.data$cohort_definition_id == id) %>%
          dplyr::select("subject_id") %>%
          dplyr::distinct() %>%
          dplyr::tally() %>%
          dplyr::pull("n") %>%
          as.numeric(),
        reason_id = 1,
        reason = "Qualifying initial records",
        excluded_records = 0,
        excluded_subjects = 0
      )
    } else {
      inclusionMaskId <- getInclusionMaskId(numberInclusion)
      inclusionName <- c("Qualifying initial records", inclusionName)
      attrition <- list()
      for (k in 1:(numberInclusion + 1)) {
        attrition[[k]] <- dplyr::tibble(
          cohort_definition_id = id,
          number_records = inclusionResult %>%
            dplyr::filter(.data$mode_id == 0) %>%
            dplyr::filter(.data$inclusion_rule_mask %in% inclusionMaskId[[k]]) %>%
            dplyr::pull("person_count") %>%
            base::sum() %>%
            as.numeric(),
          number_subjects = inclusionResult %>%
            dplyr::filter(.data$mode_id == 1) %>%
            dplyr::filter(.data$inclusion_rule_mask %in% inclusionMaskId[[k]]) %>%
            dplyr::pull("person_count") %>%
            base::sum() %>%
            as.numeric(),
          reason_id = k,
          reason = inclusionName[k]
        )
      }
      attrition <- attrition %>%
        dplyr::bind_rows() %>%
        dplyr::mutate(
          excluded_records =
            dplyr::lag(.data$number_records, 1, order_by = .data$reason_id) -
            .data$number_records
        ) %>%
        dplyr::mutate(
          excluded_subjects =
            dplyr::lag(.data$number_subjects, 1, order_by = .data$reason_id) -
            .data$number_subjects
        ) %>%
        dplyr::mutate(excluded_records = dplyr::coalesce(.data$excluded_records, 0),
                      excluded_subjects = dplyr::coalesce(.data$excluded_subjects, 0))
    }
    attritionList[[i]] <- attrition
  }

  attrition <- attritionList %>%
    dplyr::bind_rows()

  # upload attrition table to database
  DBI::dbWriteTable(con,
                    name = inSchema(schema, paste0(cohortStem, "_attrition")),
                    value = attrition)

  dplyr::tbl(con, inSchema(schema, paste0(cohortStem, "_attrition"), dbms(con)))
}

getInclusionMaskId <- function(numberInclusion) {

  inclusionMaskMatrix <- dplyr::tibble(
    inclusion_rule_mask = 0:(2^numberInclusion - 1)
  )

  for (k in 0:(numberInclusion - 1)) {
    inclusionMaskMatrix <- inclusionMaskMatrix %>%
      dplyr::mutate(!!paste0("inclusion_", k) :=
          rep(c(rep(0, 2^k), rep(1, 2^k)), 2^(numberInclusion - k - 1))
      )
  }

  lapply(-1:(numberInclusion - 1), function(x) {
    if (x == -1) {
      return(inclusionMaskMatrix$inclusion_rule_mask)
    } else {
      inclusionMaskMatrix <- inclusionMaskMatrix
      for (k in 0:x) {
        inclusionMaskMatrix <- inclusionMaskMatrix %>%
          dplyr::filter(.data[[paste0("inclusion_", k)]] == 1)
      }
      return(inclusionMaskMatrix$inclusion_rule_mask)
    }
  })
}
