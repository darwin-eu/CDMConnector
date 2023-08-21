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
      dplyr::mutate(jsonPath = file.path(path, .data$jsonPath)) %>%
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
#' A "GeneratedCohortSet" object consists of four components
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
#' @param cohort_set,cohortSet Can be a cohortSet object created with `readCohortSet()`,
#' a single Capr cohort definition,
#' or a named list of Capr cohort definitions.
#' @param compute_attrition,computeAttrition Should attrition be computed? TRUE (default) or FALSE
#' @param overwrite Should the cohort table be overwritten if it already
#' exists? TRUE or FALSE (default)
#' @export
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con,
#'                     cdm_schema = "main",
#'                     write_schema = "main")
#'
#' cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
#' cdm <- generateCohortSet(cdm, cohortSet, name = "cohort")
#'
#' print(cdm$cohort)
#'
#' cohortAttrition(cdm$cohort)
#' cohortSet(cdm$cohort)
#' cohortCount(cdm$cohort)
#' }
generateCohortSet <- function(cdm,
                              cohortSet,
                              name = "cohort",
                              computeAttrition = TRUE,
                              overwrite = FALSE) {

  rlang::check_installed("CirceR")
  rlang::check_installed("SqlRender")

  checkmate::assertClass(cdm, "cdm_reference")
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(con))

  withr::local_options(list("cli.progress_show_after" = 0))
  # cli::cli_progress_bar(
    # total = nrow(cohortSet),
    # format = "Generating cohorts {cli::pb_bar} {cli::pb_current}/{cli::pb_total}")
  # cli::cli_progress_update(set = 0)

  assert_write_schema(cdm) # required for now

  checkmate::assertLogical(computeAttrition, len = 1)
  checkmate::assertLogical(overwrite, len = 1)

  write_schema <- attr(cdm, "write_schema")
  checkmate::assert_character(write_schema,
                              min.chars = 1,
                              min.len = 1,
                              max.len = 3,
                              null.ok = FALSE)

  if ("prefix" %in% names(write_schema)) {
    prefix <- unname(write_schema["prefix"])
    # write_schema <- write_schema[-which(names(write_schema) == "prefix")]
  } else {
    prefix <- ""
  }

  if (!is.data.frame(cohortSet)) {
    if (!is.list(cohortSet)) {
      rlang::abort("cohortSet must be a dataframe or a named list of Capr cohort definitions")
    }

    checkmate::assertList(cohortSet,
                          types = "Cohort",
                          min.len = 1,
                          names = "strict",
                          any.missing = FALSE)

    cohortSet <- dplyr::tibble(
      cohort_definition_id = seq_along(cohortSet),
      cohort_name = names(cohortSet),
      cohort = purrr::map(cohortSet, ~jsonlite::fromJSON(generics::compile(.), simplifyVector = FALSE)),
      json = purrr::map_chr(cohortSet, generics::compile)
    )
    class(cohortSet) <- c("CohortSet", class(cohortSet))
  }

  checkmate::assertDataFrame(cohortSet, min.rows = 1, col.names = "named")

  # Handle OHDSI cohort sets
  if ("cohortId" %in% names(cohortSet) && !("cohort_definition_id" %in% names(cohortSet))) {
    cohortSet$cohort_definition_id <- cohortSet$cohortId
  }

  if ("cohortName" %in% names(cohortSet) && !("cohort_name" %in% names(cohortSet))) {
    cohortSet$cohort_name <- cohortSet$cohortName
  }

  if (!("cohort" %in% names(cohortSet)) && ("json" %in% names(cohortSet))) {
    cohortColumn <- list()
    for (i in seq_len(nrow(cohortSet))) {
      x <- cohortSet$json[i]
      if (!validUTF8(x)) { x <- stringi::stri_enc_toutf8(x, validate = TRUE) }
      if (!validUTF8(x)) { rlang::abort("Failed to convert json UTF-8 encoding") }
      cohortColumn[[i]] <- jsonlite::fromJSON(x, simplifyVector = FALSE)
    }
    cohortSet$cohort <- cohortColumn
  }

  checkmate::assertTRUE(all(c("cohort_definition_id", "cohort_name", "json") %in% colnames(cohortSet)))

  # check name -----
  checkmate::assertCharacter(name, len = 1, min.chars = 1, pattern = "[a-z_]+")

  if (paste0(prefix, name) != tolower(paste0(prefix, name))) {
    rlang::abort(glue::glue("Cohort table name `{paste0(prefix, name)}` must be lowercase!"))
  }

  existingTables <- CDMConnector::listTables(con, write_schema)

  if ((paste0(prefix, name) %in% existingTables) && isFALSE(overwrite)) {
    rlang::abort(glue::glue("The cohort table {paste0(prefix, name)} already exists.
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

    # if (dbms(con) == "snowflake") {
      # a hack because we are using lowercase column names for cohort tables
      # q <- function(x) DBI::dbQuoteIdentifier(con, x)
      # r <- "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;"
      # r2 <- glue::glue("DELETE FROM @target_database_schema.@target_cohort_table where {q('cohort_definition_id')} = @target_cohort_id;")
      # if (!stringr::str_detect(cohortSet$sql[i], r)) rlang::abort("critical SQL replacement failed!")
      # cohortSet$sql[i] <- stringr::str_replace(cohortSet$sql[i], r, r2)
      #
      # if (computeAttrition) {
      #   r <- "delete from @results_database_schema.cohort_censor_stats where cohort_definition_id = @target_cohort_id;"
      #   r2 <- glue::glue("delete from @results_database_schema.cohort_censor_stats where {q('cohort_definition_id')} = @target_cohort_id;")
      #   if (!stringr::str_detect(cohortSet$sql[i], r)) rlang::abort("critical SQL replacement failed!")
      #   cohortSet$sql[i] <- stringr::str_replace(cohortSet$sql[i], r, r2)
      # }
      #
      # r <- "INSERT INTO @target_database_schema.@target_cohort_table \\(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date\\)"
      # r2 <- "INSERT INTO @target_database_schema.@target_cohort_table "
      # if (!stringr::str_detect(cohortSet$sql[i], r)) rlang::abort("critical SQL replacement failed!")
      # cohortSet$sql[i] <- stringr::str_replace(cohortSet$sql[i], r, r2)

      # r <- "select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date "
      # r2 <- glue::glue("select @target_cohort_id as {q('cohort_definition_id')}, person_id, start_date, end_date ")
      # if (!stringr::str_detect(cohortSet$sql[i], r)) rlang::abort("critical SQL replacement failed!")
      # cohortSet$sql[i] <- stringr::str_replace(cohortSet$sql[i], r, r2)
    # }
  }
# cat(cohortSet$sql[1])
  # Create the cohort tables ----
  createCohortTables(con, write_schema, name, computeAttrition)
  # Run the OHDSI-SQL ----

  cdm_schema <- attr(cdm, "cdm_schema")
  checkmate::assertCharacter(cdm_schema, max.len = 3, min.len = 1, min.chars = 1)
  if ("prefix" %in% names(cdm_schema)) {
    cdm_schema_sql <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, cdm_schema[-which(names(cdm_schema) == "prefix")]), sep = ".")
  } else {
    cdm_schema_sql <- glue::glue_sql_collapse(DBI::dbQuoteIdentifier(con, cdm_schema), sep = ".")
  }

  if ("prefix" %in% names(write_schema)) {
    write_schema_sql <- paste(DBI::dbQuoteIdentifier(con, write_schema[-which(names(write_schema) == "prefix")]), collapse = ".")
  } else {
    write_schema_sql <- paste(DBI::dbQuoteIdentifier(con, write_schema), collapse = ".")
  }

  # if (!(dbms(con) %in% c("snowflake", "oracle"))) {
    # target_cohort_table <- paste0(prefix, name)
  # } else {
    # target_cohort_table <- paste0(prefix, name)
  # }

  dropTempTableIfExists <- function(con, table) {
    suppressMessages(
      DBI::dbExecute(
        con,
        SqlRender::translate(
          glue::glue("IF OBJECT_ID('#{table}', 'U') IS NOT NULL DROP TABLE #{table};"),
          targetDialect = dbms(con))
        )
    )
  }

  for (i in seq_len(nrow(cohortSet))) {
    # Note: you cannot quote the auxiliary cohort table names because it will end up
    # generating sql like this: delete from "ATLAS"."RESULTS"."chrt0_inclusion"_stats where cohort_definition_id = 3
    # which fails
    sql <- cohortSet$sql[i] %>%
      SqlRender::render(
        cdm_database_schema = cdm_schema_sql,
        vocabulary_database_schema = cdm_schema_sql,
        target_database_schema = write_schema_sql,
        results_database_schema.cohort_inclusion        = paste0(write_schema_sql, ".", prefix, name, "_inclusion"),
        results_database_schema.cohort_inclusion_result = paste0(write_schema_sql, ".", prefix, name, "_inclusion_result"),
        results_database_schema.cohort_summary_stats    = paste0(write_schema_sql, ".", prefix, name, "_summary_stats"),
        results_database_schema.cohort_censor_stats     = paste0(write_schema_sql, ".", prefix, name, "_censor_stats"),
        results_database_schema.cohort_inclusion        = paste0(write_schema_sql, ".", prefix, name, "_inclusion"),
        target_cohort_table = paste0(prefix, name),
        target_cohort_id = cohortSet$cohort_definition_id[i],
        warnOnMissingParameters = FALSE
      )

    stopifnot(length(unique(stringr::str_extract_all(sql, "@\\w+"))[[1]]) == 0)

    if (dbms(con) == "spark") {
      # remove comments from SQL which are causing an issue on spark
      # --([^\n])*?\n => match strings starting with -- followed by anything except a newline
      sql <- stringr::str_replace_all(sql, "--([^\n])*?\n", "\n")
    }

    tempEmulationSchema <- getOption("sqlRenderTempEmulationSchema") %||% write_schema_sql

    sql <- SqlRender::translate(sql,
                                targetDialect = CDMConnector::dbms(con),
                                tempEmulationSchema = tempEmulationSchema) %>%
      SqlRender::splitSql()

    if (dbms(con) == "duckdb") {
      # hotfix for duckdb sql translation https://github.com/OHDSI/SqlRender/issues/340
      sql <- gsub("'-1 \\* (\\d+) day'", "'-\\1 day'", sql)
    }

    if (!(dbms(con) %in% c("snowflake", "oracle", "bigquery"))) {
      # TODO: issue dropping temp tables on dbms which are using tempEmulation
      # Error: nanodbc/nanodbc.cpp:1526: 00000: Cannot perform DROP.
      # This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name.
      # SqlRender::getTempTablePrefix()
      dropTempTableIfExists(con, "Codesets")
      dropTempTableIfExists(con, "qualified_events")
      dropTempTableIfExists(con, "cohort_rows")
      dropTempTableIfExists(con, "Inclusion")
      dropTempTableIfExists(con, "inclusion_events")
      dropTempTableIfExists(con, "included_events")
      dropTempTableIfExists(con, "final_cohort")
    } else if (dbms(con) == "bigquery") {
      tables <- c("codesets", "qualified_events", "cohort_rows", "inclusion", "inclusion_events", "included_events", "final_cohort")
      sql <- glue::glue("drop table if exists {write_schema}.{SqlRender::getTempTablePrefix()}{tables}")
      purrr::walk(sql, ~DBI::dbExecute(con, .))
    }

    for (j in seq_along(sql)) {
      DBI::dbExecute(con, sql[j], immediate = TRUE)
    }
    # cli::cli_progress_update(set = i)
  }

  if (dbms(con) %in% c("snowflake", "oracle")) {
    # make table lowercase

    cohort_ref <- dplyr::tbl(con, inSchema(write_schema, toupper(name), dbms(con))) %>%
      dplyr::rename_all(tolower) %>%
      # compute_query() %>%
      compute_query(
        name = name,
        temporary = FALSE,
        schema = write_schema,
        overwrite = TRUE
      )
  } else {
    cohort_ref <- dplyr::tbl(con, inSchema(write_schema, name, dbms = dbms(con)))
  }

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
  if (paste0(name, "_set") %in% existingTables) {
    DBI::dbRemoveTable(con, inSchema(write_schema, paste0(name, "_set"), dbms = dbms(con)))
  }

  # overwrite not working on snowflake
  DBI::dbWriteTable(con,
                    name = inSchema(write_schema, paste0(name, "_set"), dbms(con)),
                    value = as.data.frame(cohortSet[,c("cohort_definition_id", "cohort_name")]),
                    overwrite = TRUE)

  cohort_set_ref <- dplyr::tbl(con, inSchema(write_schema, paste0(name, "_set"), dbms(con)))

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

  # cohort_ref must be a cdm table before it is passed in to new_generated_cohort_set.
  cdm[[name]] <- cohort_ref

  # Create the object. Let the constructor handle getting the counts.----
  cdm[[name]] <- new_generated_cohort_set(
    cohort_ref = cdm[[name]],
    cohort_set_ref = cohort_set_ref,
    cohort_attrition_ref = cohort_attrition_ref,
    cohort_count_ref = cohort_count_ref)

  cli::cli_progress_done()
  return(cdm)
}


#' @rdname generateCohortSet
#' @export
generate_cohort_set <- function(cdm,
                                cohort_set,
                                name = "cohort",
                                compute_attrition = TRUE,
                                overwrite = FALSE) {
  generateCohortSet(cdm = cdm,
                    cohortSet = cohort_set,
                    name = name,
                    computeAttrition = compute_attrition,
                    overwrite = overwrite)

}


#' Constructor for GeneratedCohortSet objects
#'
#' This constructor function is to be used by analytic package developers to
#' create `generatedCohortSet` objects.
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
#' to store metadata about the cohort definition. Since this table is required it
#' will be created if it it is not supplied.
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
#' number_subjects. This table is required for generatedCohortSet objects and
#' will be created if not supplied.
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
#' @param overwrite Should tables be overwritten if they already exist? TRUE or FALSE (default)
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
#'    # add to the cdm
#'    cdm[[name]] <- cohort_ref
#'
#'    # create the generated cohort set object using the constructor
#'    cdm[[name]] <- new_generated_cohort_set(
#'       cdm[[name]],
#'       cohort_set_ref = cohort_set_ref,
#'       cohort_attrition_ref = cohort_attrition_ref,
#'       cohort_count_ref = cohort_count_ref)
#'
#'    return(cdm)
#'  }
#' }
new_generated_cohort_set <- function(cohort_ref,
                                     cohort_set_ref = NULL,
                                     cohort_attrition_ref = NULL,
                                     cohort_count_ref = NULL,
                                     overwrite = FALSE) {

  if (!methods::is(cohort_ref, "tbl_sql")) {
    rlang::abort("cohort_ref must be a remote database table reference (tbl_sql)")
  }

  q <- cohort_ref %>%
    dbplyr::sql_render() %>%
    as.character()

  if (stringr::str_detect(q, "\\(") || stringr::str_detect(q, "^SELECT *", negate = TRUE)) {
    rlang::abort("cohort_ref needs to be a computed table in the database. \nUse `compute_query()` before passing cohort_ref into `new_generated_cohort_set()`")
  }

  con <- cohort_ref[[1]]$con
  checkmate::assertTRUE(DBI::dbIsValid(con))
  cdm <- attr(cohort_ref, "cdm_reference")
  if (is.null(cdm)) rlang::abort("cohort_ref must be part of a cdm!")
  write_schema <- attr(cdm, "write_schema")
  if (is.null(write_schema)) rlang::abort("cohort_ref must be part of a cdm with a write_schema!")
  checkmate::assert_character(write_schema, min.len = 1, max.len = 3, min.chars = 1)
  verify_write_access(con, write_schema = write_schema)

  # cohort table ----
  {
    expected_columns <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
    actual_columns <- tolower(colnames(cohort_ref))[1:4]
    if (!dplyr::setequal(expected_columns, actual_columns)) {
      rlang::abort(glue::glue("cohort table column names should be {paste(expected_columns, collapse = ', ')} but are {paste(actual_columns, collapse = ', ')}!"))
    }

    # get the table name from the cohort table. name argument will be ignored.
    name <- rev(stringr::str_split(as.character(cohort_ref[[2]]$x), "\\.")[[1]])[1] %>%
      stringr::str_remove_all("[^A-Za-z0-9_]")
    write_prefix <- write_schema["prefix"]
    if (!is.na(write_prefix)) {
      if (substr(name, 1, nchar(write_prefix)) != write_prefix) {
        rlang::abort(glue::glue("cohort_ref ({name}) does not have the same prefix than the write_schema ({write_prefix})"))
      }
      name <- substr(name, nchar(write_prefix) + 1, nchar(name))
    }
    checkmate::assertCharacter(name, len = 1, min.chars = 1)
  }

  # we will check that cohort_set contains all the ids in the cohort table
  cohort_ids <- dplyr::distinct(cohort_ref, .data$cohort_definition_id) %>% dplyr::pull()

  # cohort_set table ----
  if (is.data.frame(cohort_set_ref)) {

    if ((paste0(name, "_set") %in% list_tables(con, write_schema)) && overwrite) {
      DBI::dbRemoveTable(con, inSchema(write_schema, paste0(name, "_set"), dbms = dbms(con)))
    }

    DBI::dbWriteTable(con, name = inSchema(write_schema, paste0(name, "_set"), dbms = dbms(con)), cohort_set_ref, overwrite = overwrite)
    cohort_set_ref <- dplyr::tbl(con, inSchema(write_schema, paste0(name, "_set"), dbms = dbms(con))) %>%
      dplyr::rename_all(tolower)

  } else if (is.null(cohort_set_ref)) {

    # create the cohort_set table
    cohort_set_ref <- cohort_ref %>%
      dplyr::distinct(.data$cohort_definition_id) %>%
      dplyr::mutate(cohort_name = paste0("cohort_", .data$cohort_definition_id)) %>%
      computeQuery(
        name = paste0(name, "_set"),
        temporary = FALSE,
        schema = write_schema,
        overwrite = overwrite)
  }

  {
    if (!methods::is(cohort_set_ref, "tbl_sql")) {
      rlang::abort("cohort_set_ref must be a dataframe or a remote table reference")
    }

    nm <- rev(stringr::str_split(as.character(cohort_set_ref[[2]]$x), "\\.")[[1]])[1] %>%
      stringr::str_remove_all("[^A-Za-z0-9_]")
    if (!is.na(write_prefix)) {
      if (substr(nm, 1, nchar(write_prefix)) != write_prefix) {
        rlang::abort(glue::glue("cohort_set_ref ({nm}) does not have the same prefix than the write_schema ({write_prefix})"))
      }
      nm <- substr(nm, nchar(write_prefix) + 1, nchar(nm))
    }
    name_set <- paste0(name, "_set")
    if (nm != name_set) {
      rlang::abort(glue::glue("cohort_set_ref database table name is {nm} but should be {name_set}!"))
    }

    expected_columns <- c("cohort_definition_id", "cohort_name")
    actual_columns <- tolower(colnames(cohort_set_ref))[1:2]
    if (!dplyr::setequal(expected_columns, actual_columns)) {
      rlang::abort(glue::glue("cohort_set column names should be `cohort_definition_id` `cohort_name` but are {paste(actual_columns, collapse = ', ')}!"))
    }

    # primary key check
    one <- cohort_set_ref %>%
      dplyr::count(.data$cohort_definition_id, name = "one") %>%
      dplyr::count(.data$one) %>%
      dplyr::pull("one")

    if (!(length(one) == 1 && one == 1)) {
      rlang::abort("cohort_definition_id is not a primary key on cohort_set_ref!")
    }

    cohort_set_ids <- dplyr::pull(cohort_set_ref, "cohort_definition_id")
    if (!all(cohort_ids %in% cohort_set_ids)) {
      diff <- dplyr::setdiff(cohort_ids, cohort_set_ids) %>% paste(collapse = ", ")
      rlang::abort(glue::glue("cohort IDs {diff} are in the cohort table but not in the cohort_set table!"))
    }
  }

  # cohort_count_table ----
  if (is.data.frame(cohort_count_ref)) {

    DBI::dbWriteTable(con, name = inSchema(write_schema, paste0(name, "_count"), dbms = dbms(con)), cohort_count_ref, overwrite = overwrite)
    cohort_count_ref <- dplyr::tbl(con, inSchema(write_schema, paste0(name, "_count"), dbms = dbms(con))) %>%
      dplyr::rename_all(tolower)

  } else if (is.null(cohort_count_ref)) {
    # create the cohort_count table
    cohort_count_ref <- cohort_ref %>%
      dplyr::group_by(.data$cohort_definition_id) %>%
      dplyr::summarise(
        number_records = dplyr::n(),
        number_subjects = dplyr::n_distinct(.data$subject_id)) %>%
      dplyr::right_join(
        dplyr::select(cohort_set_ref, "cohort_definition_id"),
        by = "cohort_definition_id") %>%
      dplyr::mutate(
        number_records = dplyr::coalesce(.data$number_records, 0),
        number_subjects = dplyr::coalesce(.data$number_subjects, 0)) %>%
      computeQuery(
        name = paste0(name, "_count"),
        temporary = FALSE,
        schema = write_schema,
        overwrite = overwrite)
  }

  {
    checkmate::assert_class(cohort_count_ref, "tbl_sql")

    nm <- rev(stringr::str_split(as.character(cohort_count_ref[[2]]$x), "\\.")[[1]])[1] %>%
      stringr::str_remove_all("[^A-Za-z0-9_]")

    if (!is.na(write_prefix)) {
      if (substr(nm, 1, nchar(write_prefix)) != write_prefix) {
        rlang::abort(glue::glue("cohort_count_ref ({nm}) does not have the same prefix than the write_schema ({write_prefix})"))
      }
      nm <- substr(nm, nchar(write_prefix) + 1, nchar(nm))
    }
    name_count <- paste0(name, "_count")
    if (nm != name_count) {
      rlang::abort(glue::glue("cohort_count_ref database table name is {nm} but should be {name_count}!"))
    }

    expected_columns <- c("cohort_definition_id", "number_records", "number_subjects")
    actual_columns <- tolower(colnames(cohort_count_ref))[1:3]
    if (!dplyr::setequal(actual_columns, expected_columns)) {
      rlang::abort(glue::glue("cohort_set column names should be {paste(expected_columns, collapse = ', ')} but are {paste(actual_columns, collapse = ', ')}!"))
    }
  }

  # cohort_attrition table ----
  if (is.data.frame(cohort_attrition_ref)) {

    DBI::dbWriteTable(con, name = inSchema(write_schema, paste0(name, "_attrition"), dbms = dbms(con)), cohort_attrition_ref, overwrite = overwrite)
    cohort_attrition_ref <- dplyr::tbl(con, inSchema(write_schema, paste0(name, "_attrition"), dbms = dbms(con))) %>%
      dplyr::rename_all(tolower)

  } else if (is.null(cohort_attrition_ref)) {

    # create a brand new attrition table
    cohort_attrition_ref <- cohort_count_ref %>%
      dplyr::mutate(
        reason = "Qualifying initial records",
        number_records = dplyr::coalesce(.data$number_records, 0),
        number_subjects = dplyr::coalesce(.data$number_subjects, 0),
        reason_id = 1, excluded_records = 0, excluded_subjects = 0) %>%
        dplyr::select(
          "cohort_definition_id", "number_records", "number_subjects",
          "reason_id", "reason", "excluded_records", "excluded_subjects") %>%
        computeQuery(
          name = paste0(name, "_attrition"),
          temporary = FALSE,
          schema = write_schema,
          overwrite = overwrite
        )
  }

  {
    checkmate::assert_class(cohort_attrition_ref, "tbl_sql")

    nm <- rev(stringr::str_split(as.character(cohort_attrition_ref[[2]]$x), "\\.")[[1]])[1] %>%
      stringr::str_remove_all("[^A-Za-z0-9_]")
    if (!is.na(write_prefix)) {
      if (substr(nm, 1, nchar(write_prefix)) != write_prefix) {
        rlang::abort(glue::glue("cohort_attrition_ref ({nm}) does not have the same prefix than the write_schema ({write_prefix})"))
      }
      nm <- substr(nm, nchar(write_prefix) + 1, nchar(nm))
    }
    name_attrition <- paste0(name, "_attrition")
    if (nm != name_attrition) {
      rlang::abort(glue::glue("cohort_attrition_ref database table name is {nm} but should be {name_attrition}!"))
    }

    expected_columns <- c("cohort_definition_id", "number_records", "number_subjects",
                          "reason_id", "reason", "excluded_records", "excluded_subjects")
    actual_columns <- tolower(colnames(cohort_attrition_ref))[1:7]

    if (!dplyr::setequal(actual_columns, expected_columns)) {
      rlang::abort(glue::glue("cohort_attrition column names should be {paste(expected_columns, collapse = ', ')} but are {paste(actual_columns, collapse = ', ')}!"))
    }
  }

  # create the GeneratedCohortSet object ----
  if (!("GeneratedCohortSet" %in% class(cohort_ref))) {
    class(cohort_ref) <- c("GeneratedCohortSet", class(cohort_ref))
  }

  attr(cohort_ref, "cohort_set") <- cohort_set_ref
  attr(cohort_ref, "cohort_attrition") <- cohort_attrition_ref
  attr(cohort_ref, "cohort_count") <- cohort_count_ref
  attr(cohort_ref, "tbl_name") <- name

  return(cohort_ref)
}


#' @rdname new_generated_cohort_set
#' @export
newGeneratedCohortSet <- function(cohortRef,
                                  cohortSetRef = NULL,
                                  cohortAttritionRef = NULL,
                                  cohortCountRef = NULL,
                                  overwrite = FALSE) {
  new_generated_cohort_set(cohort_ref = cohortRef,
                           cohort_set_ref = cohortSetRef,
                           cohort_attrition_ref = cohortAttritionRef,
                           cohort_count_ref = cohortCountRef,
                           overwrite = overwrite
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
  if (is.null(attr(x, "cohort_attrition"))) {
    NULL
  } else {
    dplyr::collect(attr(x, "cohort_attrition"))
  }
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
  dplyr::collect(attr(x, "cohort_set"))
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
  dplyr::collect(attr(x, "cohort_count"))
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

  if (dbms(attr(cdm, "dbcon")) %in% c("oracle", "snowflake")) {
    inclusionResultTableName <- toupper(inclusionResultTableName)
  }

  schema <- attr(cdm, "write_schema")
  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, min.chars = 1)

  if (paste0(cohortStem, "_attrition") %in% listTables(con, schema = schema)) {
    if (overwrite) {
      DBI::dbRemoveTable(con, inSchema(schema, paste0(cohortStem, "_attrition"), dbms = dbms(con)))
    } else {
      rlang::abort(paste0(cohortStem, "_attrition already exists in the database. Set overwrite = TRUE."))
    }
  }

  # Bring the inclusion result table to R memory
  inclusionResult <- dplyr::tbl(con, inSchema(schema, inclusionResultTableName, dbms(con))) %>%
    dplyr::collect() %>%
    dplyr::rename_all(tolower) %>%
    dplyr::mutate(inclusion_rule_mask = as.numeric(.data$inclusion_rule_mask))

  attritionList <- list()

  for (i in seq_along(cohortId)) {

    id <- cohortId[i]
    inclusionName <- NULL
    for (k in seq_along(cohortSet$cohort[[i]]$InclusionRules)) {
      if ("name" %in% names(cohortSet$cohort[[i]]$InclusionRules[[k]])) {
        inclusionName <- c(
          inclusionName, cohortSet$cohort[[i]]$InclusionRules[[k]]$name
        )
      } else {
        inclusionName <- c(inclusionName, "Unnamed criteria")
      }
    }

    numberInclusion <- length(inclusionName)
    if (numberInclusion == 0) {
      #cohortTableName <- paste0(cohortStem, "_cohort")
      cohortTableName <- cohortStem
      attrition <- dplyr::tibble(
        cohort_definition_id = id,
        number_records = dplyr::tbl(con, inSchema(schema, cohortTableName, dbms(con))) %>%
          dplyr::rename_all(tolower) %>%
          dplyr::filter(.data$cohort_definition_id == id) %>%
          dplyr::tally() %>%
          dplyr::pull("n") %>%
          as.numeric(),
        number_subjects = dplyr::tbl(con, inSchema(schema, cohortTableName, dbms(con))) %>%
          dplyr::rename_all(tolower) %>%
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
            dplyr::filter(.data$cohort_definition_id == id) %>%
            dplyr::filter(.data$mode_id == 0) %>%
            dplyr::filter(.data$inclusion_rule_mask %in% inclusionMaskId[[k]]) %>%
            dplyr::pull("person_count") %>%
            base::sum() %>%
            as.numeric(),
          number_subjects = inclusionResult %>%
            dplyr::filter(.data$cohort_definition_id == id) %>%
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
            .data$number_records,
          excluded_subjects =
            dplyr::lag(.data$number_subjects, 1, order_by = .data$reason_id) -
            .data$number_subjects
        ) %>%
        dplyr::mutate(
          excluded_records = dplyr::coalesce(.data$excluded_records, 0),
          excluded_subjects = dplyr::coalesce(.data$excluded_subjects, 0)
        )
    }
    attritionList[[i]] <- attrition
  }

  attrition <- attritionList %>%
    dplyr::bind_rows() %>%
    dplyr::rename_all(tolower)

  # upload attrition table to database
  DBI::dbWriteTable(con,
                    name = inSchema(schema, paste0(cohortStem, "_attrition"), dbms = dbms(con)),
                    value = attrition)

  dplyr::tbl(con, inSchema(schema, paste0(cohortStem, "_attrition"), dbms(con))) %>%
    dplyr::rename_all(tolower)
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

caprConceptToDataframe <- function(x) {
  tibble::tibble(
    conceptId = purrr::map_int(x@Expression, ~.@Concept@concept_id),
    conceptCode = purrr::map_chr(x@Expression, ~.@Concept@concept_code),
    conceptName = purrr::map_chr(x@Expression, ~.@Concept@concept_name),
    domainId = purrr::map_chr(x@Expression, ~.@Concept@domain_id),
    vocabularyId = purrr::map_chr(x@Expression, ~.@Concept@vocabulary_id),
    standardConcept = purrr::map_chr(x@Expression, ~.@Concept@standard_concept),
    includeDescendants = purrr::map_lgl(x@Expression, "includeDescendants"),
    isExcluded = purrr::map_lgl(x@Expression, "isExcluded"),
    includeMapped = purrr::map_lgl(x@Expression, "includeMapped")
  )
}

#' Add attrition reason to a GeneratedCohortSet object
#'
#' Update the cohort attrition table with new counts and a reason for attrition.
#'
#' @param cohort A generated cohort set
#' @param reason The reason for attrition as a character string
#' @param cohortId Cohort definition id of the cohort you want to update the
#' attrition
#'
#' @return The cohort object with the attributes created or updated.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con = con, cdm_schema = "main", write_schema = "main")
#' cdm <- generateConceptCohortSet(
#'   cdm = cdm, conceptSet = list(pharyngitis = 4112343), name = "new_cohort"
#' )
#'
#' cohortSet(cdm$new_cohort)
#' cohortCount(cdm$new_cohort)
#' cohortAttrition(cdm$new_cohort)
#'
#' cdm$new_cohort <- cdm$new_cohort %>%
#'   filter(cohort_start_date >= as.Date("2010-01-01"))
#'
#' cdm$new_cohort <- updateCohortAttributes(
#'   cohort = cdm$new_cohort, reason = "Only events after 2010"
#' )
#'
#' cohortSet(cdm$new_cohort)
#' cohortCount(cdm$new_cohort)
#' cohortAttrition(cdm$new_cohort)
#' }
recordCohortAttrition <- function(cohort,
                                  reason,
                                  cohortId = NULL) {
  checkmate::assertClass(cohort, "GeneratedCohortSet")
  name <- attr(cohort, "tbl_name")
  checkmate::assertCharacter(name, len = 1, min.chars = 1)
  checkmate::assertCharacter(reason, len = 1, min.chars = 1, any.missing = FALSE)
  checkmate::assertIntegerish(cohortId, any.missing = FALSE, null.ok = TRUE)

  cdm <- attr(cohort, "cdm_reference")
  checkmate::assertClass(cdm, "cdm_reference")

  if (is.null(cohortId)) {
    cohortId <- attr(cohort, "cohort_set") %>%
      dplyr::pull("cohort_definition_id")
  }

  tm <- as.integer(Sys.time())

  tempCohortCount <- attr(cohort, "cohort_count") %>%
    dplyr::filter(!(.data$cohort_definition_id %in% .env$cohortId)) %>%
    computeQuery(
      name = paste0("temp", tm, "a_"),
      # temporary = getOption("intermediate_as_temp", TRUE),
      temporary = TRUE,
      overwrite = TRUE,
      schema = attr(cdm, "write_schema"))

  # update cohort_count ----
  attr(cohort, "cohort_count") <- cohort %>%
    dplyr::filter(.data$cohort_definition_id %in% .env$cohortId) %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(
      number_records = dplyr::n(),
      number_subjects = dplyr::n_distinct(.data$subject_id)
    ) %>%
    dplyr::union_all(tempCohortCount) %>%
    dplyr::right_join(
      attr(cohort, "cohort_set") %>% dplyr::select("cohort_definition_id"),
      by = "cohort_definition_id"
    ) %>%
    dplyr::mutate(
      number_records = dplyr::coalesce(.data$number_records, 0),
      number_subjects = dplyr::coalesce(.data$number_subjects, 0)) %>%
    computeQuery(
      name = paste0(name, "_count"),
      temporary = FALSE,
      schema = attr(cdm, "write_schema"),
      overwrite = TRUE
    )

  # update cohort_attrition ----
  newAttritionRow <- attr(cohort, "cohort_attrition") %>%
    dplyr::filter(.data$cohort_definition_id %in% .env$cohortId) %>%
    dplyr::select("cohort_definition_id",
                  "reason_id",
                  "previous_records" = "number_records",
                  "previous_subjects" = "number_subjects") %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::filter(.data$reason_id == max(.data$reason_id, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::inner_join(attr(cohort, "cohort_count"), by = c("cohort_definition_id")) %>%
    dplyr::mutate(
      reason_id = .data$reason_id + 1,
      reason = .env$reason,
      excluded_records = .data$previous_records - .data$number_records,
      excluded_subjects = .data$previous_subjects - .data$number_subjects) %>%
    dplyr::select(
      "cohort_definition_id", "number_records", "number_subjects",
      "reason_id", "reason", "excluded_records", "excluded_subjects") %>%
    computeQuery(
      # temporary = getOption("intermediate_as_temp", TRUE),
      name = paste0("temp", tm, "b_"),
      temporary = TRUE,
      overwrite = TRUE,
      schema = attr(cdm, "write_schema"))

  tempCohortAttrition <- attr(cohort, "cohort_attrition") %>%
    computeQuery(
      # temporary = getOption("intermediate_as_temp", TRUE),
      name = paste0("temp", tm, "c_"),
      temporary = TRUE,
      overwrite = TRUE,
      schema = attr(cdm, "write_schema"))

  # note that overwrite will drop the table that is needed for the query.
  # TODO support overwrite existing table using rename in computeQuery. Cross platform table rename is needed for this though.
  attr(cohort, "cohort_attrition")  <- tempCohortAttrition %>%
    dplyr::union_all(newAttritionRow) %>%
    computeQuery(
      name = paste0(name, "_attrition"),
      temporary = FALSE,
      schema = attr(cdm, "write_schema"),
      overwrite = TRUE)

  return(cohort)
}

#' @export
#' @rdname recordCohortAttrition
record_cohort_attrition <- recordCohortAttrition
