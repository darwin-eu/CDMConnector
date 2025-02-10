# Copyright 2024 DARWIN EU®
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
readCohortSet <- function(path) {
  checkmate::checkCharacter(path, len = 1, min.chars = 1)
  ensureInstalled("readr")
  ensureInstalled("jsonlite")

  if (!dir.exists(path)) {
    rlang::abort(glue::glue("The directory {path} does not exist!"))
  }

  if (!file.info(path)$isdir) {
    rlang::abort(glue::glue("{path} is not a directory!"))
  }

  if (file.exists(file.path(path, "CohortsToCreate.csv"))) {
    readr::local_edition(1)
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
      dplyr::mutate(json = purrr::map(.data$json_path, readr::read_file)) %>%
      dplyr::mutate(cohort_name = stringr::str_replace_all(tolower(.data$cohort_name), "\\s", "_")) %>%
      dplyr::mutate(cohort_name = stringr::str_remove_all(.data$cohort_name, "[^a-z0-9_]")) %>%
      # if the cohort filenames are numbers then use the number as the id and prefix the name with "cohort"
      # Supress Warnings about NA conversion. If the string is not a number we don't treat it as a number.
      dplyr::mutate(cohort_definition_id = dplyr::if_else(stringr::str_detect(.data$cohort_name, "^[0-9]+$"), suppressWarnings(as.integer(.data$cohort_name)), .data$cohort_definition_id)) %>%
      dplyr::mutate(cohort_name = dplyr::if_else(stringr::str_detect(.data$cohort_name, "^[0-9]+$"), paste0("cohort_", .data$cohort_name), .data$cohort_name))

      if (length(unique(cohortsToCreate$cohort_definition_id)) != nrow(cohortsToCreate) ||
          length(unique(cohortsToCreate$cohort_name)) != nrow(cohortsToCreate)) {

        tryCatch(
          cli::cli_abort("Problem creating cohort IDs and names from json file names. IDs and filenames must be unique!"),
          finally = print(cohortsToCreate[,1:2])
        )
      }

    }

  # snakecase name can be used for column names or filenames
  cohortsToCreate <- cohortsToCreate %>%
    dplyr::mutate(cohort_name_snakecase = snakecase::to_snake_case(.data$cohort_name)) %>%
    dplyr::select("cohort_definition_id", "cohort_name", "cohort", "json", "cohort_name_snakecase")

  for (i in seq_len(nrow(cohortsToCreate))) {
    first_chr <- substr(cohortsToCreate$cohort_name[i], 1, 1)
    if (!grepl("[a-zA-Z]", first_chr)) {
      cli::cli_abort("Cohort names must start with a letter but {cohortsToCreate$cohort_name[i]} does not.
                     Rename the json file or use a CohortsToCreate.csv file to explicity set cohort names.")
    }
  }

  class(cohortsToCreate) <- c("CohortSet", class(cohortsToCreate))
  return(cohortsToCreate)
}

#' Generate a cohort set on a cdm object
#'
#' @description
#' A "chort_table" object consists of four components
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

#' One key design principle is that cohort_table objects are created once
#' and can persist across analysis execution but should not be modified after
#' creation. While it is possible to modify a cohort_table object doing
#' so will invalidate it and it's attributes may no longer be accurate.
#'
#' @param cdm A cdm reference created by CDMConnector. write_schema must be
#'   specified.
#' @param name Name of the cohort table to be created. This will also be used
#' as a prefix for the cohort attribute tables. This must be a lowercase character string
#' that starts with a letter and only contains letters, numbers, and underscores.
#' @param cohortSet A cohortSet dataframe created with `readCohortSet()`
#' @param computeAttrition Should attrition be computed? TRUE (default) or FALSE
#' @param overwrite Should the cohort table be overwritten if it already
#' exists? TRUE (default) or FALSE
#' @export
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = "main",
#'                   writeSchema = "main")
#'
#' cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
#' cdm <- generateCohortSet(cdm, cohortSet, name = "cohort")
#'
#' print(cdm$cohort)
#'
#' attrition(cdm$cohort)
#' settings(cdm$cohort)
#' cohortCount(cdm$cohort)
#' }
generateCohortSet <- function(cdm,
                              cohortSet,
                              name,
                              computeAttrition = TRUE,
                              overwrite = TRUE) {
  rlang::check_installed("CirceR")
  rlang::check_installed("SqlRender")


  if (!is.data.frame(cohortSet)) {
    rlang::abort("`cohortSet` must be a dataframe from the output of `readCohortSet()`.")
  }

  checkmate::assertDataFrame(cohortSet, min.rows = 1, col.names = "named")
  stopifnot(all(c("cohort_definition_id", "cohort_name", "cohort", "json") %in% names(cohortSet)))

  cli::cli_alert_info("Generating {nrow(cohortSet)} cohort{?s}")
  withr::local_options(list("cli.progress_show_after" = 0, "cli.progress_clear" = FALSE))
  checkmate::assertClass(cdm, "cdm_reference")
  con <- cdmCon(cdm)
  checkmate::assertTRUE(DBI::dbIsValid(con))
  checkmate::assertCharacter(name, len = 1, min.chars = 1, any.missing = FALSE)
  if (name != tolower(name)) {
    rlang::abort("Cohort table name {name} must be lowercase!")
  }
  if (!grepl("^[a-z]", substr(name, 1, 1))) {
    cli::cli_abort("Cohort table name {name} must start with a letter!")
  }
  if (!grepl("^[a-z][a-z0-9_]*$", name)) {
    cli::cli_abort("Cohort table name {name} must only contain letters, numbers, and underscores!")
  }
  checkmate::assertLogical(computeAttrition, len = 1)
  checkmate::assertLogical(overwrite, len = 1)

  write_schema <- cdmWriteSchema(cdm)
  checkmate::assert_character(write_schema,
                              min.chars = 1,
                              min.len = 1,
                              max.len = 3,
                              null.ok = FALSE)

  if ("prefix" %in% names(write_schema)) {
    prefix <- unname(write_schema["prefix"])
  } else {
    prefix <- ""
  }

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
    cli::cli_abort("Cohort table name {.code {paste0(prefix, name)}} must be lowercase!")
  }

  # Make sure tables do not already exist
  existingTables <- listTables(con, write_schema)

  for (x in paste0(name, c("", "_count", "_set", "_attrition"))) {
    if (x %in% existingTables) {
      if (overwrite) {
        DBI::dbRemoveTable(con, .inSchema(write_schema, x, dbms = dbms(con)))
      } else {
        cli::cli_abort("The cohort table {paste0(prefix, name)} already exists.\nSpecify overwrite = TRUE to overwrite it.")
      }
    }
  }

  # Create the OHDSI-SQL for each cohort ----

  cohortSet$sql <- character(nrow(cohortSet))

  for (i in seq_len(nrow(cohortSet))) {
    cohortJson <- cohortSet$json[[i]]
    cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
    cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                          options = CirceR::createGenerateOptions(
                                            generateStats = computeAttrition))
    cohortSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
  }

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

  # dropTempTableIfExists <- function(con, table) {
  #   # used for dropping temp emulation tables
  #   suppressMessages(
  #     DBI::dbExecute(
  #       con,
  #       SqlRender::translate(
  #         glue::glue("IF OBJECT_ID('#{table}', 'U') IS NOT NULL DROP TABLE #{table};"),
  #         targetDialect = dbms(con))
  #       )
  #   )
  # }

  generate <- function(i) {
    pct <- ""
    cli::cli_progress_step("Generating cohort ({i}/{nrow(cohortSet)}{pct}) - {cohortSet$cohort_name[i]}", spinner = interactive())

    sql <- cohortSet$sql[i] %>%
      SqlRender::render(
        cdm_database_schema = cdm_schema_sql,
        vocabulary_database_schema = cdm_schema_sql,
        target_database_schema = write_schema_sql,
        results_database_schema.cohort_inclusion        = paste0(write_schema_sql, ".", DBI::dbQuoteIdentifier(con, paste0(prefix, name, "_inclusion"))),
        results_database_schema.cohort_inclusion_result = paste0(write_schema_sql, ".", DBI::dbQuoteIdentifier(con, paste0(prefix, name, "_inclusion_result"))),
        results_database_schema.cohort_summary_stats    = paste0(write_schema_sql, ".", DBI::dbQuoteIdentifier(con, paste0(prefix, name, "_summary_stats"))),
        results_database_schema.cohort_censor_stats     = paste0(write_schema_sql, ".", DBI::dbQuoteIdentifier(con, paste0(prefix, name, "_censor_stats"))),
        results_database_schema.cohort_inclusion        = paste0(write_schema_sql, ".", DBI::dbQuoteIdentifier(con, paste0(prefix, name, "_inclusion"))),
        target_cohort_table = DBI::dbQuoteIdentifier(con, paste0(prefix, name)),
        target_cohort_id = cohortSet$cohort_definition_id[i],
        warnOnMissingParameters = FALSE
      )

    if (dbms(con) == "snowflake") {
      # we don't want to use temp emulation on snowflake. We want to use actual temp tables.
      sql <- stringr::str_replace_all(sql, "CREATE TABLE #", "CREATE TEMPORARY TABLE ") %>%
        stringr::str_replace_all("create table #", "create temporary table ") %>%
        stringr::str_replace_all("#", "")

      # temp tables created by circe that can be left dangling.
      tempTablesToDrop <- c(
        "Codesets",
        "qualified_events",
        "cohort_rows",
        "Inclusion",
        "strategy_ends",
        "inclusion_events",
        "included_events",
        "final_cohort",
        "inclusion_rules",
        "BEST_EVENTS",
        paste0("Inclusion_", 0:9))

      for (j in seq_along(tempTablesToDrop)) {
        suppressMessages({
          invisible(DBI::dbExecute(con, paste("drop table if exists", tempTablesToDrop[j])))
        })
      }

      namesToQuote <- c("cohort_definition_id",
                        "subject_id",
                        "cohort_start_date",
                        "cohort_end_date",
                        "mode_id",
                        "inclusion_rule_mask",
                        "person_count",
                        "rule_sequence",
                        "gain_count",
                        "person_total",
                        "base_count", "final_count")

      for (n in namesToQuote) {
        sql <- stringr::str_replace_all(sql, n, DBI::dbQuoteIdentifier(con, n))
      }
    }

    # total hack workaround for circe - temp23019_chrt0_inclusion"_stats
    quoteSymbol <- substr(as.character(DBI::dbQuoteIdentifier(con, "a")), 1, 1)
    sql <- stringr::str_replace_all(sql,
                             paste0("_inclusion", quoteSymbol, "_stats"),
                             paste0("_inclusion_stats", quoteSymbol))

    # if parameters exist in the sql (starting with @), stop.
    stopifnot(length(unique(stringr::str_extract_all(sql, "@\\w+"))[[1]]) == 0)

    # remove comments from SQL which are causing an issue on spark
    # --([^\n])*?\n => match strings starting with -- followed by anything except a newline
    sql <- stringr::str_replace_all(sql, "--([^\n])*?\n", "\n")

    if (dbms(con) != "spark" && dbms(con) != "bigquery") {
      sql <- SqlRender::translate(sql,
                                  targetDialect = CDMConnector::dbms(con),
                                  tempEmulationSchema = "SQL ERROR")

      if (stringr::str_detect(sql, "SQL ERROR")) {
        cli::cli_abort("sqlRenderTempEmulationSchema being used for cohort generation!
        Please open a github issue at {.url https://github.com/darwin-eu/CDMConnector/issues} with your cohort definition.")
      }
    } else {
      # we need temp emulation on spark as there are no temp tables

      if ("schema" %in% names(write_schema)) {
        s <- unname(write_schema["schema"])
      } else if (length(write_schema) == 1) {
        s <- unname(write_schema)
      } else {
        s <- unname(write_schema[2])
      }

      sql <- SqlRender::translate(sql,
                                  targetDialect = CDMConnector::dbms(con),
                                  tempEmulationSchema = s)
    }

    if (dbms(con) == "duckdb") {
      # hotfix for duckdb sql translation https://github.com/OHDSI/SqlRender/issues/340
      sql <- gsub("'-1 \\* (\\d+) day'", "'-\\1 day'", sql)
    }

    if (dbms(con) == "spark") {
      # issue with date add translation on spark
      sql <- stringr::str_replace_all(sql, "date_add", "dateadd")
      sql <- stringr::str_replace_all(sql, "DATE_ADD", "DATEADD")
    }

    sql <- stringr::str_replace_all(sql, "\\s+", " ")

    sql <- stringr::str_split(sql, ";")[[1]] %>%
      stringr::str_trim() %>%
      stringr::str_c(";") %>% # remove empty statements
      stringr::str_subset("^;$", negate = TRUE)

    # drop temp tables if they already exist
    drop_statements <- c(
      stringr::str_subset(sql, "DROP TABLE") %>%
        stringr::str_subset("IF EXISTS", negate = TRUE) %>%
        stringr::str_replace("DROP TABLE", "DROP TABLE IF EXISTS"),

      stringr::str_subset(sql, "DROP TABLE IF EXISTS")
    ) %>%
      purrr::map_chr(~SqlRender::translate(., dbms(con)))

    drop_statements <- stringr::str_replace_all(drop_statements, "--([^\n])*?\n", "\n")

    for (k in seq_along(drop_statements)) {
      if (grepl("^--", drop_statements[k])){
        next
      }
      suppressMessages(DBI::dbExecute(con, drop_statements[k], immediate = TRUE))
    }

    sql <- stringr::str_replace_all(sql, "--([^\n])*?\n", "\n")

    for (k in seq_along(sql)) {
      # cli::cat_rule(glue::glue("sql {k} with {nchar(sql[k])} characters."))
      # cli::cat_line(sql[k])
      if (grepl("^--", sql[k])){
        next
      }

      DBI::dbExecute(con, sql[k], immediate = TRUE)

      if (interactive()) {
        pct <- ifelse(k == length(sql), "", glue::glue(" ~ {floor(100*k/length(sql))}%"))
        cli::cli_progress_update()
      }
    }
  }

  # this loop makes cli updates look correct
  for (i in seq_len(nrow(cohortSet))) {
    generate(i)
  }

  cohort_ref <- dplyr::tbl(con, .inSchema(write_schema, name, dbms = dbms(con)))

  # Create attrition attribute ----
  if (computeAttrition) {
    cohort_attrition_ref <- computeAttritionTable(
      cdm = cdm,
      cohortStem = name,
      cohortSet = cohortSet,
      overwrite = overwrite
    ) |>
      dplyr::collect()
  } else {
    cohort_attrition_ref <- NULL
  }

  # Create cohort_set attribute -----
  # if (paste0(name, "_set") %in% existingTables) {
  #   DBI::dbRemoveTable(con, inSchema(write_schema, paste0(name, "_set"), dbms = dbms(con)))
  # }

  cdm[[name]] <- cohort_ref |>
    omopgenerics::newCdmTable(src = attr(cdm, "cdm_source"), name = name)

  # Create the object. Let the constructor handle getting the counts.----

  cohortSetRef <- dplyr::transmute(cohortSet,
    cohort_definition_id = as.integer(.data$cohort_definition_id),
    cohort_name = as.character(.data$cohort_name))

  cdm[[name]] <- omopgenerics::newCohortTable(
    table = cdm[[name]],
    cohortSetRef = cohortSetRef,
    cohortAttritionRef = cohort_attrition_ref)

  cli::cli_progress_done()

  return(cdm)
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

  con <- cdmCon(cdm)
  checkmate::assertTRUE(DBI::dbIsValid(con))

  inclusionResultTableName <- paste0(cohortStem, "_inclusion_result")

  # if (dbms(cdmCon(cdm)) %in% c("oracle", "snowflake")) {
  #   inclusionResultTableName <- toupper(inclusionResultTableName)
  # }

  schema <- cdmWriteSchema(cdm)
  checkmate::assertCharacter(schema, min.len = 1, max.len = 3, min.chars = 1)

  if (paste0(cohortStem, "_attrition") %in% listTables(con, schema = schema)) {
    if (overwrite) {
      DBI::dbRemoveTable(con, .inSchema(schema, paste0(cohortStem, "_attrition"), dbms = dbms(con)))
    } else {
      rlang::abort(paste0(cohortStem, "_attrition already exists in the database. Set overwrite = TRUE."))
    }
  }

  # Bring the inclusion result table to R memory
  inclusionResult <- dplyr::tbl(con, .inSchema(schema, inclusionResultTableName, dbms(con))) %>%
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
        number_records = dplyr::tbl(con, .inSchema(schema, cohortTableName, dbms(con))) %>%
          dplyr::rename_all(tolower) %>%
          dplyr::filter(.data$cohort_definition_id == id) %>%
          dplyr::tally() %>%
          dplyr::pull("n") %>%
          as.numeric(),
        number_subjects = dplyr::tbl(con, .inSchema(schema, cohortTableName, dbms(con))) %>%
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
    dplyr::rename_all(tolower) %>%
    dplyr::transmute(
      cohort_definition_id = as.integer(.data$cohort_definition_id),
      number_records = as.integer(.data$number_records),
      number_subjects = as.integer(.data$number_subjects),
      reason_id = as.integer(.data$reason_id),
      reason = as.character(.data$reason),
      excluded_records = as.integer(.data$excluded_records),
      excluded_subjects = as.integer(.data$excluded_subjects)
    )

  # Make sure last row of attrition matches actual record counts in the cohort table
  # Difference can be due to collapsing of cohort records in the cohort eras Circe step.
  finalCounts <- dplyr::tbl(con, .inSchema(schema, cohortStem, dbms(con))) %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(n_records = dplyr::n(), n_subjects = dplyr::n_distinct(.data$subject_id, na.rm = TRUE)) %>%
    dplyr::collect()

  lastAttritionRow <- dplyr::slice_max(attrition, n = 1, order_by = .data$reason_id, by = "cohort_definition_id")

  newAttritionRow <- dplyr::inner_join(finalCounts, lastAttritionRow, by = "cohort_definition_id") %>%
    dplyr::mutate(
      excluded_records = .data$number_records - .data$n_records,
      excluded_subjects = .data$number_subjects - .data$n_subjects,
    ) %>%
    dplyr::transmute(
      cohort_definition_id = .data$cohort_definition_id,
      number_records = .data$n_records,
      number_subjects = .data$n_subjects,
      reason_id = 1L + .data$reason_id,
      reason = "Cohort records collapsed",
      excluded_records = .data$excluded_records,
      excluded_subjects = .data$excluded_subjects
    )

  attrition <- dplyr::bind_rows(attrition, newAttritionRow) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$reason_id)

  # upload attrition table to database
  DBI::dbWriteTable(con,
                    name = .inSchema(schema, paste0(cohortStem, "_attrition"), dbms = dbms(con)),
                    value = attrition)

  dplyr::tbl(con, .inSchema(schema, paste0(cohortStem, "_attrition"), dbms(con))) %>%
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

