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

table_refs <- function(domain_id) {
  dplyr::tribble(
    ~domain_id,    ~table_name,            ~concept_id,              ~start_date,                  ~end_date,
    "condition",   "condition_occurrence", "condition_concept_id",   "condition_start_date",       "condition_end_date",
    "drug",        "drug_exposure",        "drug_concept_id",        "drug_exposure_start_date",   "drug_exposure_end_date",
    "procedure",   "procedure_occurrence", "procedure_concept_id",   "procedure_date",             "procedure_date",
    "observation", "observation",          "observation_concept_id", "observation_date",           "observation_date",
    "measurement", "measurement",          "measurement_concept_id", "measurement_date",           "measurement_date",
    "visit",       "visit_occurrence",     "visit_concept_id",       "visit_start_date",           "visit_end_date",
    "device",      "device_exposure",      "device_concept_id",      "device_exposure_start_date", "device_exposure_end_date"
  ) %>% dplyr::filter(.data$domain_id %in% .env$domain_id)
}


#' Create a new generated cohort set from a list of concept sets
#'
#' @description
#'
#' Generate a new cohort set from one or more concept sets. Each
#' concept set will result in one cohort and represent the time during which
#' the concept was observed for each subject/person. Concept sets can be
#' passed to this function as:
#' \itemize{
#'  \item{A named list of numeric vectors, one vector per concept set}
#'  \item{A named list of Capr concept sets}
#' }
#'
#' Clinical observation records will be looked up in the respective domain tables
#' using the vocabulary in the CDM. If a required domain table does not exist in
#' the cdm object a warning will be given.
#' Concepts that are not in the vocabulary or in the data will be silently ignored.
#' If end dates are missing or do not exist, as in the case of the procedure and
#' observation domains, the the start date will be used as the end date.
#'
#' @param cdm A cdm reference object created by `CDMConnector::cdmFromCon` or `CDMConnector::cdm_from_con`
#' @param conceptSet,concept_set A named list of numeric vectors or Capr concept sets
#' @param name The name of the new generated cohort table as a character string
#' @param limit Include "first" (default) or "all" occurrences of events in the cohort
#' \itemize{
#'  \item{"first" will include only the first occurrence of any event in the concept set in the cohort.}
#'  \item{"all" will include all occurrences of the events defined by the concept set in the cohort.}
#' }
#' @param requiredObservation,required_observation A numeric vector of length 2 that specifies the number of days of
#' required observation time prior to index and post index for an event to be included in the cohort.
#' @param end How should the `cohort_end_date` be defined?
#' \itemize{
#'  \item{"observation_period_end_date" (default): The earliest observation_period_end_date after the event start date}
#'  \item{numeric scalar: A fixed number of days from the event start date}
#'  \item{"event_end_date"}: The event end date. If the event end date is not populated then the event start date will be used
#' }
#' @param subsetCohort,subset_cohort  A cohort table containing the individuals for which to
#' generate cohorts for. Only individuals in the cohort table will appear in
#' the created generated cohort set.
#' @param subsetCohortId,subset_cohort_id A set of cohort IDs from the cohort table for which
#' to include. If none are provided, all cohorts in the cohort table will
#' be included.
#' @param overwrite Should the cohort table be overwritten if it already exists? TRUE (default) or FALSE.
#'
#' @return A cdm reference object with the new generated cohort set table added
#' @export
generateConceptCohortSet <- function(cdm,
                                     conceptSet = NULL,
                                     name,
                                     limit = "first",
                                     requiredObservation = c(0,0),
                                     end = "observation_period_end_date",
                                     subsetCohort = NULL,
                                     subsetCohortId = NULL,
                                     overwrite = TRUE) {

  # check cdm ----
  checkmate::assertClass(cdm, "cdm_reference")
  con <- cdmCon(cdm)
  checkmate::assertTRUE(DBI::dbIsValid(cdmCon(cdm)))
  checkmate::assert_character(name, len = 1, min.chars = 1, any.missing = FALSE, pattern = "[a-zA-Z0-9_]+")

  assertTables(cdm, "observation_period", empty.ok = FALSE)

  # check name ----
  checkmate::assertLogical(overwrite, len = 1, any.missing = FALSE)
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE, min.chars = 1, pattern = "[a-z1-9_]+")
  existingTables <- listTables(con, cdmWriteSchema(cdm))

  if (name %in% existingTables && !overwrite) {
    rlang::abort(glue::glue("{name} already exists in the CDM write_schema and overwrite is FALSE!"))
  }

  # check limit ----
  checkmate::assertChoice(limit, c("first", "all"))

  # check requiredObservation ----
  checkmate::assertIntegerish(requiredObservation, lower = 0, any.missing = FALSE, len = 2)

  # check end ----
  if (is.numeric(end)) {
    checkmate::assertIntegerish(end, lower = 0L, len = 1)
  } else if (is.character(end)) {
    checkmate::assertCharacter(end, len = 1)
    checkmate::assertChoice(end, choices = c("observation_period_end_date", "event_end_date"))
  } else {
    rlang::abort('`end` must be a natural number of days from start, "observation_period_end_date", or "event_end_date"')
  }

  # check ConceptSet ----
  checkmate::assertList(conceptSet, min.len = 1, any.missing = FALSE, types = c("numeric", "ConceptSet"), names = "named")
  checkmate::assertList(conceptSet, min.len = 1, names = "named")
  CDMConnector::assert_tables(cdm, "concept")

  if (methods::is(conceptSet[[1]], "ConceptSet")) {
    purrr::walk(conceptSet, ~checkmate::assertClass(., "ConceptSet"))

    df <- dplyr::tibble(cohort_definition_id = seq_along(conceptSet),
                        cohort_name = names(conceptSet),
                        df = purrr::map(conceptSet, caprConceptToDataframe)) %>%
      tidyr::unnest(cols = df) %>%
      dplyr::mutate(
        "limit" = .env$limit,
        "prior_observation" = .env$requiredObservation[1],
        "future_observation" = .env$requiredObservation[2],
        "end" = .env$end
      ) %>%
      dplyr::select(
        "cohort_definition_id", "cohort_name", "concept_id" = "conceptId",
        "include_descendants" = "includeDescendants",
        "is_excluded" = "isExcluded",
        dplyr::any_of(c(
          "limit", "prior_observation", "future_observation", "end"
        ))
      )

  } else {
    # remove any empty concept sets
    conceptSet <- conceptSet[lengths(conceptSet) > 0]
    # conceptSet must be a named list of integer-ish vectors
    purrr::walk(conceptSet, ~checkmate::assert_integerish(., lower = 0, min.len = 1, any.missing = FALSE))

    df <- dplyr::tibble(cohort_definition_id = seq_along(.env$conceptSet),
                        cohort_name = names(.env$conceptSet),
                        limit = .env$limit,
                        prior_observation = .env$requiredObservation[1],
                        future_observation = .env$requiredObservation[2],
                        end = .env$end,
                        concept_id = .env$conceptSet) %>%
      tidyr::unnest(cols = "concept_id") %>%
      dplyr::transmute(.data$cohort_definition_id,
                       .data$cohort_name,
                       .data$concept_id,
                       .data$limit,
                       .data$prior_observation,
                       .data$future_observation,
                       .data$end,
                       include_descendants = FALSE,
                       is_excluded = FALSE)
  }

  cohort_set_ref <- df |>
    dplyr::select("cohort_definition_id", "cohort_name", "limit",
                  "prior_observation", "future_observation", "end") |>
    dplyr::distinct()

  # check target cohort -----
  if (!is.null(subsetCohort)) {
    assertTables(cdm, subsetCohort)
  }

  if (!is.null(subsetCohort) && !is.null(subsetCohortId)){
    if (!nrow(omopgenerics::settings(cdm[[subsetCohort]]) %>% dplyr::filter(.data$cohort_definition_id %in% .env$subsetCohortId)) > 0){
      cli::cli_abort("cohort_definition_id {subsetCohortId} not found in cohort set of {subsetCohort}")
   }}

  # upload concept data to the database ----
  tempName <- paste0("tmp", as.integer(Sys.time()), "_")

  DBI::dbWriteTable(cdmCon(cdm),
                    name = .inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con)),
                    value = df,
                    overwrite = TRUE)

  if (any(df$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }

  # realize full list of concepts ----
  concepts <- dplyr::tbl(cdmCon(cdm), .inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con))) %>%
    dplyr::rename_all(tolower)

  if (any(df$include_descendants)) {
    concepts <- concepts  %>%
      dplyr::filter(.data$include_descendants == TRUE) %>%
        dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
        dplyr::select(
          "cohort_definition_id", "cohort_name",
          "concept_id" = "descendant_concept_id", "is_excluded",
          dplyr::any_of(c("limit", "prior_observation", "future_observation", "end"))
        ) %>%
        dplyr::union_all(
          dplyr::tbl(
            cdmCon(cdm),
            .inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con))
          ) %>%
          dplyr::select(dplyr::any_of(c(
            "cohort_definition_id", "cohort_name", "concept_id", "is_excluded",
            "limit", "prior_observation", "future_observation", "end"
          )))
        )
    }

  concepts <- concepts %>%
        dplyr::filter(.data$is_excluded == FALSE) %>%
    # Note that concepts that are not in the vocab will be silently ignored
    dplyr::inner_join(dplyr::select(cdm$concept, "concept_id", "domain_id"), by = "concept_id") %>%
    dplyr::select(
      "cohort_definition_id", "cohort_name", "concept_id", "domain_id",
      dplyr::any_of(c("limit", "prior_observation", "future_observation", "end"))
    ) %>%
    dplyr::distinct() %>%
    dplyr::compute()

  on.exit({
    if (DBI::dbIsValid(cdmCon(cdm))) {
      DBI::dbRemoveTable(cdmCon(cdm), name = .inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con)))
    }},
    add = TRUE
  )

  domains <- concepts %>% dplyr::distinct(.data$domain_id) %>% dplyr::pull() %>% tolower()
  domains <- domains[!is.na(domains)] # remove NAs
  domains <- domains[domains %in% c("condition", "drug", "procedure", "observation", "measurement", "visit", "device")]

  if (length(domains) == 0){
    cli::cli_warn("None of the input concept IDs found for the cdm reference - returning an empty cohort")
    cdm <- insertTable(cdm = cdm,
                       name = name,
                       table = dplyr::tibble(
                         cohort_definition_id = numeric(),
                         subject_id = numeric(),
                         cohort_start_date = as.Date(character()),
                         cohort_end_date = as.Date(character())),
                       overwrite = overwrite)

    cdm[[name]] <- omopgenerics::newCohortTable(
      table = cdm[[name]],
      cohortSetRef = cohort_set_ref
    )
    return(cdm)
  }

  # check we have references to all required tables ----
  missing_tables <- dplyr::setdiff(table_refs(domain_id = domains) %>% dplyr::pull("table_name"), names(cdm))

  if (length(missing_tables) > 0) {
    s <- ifelse(length(missing_tables) > 1, "s", "")
    is <- ifelse(length(missing_tables) > 1, "are", "is")
    missing_tables <- paste(missing_tables, collapse = ", ")
    cli::cli_warn("Concept set includes concepts from the {missing_tables} table{s} which {is} not found in the cdm reference and will be skipped.")

    domains <- table_refs(domain_id = domains) %>%
      dplyr::filter(!(.data$table_name %in% missing_tables)) %>%
      dplyr::pull("domain_id")
  }

  # rowbind results from clinical data tables ----
  get_domain <- function(domain, cdm, concepts) {
    df <- table_refs(domain_id = domain)

    if (isFALSE(df$table_name %in% names(cdm))) {
      return(NULL)
    }

    by <- rlang::set_names("concept_id", df[["concept_id"]])

    cdm[[df$table_name]] %>%
      dplyr::inner_join(concepts, by = local(by)) %>%
      dplyr::transmute(.data$cohort_definition_id,
                       subject_id = .data$person_id,
                       cohort_start_date = !!rlang::parse_expr(df$start_date),
                       cohort_end_date = dplyr::coalesce(!!rlang::parse_expr(df$end_date),
                                                         !!dateadd(df$start_date, 1))) %>%
      # silently ignore cdm records where end date is before start.
      # Another reasonable option could be to set end to start if end is before start.
      dplyr::filter(cohort_start_date <= cohort_end_date)
  }

  if (length(domains) == 0) {
    cohort <- NULL
  } else {
    cohort <- purrr::map(domains, ~get_domain(., cdm = cdm, concepts = concepts)) %>%
      purrr::reduce(dplyr::union_all)
  }

  if (is.null(cohort)) {
    # no domains included. Create empty cohort.
    cohort <- dplyr::tibble(
      cohort_definition_id = integer(),
      subject_id = integer(),
      cohort_start_date = as.Date(x = integer(0), origin = "1970-01-01"),
      cohort_end_date = as.Date(x = integer(0), origin = "1970-01-01")
    )
    cdm <- omopgenerics::insertTable(
      cdm = cdm, name = name, table = cohort, overwrite = overwrite
    )
    cohortRef <- cdm[[name]]
  } else {

    # drop any outside of an observation period
    obs_period <- cdm[["observation_period"]] %>%
      dplyr::select("subject_id" = "person_id",
                    "observation_period_start_date",
                    "observation_period_end_date")

    # subset to target cohort
    if (!is.null(subsetCohort)) {
      obs_period <- cdm[[subsetCohort]] %>%
        {if (!is.null(subsetCohortId)) dplyr::filter(., .data$cohort_definition_id %in% .env$subsetCohortId) else .} %>%
        dplyr::distinct(.data$subject_id) %>%
        dplyr::inner_join(obs_period, by = "subject_id")
    }

    # TODO remove this variable since it is confusing
    cohort_start_date <- "cohort_start_date"

    cohortRef <- cohort %>%
      dplyr::inner_join(obs_period, by = "subject_id") %>%
      # TODO fix dplyr::between sql translation, also pmin.
      dplyr::filter(.data$observation_period_start_date <= .data$cohort_start_date  &
                    .data$cohort_start_date <= .data$observation_period_end_date) %>%
      {if (requiredObservation[1] > 0) dplyr::filter(., !!dateadd("observation_period_start_date",
                                                                  requiredObservation[1]) <=.data$cohort_start_date) else .} %>%
      {if (requiredObservation[2] > 0) dplyr::filter(., !!dateadd("cohort_start_date",
                                                                  requiredObservation[2]) <= .data$observation_period_end_date) else .} %>%
      {if (end == "observation_period_end_date") dplyr::mutate(., cohort_end_date = .data$observation_period_end_date) else .} %>%
      {if (is.numeric(end)) dplyr::mutate(., cohort_end_date = !!dateadd("cohort_start_date", end)) else .} %>%
      dplyr::mutate(cohort_end_date = dplyr::case_when(
        .data$cohort_end_date > .data$observation_period_end_date ~ .data$observation_period_end_date,
        TRUE ~ .data$cohort_end_date)) %>%
      dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
      # TODO order_by = .data$cohort_start_date
      {if (limit == "first") dplyr::slice_min(., n = 1, order_by = cohort_start_date, by = c("cohort_definition_id", "subject_id")) else .} %>%
      cohort_collapse() %>%
      dplyr::mutate(cohort_start_date = as.Date(.data$cohort_start_date),
                    cohort_end_date = as.Date(.data$cohort_end_date)) %>%
      dplyr::compute(name = name, temporary = FALSE, overwrite = overwrite)
  }

  cohortCountRef <- cohort_set_ref  %>%
    dplyr::left_join(cohortRef %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(
      number_records = dplyr::n(),
      number_subjects = dplyr::n_distinct(.data$subject_id)) %>%
    dplyr::collect(),
    by = "cohort_definition_id")  %>%
    dplyr::mutate(number_records = dplyr::if_else(is.na(.data$number_records),
                                                  0L,
                                                  as.integer(.data$number_records)),
                  number_subjects = dplyr::if_else(is.na(.data$number_subjects),
                                                  0L,
                                                  as.integer(.data$number_subjects)))

  cohortAttritionRef <- cohort_set_ref %>%
    dplyr::select("cohort_definition_id") %>%
    dplyr::distinct() %>%
    dplyr::left_join(cohortCountRef, by = "cohort_definition_id") %>%
    dplyr::mutate(
      number_records = dplyr::coalesce(.data$number_records, 0L),
      number_subjects = dplyr::coalesce(.data$number_subjects, 0L),
      reason_id = 1,
      reason = "Initial qualifying events",
      excluded_records = 0,
      excluded_subjects = 0)

    cohortCodelistRef <- df  %>%
      dplyr::mutate(type = "index event") %>%
      dplyr::select("cohort_definition_id",
                    "codelist_name" = "cohort_name",
                    "concept_id",
                    "type")

    cdm[[name]] <- omopgenerics::newCohortTable(
      table = cohortRef,
      cohortSetRef = cohort_set_ref,
      cohortAttritionRef = cohortAttritionRef,
      cohortCodelistRef = cohortCodelistRef
    )

  return(cdm)
}

#' @rdname generateConceptCohortSet
#' @export
generate_concept_cohort_set <- function(cdm,
                                        concept_set = NULL,
                                        name = "cohort",
                                        limit = "first",
                                        required_observation = c(0,0),
                                        end = "observation_period_end_date",
                                        subset_cohort = NULL,
                                        subset_cohort_id = NULL,
                                        overwrite = TRUE) {

  generateConceptCohortSet(cdm = cdm,
                           conceptSet = concept_set,
                           name = name,
                           limit = limit,
                           requiredObservation = required_observation,
                           end = end,
                           subsetCohort = subset_cohort,
                           subsetCohortId = subset_cohort_id,
                           overwrite = overwrite)
}

