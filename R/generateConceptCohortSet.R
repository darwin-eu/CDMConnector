#' Internal function to create a new generated cohort set from a list of concept sets
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
#' @param conceptSet A named list of numeric vectors or Capr concept sets
#' @param name The name of the new generated cohort table as a character string
#' @param overwrite Should the cohort table be overwritten if it already exists? TRUE or FALSE
#'
#' @return A cdm reference object with the new generated cohort set table added
#' @export
generateConceptCohortSet <- function(cdm,
                                     conceptSet = NULL,
                                     name = "cohort",
                                     overwrite = FALSE) {

  checkmate::checkClass(cdm, "cdm_reference")
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(attr(cdm, "dbcon")))

  if (isFALSE(getOption("CDMConnector.cohort_as_temp", FALSE))){
    assert_write_schema(cdm)
  }

  if (!is.list(conceptSet) || methods::is(conceptSet, "ConceptSet")) {
    conceptSet <- list("unnamed cohort" = conceptSet)
  }

  checkmate::assertList(conceptSet, min.len = 1, any.missing = FALSE, types = c("numeric", "ConceptSet"), names = "named")
  checkmate::assertList(conceptSet, min.len = 1, names = "named")
  CDMConnector::assert_tables(cdm, "concept")

  if (methods::is(conceptSet[[1]], "ConceptSet")) {
    purrr::walk(conceptSet, ~checkmate::assertClass(., "ConceptSet"))

    df <- dplyr::tibble(cohort_definition_id = seq_along(conceptSet),
                        cohort_name = names(conceptSet),
                        df = purrr::map(conceptSet, caprConceptToDataframe)) %>%
      tidyr::unnest(cols = df) %>%
      dplyr::select("cohort_definition_id",
                    "cohort_name",
                    concept_id = "conceptId",
                    include_descendants = "includeDescendants",
                    is_excluded = "isExcluded")

  } else {
    # conceptSet must be a named list of integer-ish vectors
    purrr::walk(conceptSet, ~checkmate::assert_integerish(., lower = 0, min.len = 1))

    df <- dplyr::tibble(cohort_definition_id = seq_along(.env$conceptSet),
                        cohort_name = names(.env$conceptSet),
                        concept_id = .env$conceptSet) %>%
      tidyr::unnest(cols = "concept_id") %>%
      dplyr::transmute(.data$cohort_definition_id,
                       .data$cohort_name,
                       .data$concept_id,
                       include_descendants = FALSE,
                       is_excluded = FALSE)
  }

  # upload data to the database
  tempName <- uniqueTableName()

  DBI::dbWriteTable(attr(cdm, "dbcon"),
                    name = inSchema(attr(cdm, "write_schema"),
                                    tempName, dbms = dbms(con)),
                    value = df,
                    overwrite = TRUE)

  if (any(df$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }

  # realize full list of concepts
  concepts <- dplyr::tbl(attr(cdm, "dbcon"), inSchema(attr(cdm, "write_schema"),
                                                      tempName,
                                                      dbms = dbms(con))) %>%
    { if (any(df$include_descendants)) {
      dplyr::filter(., .data$include_descendants) %>%
        dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
        dplyr::select("cohort_definition_id", "cohort_name", concept_id = "descendant_concept_id", "is_excluded") %>%
        dplyr::union_all(dplyr::select(dplyr::tbl(attr(cdm, "dbcon"), inSchema(attr(cdm, "write_schema"), tempName, dbms = dbms(con))), "cohort_definition_id", "cohort_name", "concept_id", "is_excluded"))
    } else . } %>%
    dplyr::filter(!.data$is_excluded) %>%
    dplyr::left_join(dplyr::select(cdm$concept, "concept_id", "domain_id"), by = "concept_id") %>%
    dplyr::select("cohort_definition_id", "cohort_name", "concept_id", "domain_id") %>%
    dplyr::distinct() %>%
    CDMConnector::computeQuery(temporary = TRUE)

  DBI::dbRemoveTable(attr(cdm, "dbcon"), name = inSchema(attr(cdm, "write_schema"), tempName, dbms = dbms(con)))

  domains <- concepts %>% dplyr::distinct(.data$domain_id) %>% dplyr::pull() %>% tolower()


  # check we have references to all required tables
  missing_tables <- setdiff(table_refs(domain_id = domains) %>% dplyr::pull("table_name"),
                            names(cdm))

  if (length(missing_tables)>0) {
    cli::cli_abort("Concepts included from the {.missing_tables {missing_tables}} table{?s} but table{?s} not found in the cdm reference")
  }

  # rowbind results
  cohort <- purrr::map(domains,
                       get_domain,
                       cdm = cdm,
                       concepts = concepts) %>%
    purrr::reduce(dplyr::union_all)

  # drop any outside of an observation period
  cohort <- cohort  %>%
    dplyr::inner_join(
      cdm[["observation_period"]] %>%
        dplyr::rename("subject_id" = "person_id") %>%
        dplyr::select(
          "subject_id",
          "observation_period_start_date",
          "observation_period_end_date"
        ),
      by = "subject_id"
    ) %>%
    dplyr::mutate(in_observation = dplyr::if_else(
                  .data$observation_period_start_date <=
                    .data$cohort_start_date  &
                    .data$observation_period_end_date >=
                    .data$cohort_start_date,1,0)) %>%
    dplyr::filter(.data$in_observation == 1) %>%
    dplyr::select("cohort_definition_id", "subject_id",
                  "cohort_start_date", "cohort_end_date") %>%
    dplyr::distinct()

  cohortRef <- cohort %>%
    cohort_collapse() %>%
    computeQuery(temporary = getOption("CDMConnector.cohort_as_temp", FALSE),
                 schema = attr(cdm, "write_schema"),
                 name = name,
                 overwrite = overwrite)

  # create attributes
  cohortSetRef <- concepts %>%
    dplyr::distinct(.data$cohort_definition_id, .data$cohort_name) %>%
    CDMConnector::computeQuery(temporary = getOption("CDMConnector.cohort_as_temp", FALSE),
                               schema = attr(cdm, "write_schema"),
                               name = paste0(name, "_set"),
                               overwrite = overwrite)

  cohortCountRef <- cohortRef %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(n_subjects = dplyr::n_distinct(.data$subject_id),
                     n_records = dplyr::n()) %>%
    dplyr::left_join(cohortSetRef, ., by = "cohort_definition_id") %>%
    dplyr::mutate(number_subjects = ifelse(is.na(.data$n_subjects), 0L, .data$n_subjects),
                  number_records  = ifelse(is.na(.data$n_records), 0L, .data$n_records)) %>%
    dplyr::select("cohort_definition_id", "number_records", "number_subjects") %>%
    computeQuery(temporary = getOption("CDMConnector.cohort_as_temp", FALSE),
                 schema = attr(cdm, "write_schema"),
                 name = paste0(name, "_count"),
                 overwrite = overwrite)

  cohortAttritionRef <- cohortCountRef %>%
    dplyr::transmute(
      .data$cohort_definition_id,
      .data$number_records,
      .data$number_subjects,
      reason_id = 1L,
      reason = "Qualifying initial records",
      excluded_records = 0L,
      excluded_subjects = 0L) %>%
      computeQuery(temporary = getOption("CDMConnector.cohort_as_temp", FALSE),
                   schema = attr(cdm, "write_schema"),
                   name = paste0(name, "_attrition"),
                   overwrite = overwrite)

  cdm[[name]] <- CDMConnector::newGeneratedCohortSet(
    cohortRef = cohortRef,
    cohortSetRef = cohortSetRef,
    cohortAttritionRef = cohortAttritionRef,
    cohortCountRef = cohortCountRef)

  return(cdm)
}


get_domain <- function(domain, cdm, concepts) {
  df <- table_refs(domain_id = domain)

  CDMConnector::assert_tables(cdm, df$table_name, empty.ok = TRUE)

  by <- c("concept_id") %>% rlang::set_names(df[["concept_id"]])

  cdm[[df$table_name]] %>%
    dplyr::inner_join(concepts, by = local(by)) %>%
    dplyr::transmute(.data$cohort_definition_id,
                     subject_id = .data$person_id,
                     cohort_start_date = !!rlang::parse_expr(df$start_date),
                     cohort_end_date = dplyr::coalesce(!!rlang::parse_expr(df$end_date),
                                                       !!rlang::parse_expr(df$start_date)))
}

table_refs <- function(domain_id){
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
