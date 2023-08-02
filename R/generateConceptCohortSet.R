

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
#' @param requiredObservation A numeric vector of length 2 that specifies the number of days of
#' required observation time prior to index and post index for an event to be included in the cohort.
#' @param end How should the `cohort_end_date` be defined?
#' \itemize{
#'  \item{"observation_period_end_date" (default): The earliest observation_period_end_date after the event start date}
#'  \item{numeric scalar: A fixed number of days from the event start date}
#'  \item{"event_end_date"}: The event end date. If the event end date is not populated then the event start date will be used
#' }
#' @param overwrite Should the cohort table be overwritten if it already exists? TRUE or FALSE
#'
#' @return A cdm reference object with the new generated cohort set table added
#' @export
generateConceptCohortSet <- function(cdm,
                                     conceptSet = NULL,
                                     name = "cohort",
                                     limit = "first",
                                     requiredObservation = c(0,0),
                                     end = "observation_period_end_date",
                                     overwrite = FALSE) {

  # check cdm ----
  checkmate::checkClass(cdm, "cdm_reference")
  con <- attr(cdm, "dbcon")
  checkmate::assertTRUE(DBI::dbIsValid(attr(cdm, "dbcon")))

  assertTables(cdm, "observation_period", empty.ok = FALSE)
  assertWriteSchema(cdm)

  # check name ----
  checkmate::assertLogical(overwrite, len = 1, any.missing = FALSE)
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE, min.chars = 1, pattern = "[a-z1-9_]+")
  existingTables <- listTables(con, attr(cdm, "write_schema"))

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
      dplyr::select("cohort_definition_id",
                    "cohort_name",
                    concept_id = "conceptId",
                    include_descendants = "includeDescendants",
                    is_excluded = "isExcluded")

  } else {
    # conceptSet must be a named list of integer-ish vectors
    purrr::walk(conceptSet, ~checkmate::assert_integerish(., lower = 0, min.len = 1, any.missing = FALSE))

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

  # upload concept data to the database ----
  tempName <- uniqueTableName()

  DBI::dbWriteTable(attr(cdm, "dbcon"),
                    name = inSchema(attr(cdm, "write_schema"), tempName, dbms = dbms(con)),
                    value = df,
                    overwrite = TRUE)

  if (any(df$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }

  # realize full list of concepts ----
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

  # check we have references to all required tables ----
  missing_tables <- dplyr::setdiff(table_refs(domain_id = domains) %>% dplyr::pull("table_name"), names(cdm))

  if (length(missing_tables) > 0) {
    s <- ifelse(length(missing_tables) > 1, "s", "")
    is <- ifelse(length(missing_tables) > 1, "are", "is")
    missing_tables <- paste(missing_tables, collapse = ", ")
    cli::cli_abort("Concept set includes concepts from the {missing_tables} table{s} which {is} not found in the cdm reference!")
  }

  # rowbind results from clinical data tables ----
  get_domain <- function(domain, cdm, concepts) {
    df <- table_refs(domain_id = domain)

    CDMConnector::assert_tables(cdm, df$table_name, empty.ok = TRUE)

    by <- rlang::set_names("concept_id", df[["concept_id"]])

    cdm[[df$table_name]] %>%
      dplyr::inner_join(concepts, by = local(by)) %>%
      dplyr::transmute(.data$cohort_definition_id,
                       subject_id = .data$person_id,
                       cohort_start_date = !!rlang::parse_expr(df$start_date),
                       cohort_end_date = dplyr::coalesce(!!rlang::parse_expr(df$end_date),
                                                         !!dateadd(df$start_date, 1)))
  }

  cohort <- purrr::map(domains, ~get_domain(., cdm = cdm, concepts = concepts)) %>%
    purrr::reduce(dplyr::union_all)

  # drop any outside of an observation period
  obs_period <- cdm[["observation_period"]] %>%
    dplyr::select("subject_id" = "person_id",
                  "observation_period_start_date",
                  "observation_period_end_date")

  cohort_start_date <- "cohort_start_date"

  cohortRef <- cohort %>%
    dplyr::inner_join(obs_period, by = "subject_id") %>%
    # TODO fix dplyr::between sql translation, also pmin.
    dplyr::filter(.data$observation_period_start_date <= .data$cohort_start_date  &
                  .data$cohort_start_date <= .data$observation_period_end_date) %>%
    {if (requiredObservation[1] > 0) dplyr::filter(., !!dateadd("observation_period_start_date", requiredObservation[1]) <=.data$cohort_start_date) else .} %>%
    {if (requiredObservation[2] > 0) dplyr::filter(., !!dateadd("cohort_start_date", requiredObservation[2]) >= .data$observation_period_end_date) else .} %>%
    {if (end == "observation_period_end_date") dplyr::mutate(., cohort_end_date = .data$observation_period_end_date) else .} %>%
    {if (is.numeric(end)) dplyr::mutate(., cohort_end_date = !!dateadd("cohort_start_date", end)) else .} %>%
    dplyr::mutate(cohort_end_date = dplyr::case_when(
      .data$cohort_end_date > .data$observation_period_end_date ~ .data$observation_period_end_date,
      TRUE ~ .data$cohort_end_date)) %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    # TODO order_by = .data$cohort_start_date
    {if (limit == "first") dplyr::slice_min(., n = 1, order_by = cohort_start_date, by = c("cohort_definition_id", "subject_id")) else .} %>%
    cohort_collapse() %>%
    computeQuery(temporary = FALSE,
                 schema = attr(cdm, "write_schema"),
                 name = name,
                 overwrite = overwrite)

  cohortRef <- cohortRef %>%
    dplyr::mutate(cohort_start_date = as.Date(.data$cohort_start_date),
                  cohort_end_date = as.Date(.data$cohort_end_date))

  cohortSetRef <- concepts %>%
    dplyr::distinct(.data$cohort_definition_id, .data$cohort_name) %>%
    CDMConnector::computeQuery(temporary = getOption("CDMConnector.cohort_as_temp", FALSE),
                               schema = attr(cdm, "write_schema"),
                               name = paste0(name, "_set"),
                               overwrite = overwrite)

  cdm[[name]] <- newGeneratedCohortSet(
    cohortRef = cohortRef,
    cohortSetRef = cohortSetRef,
    writeSchema = attr(cdm, "write_schema"),
    overwrite = overwrite)

  return(cdm)
}

#' @rdname generateConceptCohortSet
#' @export
generate_concept_cohort_set <- function(cdm,
                                        concept_set = NULL,
                                        name = "cohort",
                                        end = "observation_period_end_date",
                                        overwrite = FALSE) {
  generateConceptCohortSet(cdm = cdm,
                           conceptSet = concept_set,
                           name = name,
                           end = end,
                           overwrite = overwrite)

}


