
# Create a new generated cohort set from a list of concept sets
#
# @description
#
# Generate a new cohort set from one or more concept sets. Each
# concept set will result in one cohort and represent the time during which
# the concept was observed for each subject/person. Concept sets can be
# passed to this function as:
# \itemize{
#  \item{A named list of numeric vectors, one vector per concept set}
#  \item{A named list of Capr concept sets}
# }
#
# Clinical observation records will be looked up in the respective domain tables
# using the vocabulary in the CDM. If a required domain table does not exist in
# the cdm object a warning will be given.
# Concepts that are not in the vocabulary or in the data will be silently ignored.
# If end dates are missing or do not exist, as in the case of the procedure and
# observation domains, the the start date will be used as the end date.
#
#
#
# @param cdm A cdm reference object created by `CDMConnector::cdmFromCon` or `CDMConnector::cdm_from_con`
# @param conceptSet A named list of numeric vectors or Capr concept sets
# @param name The name of the new generated cohort table as a character string
# @param overwrite Should the cohort table be overwritten if it already exists? TRUE or FALSE
# @param restrictToObservationPeriod Should the cohort be restricted to the observation period? TRUE or FALSE
# @param temporary Should the new cohort table be created as a temporary table? TRUE or FALSE
#
# @return A cdm reference object with the new generated cohort set table added
# @export
generateConceptCohortSet <- function(cdm,
                                     conceptSet = NULL,
                                     name = "cohort",
                                     overwrite = FALSE,
                                     computeAttrition = TRUE) {

  checkmate::checkClass(cdm, "cdm_reference")
  checkmate::assertTRUE(DBI::dbIsValid(attr(cdm, "dbcon")))

  if (!is.list(conceptSet)) {
    conceptSet <- list("unnamed cohort" = conceptSet)
  }

  checkmate::assertList(conceptSet, min.len = 1, any.missing = FALSE, types = c("numeric", "ConceptSet"), names = "named")
  checkmate::assertLogical(restrictToObservationPeriod, len = 1)
  checkmate::assertLogical(temporary, len = 1)

  if (restrictToObservationPeriod) rlang::warn("restrictToObservationPeriod is not implemented yet!")

  if (isFALSE(temporary) && is.null(attr(cdm, "write_schema"))) {
    rlang::abort("write_schema is required when temporary is FALSE")
  }

  checkmate::assertList(conceptSet, min.len = 1, names = "named")
  CDMConnector::assert_tables(cdm, "concept")

  if (methods::is(conceptSet[[1]], "ConceptSet")) {
    purrr::walk(conceptSet, ~checkmate::assertClass(., "ConceptSet"))

    df <- dplyr::tibble(cohort_definition_id = seq_along(conceptSet),
                        cohort_name = names(conceptSet),
                        df = purrr::map(conceptSet, as.data.frame)) %>%
      tidyr::unnest(cols = df) %>%
      dplyr::select(cohort_definition_id,
                    cohort_name,
                    concept_id = conceptId,
                    include_descendants = includeDescendants,
                    is_excluded = isExcluded)

  } else {
    # conceptSet must be a named list of integer-ish vectors
    purrr::walk(conceptSet, ~checkmate::assert_integerish(., lower = 0, min.len = 1))

    df <- dplyr::tibble(cohort_definition_id = seq_along(conceptSet),
                        cohort_name = names(conceptSet),
                        concept_id = conceptSet) %>%
      tidyr::unnest(cols = concept_id) %>%
      dplyr::transmute(cohort_definition_id,
                       cohort_name,
                       concept_id,
                       include_descendants = FALSE,
                       is_excluded = FALSE)
  }

  # upload data to the database
  tempName <- uniqueTableName()
  DBI::dbWriteTable(attr(cdm, "dbcon"), name = tempName, value = df, temporary = TRUE, overwrite = TRUE)

  if (any(df$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }

  # realize full list of concepts
  concepts <- dplyr::tbl(attr(cdm, "dbcon"), tempName) %>%
    { if (any(df$include_descendants)) {
      dplyr::filter(., include_descendants) %>%
        dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
        dplyr::select(cohort_definition_id, cohort_name, concept_id = descendant_concept_id, is_excluded) %>%
        dplyr::union_all(dplyr::select(dplyr::tbl(attr(cdm, "dbcon"), tempName), "cohort_definition_id", "cohort_name", "concept_id", "is_excluded"))
    } else . } %>%
    dplyr::filter(!is_excluded) %>%
    dplyr::left_join(dplyr::select(cdm$concept, "concept_id", "domain_id"), by = "concept_id") %>%
    dplyr::select("cohort_definition_id", "cohort_name", "concept_id", "domain_id") %>%
    dplyr::distinct() %>%
    CDMConnector::computeQuery(temporary = TRUE)

  domains <- dplyr::distinct(concepts, domain_id) %>% pull() %>% tolower()

  get_domain <- function(domain) {
    df <- dplyr::tribble(
      ~domain_id,    ~table_name,            ~concept_id,              ~start_date,                  ~end_date,
      "condition",   "condition_occurrence", "condition_concept_id",   "condition_start_date",       "condition_end_date",
      "drug",        "drug_exposure",        "drug_concept_id",        "drug_exposure_start_date",   "drug_exposure_end_date",
      "procedure",   "procedure_occurrence", "procedure_concept_id",   "procedure_date",             "procedure_date",
      "observation", "observation",          "observation_concept_id", "observation_date",           "observation_date",
      "measurement", "measurement",          "measurement_concept_id", "measurement_date",           "measurement_date",
      "visit",       "visit_occurrence",     "visit_concept_id",       "visit_start_date",           "visit_end_date",
      "device",      "device_exposure",      "device_concept_id",      "device_exposure_start_date", "device_exposure_end_date"
    ) %>% dplyr::filter(domain_id == domain)

    if (nrow(df) != 1) {
      return(NULL)
    }

    if (!(df$table_name %in% names(cdm))) {
      rlang::warn(glue::glue("Concept set contains {domain} concepts but the {df$table_name} table is missing from the cdm"))
      return(NULL)
    }

    CDMConnector::assert_tables(cdm, df$table_name, empty.ok = TRUE)

    by <- c("concept_id") %>% rlang::set_names(df[["concept_id"]])

    cdm[[df$table_name]] %>%
      dplyr::inner_join(concepts, by = local(by)) %>%
      dplyr::transmute(cohort_definition_id,
                       subject_id = person_id,
                       cohort_start_date = !!rlang::parse_expr(df$start_date),
                       cohort_end_date = dplyr::coalesce(!!rlang::parse_expr(df$end_date),
                                                         !!rlang::parse_expr(df$start_date)))
  }

  # rowbind results
  if (!is.na(domains) && length(domains) > 0) {
    cohort <- purrr::map(domains, get_domain) %>% purrr::reduce(dplyr::union_all)
  } else {
    # create the base cohort table
    cohort <- dplyr::transmute(
      cdm$observation_period,
      cohort_definition_id = 0L,
      subject_id = .data$person_id,
      cohort_start_date = .data$observation_period_start_date,
      cohort_end_date = .data$observation_period_end_date) %>%
      dplyr::filter(1 == 0) %>%
      computeQuery(temporary = TRUE)
  }

  checkmate::assertCharacter(attr(cdm, "write_prefix"), len = 1, null.ok = TRUE, min.chars = 1)
  cohort_table_name <- paste0(attr(cdm, "write_prefix"), name) # This works when write_prefix is null

  # debugonce(cohort_collapse)
  # TODO fix overwrite in cdmconnector when temp table exists
  cohortRef <- cohort %>%
    cohort_collapse() %>%
    CDMConnector::computeQuery(temporary = temporary,
                               schema = attr(cdm, "write_schema"),
                               name = cohort_table_name,
                               overwrite = overwrite)

  # create attributes
  cohortSetRef <- dplyr::distinct(concepts, cohort_definition_id, cohort_name) %>%
    computeQuery(temporary = temporary,
                 schema = attr(cdm, "write_schema"),
                 name = paste0(cohort_table_name, "_set"),
                 overwrite = overwite)

  # DBI::dbRemoveTable(con, "cohort2_count")

  cohortCountRef <- cohortRef %>%
    dplyr::group_by(cohort_definition_id) %>%
    dplyr::summarise(n_subjects = dplyr::n_distinct(subject_id),
                     n_records = dplyr::n()) %>%
    dplyr::left_join(cohortSetRef, ., by = "cohort_definition_id") %>%
    dplyr::mutate(number_subjects = ifelse(is.na(n_subjects), 0L, n_subjects),
                  number_records  = ifelse(is.na(n_subjects), 0L, n_subjects)) %>%
    dplyr::select("cohort_definition_id", "number_records", "number_subjects") %>%
    CDMConnector::computeQuery(temporary = temporary,
                               schema = attr(cdm, "write_schema"),
                               name = paste0(cohort_table_name, "_count"),
                               overwrite = overwrite)

  # DBI::dbRemoveTable(con, "cohort2_attrition")
  if (computeAttrition) {
    cohortAttritionRef <- cohortCountRef %>%
      dplyr::transmute(
        cohort_definition_id,
        number_records,
        number_subjects,
        reason_id = 1L,
        reason = "Qualifying initial records",
        excluded_records = 0L,
        excluded_subjects = 0L) %>%
      CDMConnector::computeQuery(temporary = temporary,
                                 schema = attr(cdm, "write_schema"),
                                 name = paste0(cohort_table_name, "_attrition"),
                                 overwrite = overwrite)
  } else {
    cohortAttritionRef <- NULL
  }

  cdm[[name]] <- CDMConnector::newGeneratedCohortSet(
    cohortRef = cohortRef,
    cohortSetRef = cohortSetRef,
    cohortAttritionRef = cohortAttritionRef,
    cohortCountRef = cohortCountRef)

  return(cdm)

}
