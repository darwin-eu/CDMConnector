library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
cdm <- cdm_from_con(con, "main", "main")



# check cdm ----
checkmate::assertClass(cdm, "cdm_reference")
con <- cdmCon(cdm)
checkmate::assertTRUE(DBI::dbIsValid(cdmCon(cdm)))

assertTables(cdm, "observation_period", empty.ok = FALSE)
assertWriteSchema(cdm)



chrt <- jsonlite::read_json(here::here("scratch/cohort.json"))

# check cohort json
# TODO use jsonschema

str(chrt, max.level = 1)
str(chrt$ConceptSets, max.level = 2)
length(chrt$ConceptSets[[1]]$expression$items)
str(chrt$ConceptSets[[1]]$expression$items)


codesets <- dplyr::tibble(cs = chrt$ConceptSets) %>%
  dplyr::mutate(codeset_id = purrr::map_int(cs, "id"),
                concept_id = purrr::map(cs, ~purrr::map_int(.$expression$items, ~.[[1]]$"CONCEPT_ID"))) %>%
  dplyr::select("codeset_id", "concept_id") %>%
  tidyr::unnest(cols = "concept_id")

missing_codesets <- dplyr::setdiff(purrr::map_int(chrt$ConceptSets, "id"), unique(codesets$codeset_id))
if (length(missing_codesets) > 0) {
  s <- ifelse(length(missing_codesets) > 1, "s", "")
  rlang::abort(glue::glue("Codeset{s} {paste(missing_codesets, collapse = ', ')} are empty!
                          Please remove empty concept sets from your cohort definition." ))
}

write_schema <- cdmWriteSchema(cdm)
DBI::dbWriteTable(con, inSchema(write_schema, "codesets", dbms(con)), codesets)

codesets <- dplyr::tbl(con, inSchema(write_schema, "codesets")) %>%
  compute_query()

DBI::dbRemoveTable(con, inSchema(write_schema, "codesets", dbms(con)))




# generateConceptCohortSet <- function(cdm,
#                                      conceptSet = NULL,
#                                      name = "cohort",
#                                      limit = "first",
#                                      requiredObservation = c(0,0),
#                                      end = "observation_period_end_date",
#                                      overwrite = FALSE) {



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
  tempName <- paste0("tmp", as.integer(Sys.time()), "_")

  DBI::dbWriteTable(cdmCon(cdm),
                    name = inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con)),
                    value = df,
                    overwrite = TRUE)

  if (any(df$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }

  # realize full list of concepts ----
  concepts <- dplyr::tbl(cdmCon(cdm), inSchema(cdmWriteSchema(cdm),
                                                      tempName,
                                                      dbms = dbms(con))) %>%
    dplyr::rename_all(tolower) %>%
    { if (any(df$include_descendants)) {
      dplyr::filter(., .data$include_descendants) %>%
        dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
        dplyr::select("cohort_definition_id", "cohort_name", concept_id = "descendant_concept_id", "is_excluded") %>%
        dplyr::union_all(dplyr::select(dplyr::tbl(cdmCon(cdm), inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con))), "cohort_definition_id", "cohort_name", "concept_id", "is_excluded"))
    } else . } %>%
    dplyr::filter(.data$is_excluded == FALSE) %>%
    dplyr::left_join(dplyr::select(cdm$concept, "concept_id", "domain_id"), by = "concept_id") %>%
    dplyr::select("cohort_definition_id", "cohort_name", "concept_id", "domain_id") %>%
    dplyr::distinct() %>%
    CDMConnector::computeQuery(temporary = TRUE)

  DBI::dbRemoveTable(cdmCon(cdm), name = inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con)))

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
                 schema = cdmWriteSchema(cdm),
                 name = name,
                 overwrite = overwrite)

  cohortRef



  cohortRef <- cohortRef %>%
    dplyr::mutate(cohort_start_date = as.Date(.data$cohort_start_date),
                  cohort_end_date = as.Date(.data$cohort_end_date))

  cohortSetRef <- concepts %>%
    dplyr::distinct(.data$cohort_definition_id, .data$cohort_name) %>%
    CDMConnector::computeQuery(temporary = getOption("CDMConnector.cohort_as_temp", FALSE),
                               schema = cdmWriteSchema(cdm),
                               name = paste0(name, "_set"),
                               overwrite = overwrite)

  cdm[[name]] <- cohortRef

  cdm[[name]] <- omopgenerics::newCohortTable(
    cohortRef = cdm[[name]], cohortSetRef = cohortSetRef, overwrite = overwrite
  )

  # return(cdm)
# }



SELECT co.*
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)

codesets
