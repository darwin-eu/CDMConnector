# Copyright 2022 DARWIN EU®
#
# This file is part of DrugUtilisation
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


#' Subset a cdm to the individuals in one or more cohorts
#'
#' `cdmSubset` will return a new cdm object that contains lazy queries pointing
#' to each of the cdm tables but subset to individuals in a generated cohort.
#' Since the cdm tables are lazy queries, the subset operation will only be
#' done when the tables are used. `computeQuery` can be used to run the SQL
#' used to subset a cdm table and store it as a new table in the database.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @param cdm A cdm_reference object
#' @param cohortTable Then name of a cohort table in the cdm reference
#' @param cohortId IDs of the cohorts that we want to subset from the cohort
#' table. If NULL (default) all cohorts in cohort table are considered.
#' @param verbose Should subset messages be printed? TRUE or FALSE (default)
#'
#' @return A modified cdm_reference with all clinical tables subset
#' to just the persons in the selected cohorts.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#'
#' cdm <- cdm_from_con(con, cdm_schema = "main", write_schema = "main")
#'
#' # generate a cohort
#' path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
#'
#' cohortSet <- readCohortSet(path) %>%
#'   filter(cohort_name == "GIBleed_male")
#'
#' # subset cdm to persons in the generated cohort
#' cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
#'
#' cdmGiBleed <- cdmSubsetCohort(cdm, cohortTable = "gibleed")
#'
#' cdmGiBleed$person %>%
#'   tally()
#' #> # Source:   SQL [1 x 1]
#' #> # Database: DuckDB 0.6.1
#' #>       n
#' #>   <dbl>
#' #> 1   237
#'
#' cdm$person %>%
#'   tally()
#' #> # Source:   SQL [1 x 1]
#' #> # Database: DuckDB 0.6.1
#' #>       n
#' #>   <dbl>
#' #> 1  2694
#'
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmSubsetCohort <- function(cdm,
                            cohortTable = "cohort",
                            cohortId = NULL,
                            verbose = FALSE) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(cohortTable, len = 1)
  checkmate::assertTRUE(cohortTable %in% names(cdm))
  checkmate::assertClass(cdm[[cohortTable]], "GeneratedCohortSet")
  checkmate::assertIntegerish(cohortId, min.len = 1, null.ok = TRUE)
  checkmate::assertLogical(verbose, len = 1)

  if (!("subject_id" %in% colnames(cdm[[cohortTable]]))) {
    m <- glue::glue("subject_id column not in cdm[['{cohortTable}']]")
    rlang::abort(m)
  }

  if (!("cohort_definition_id" %in% colnames(cdm[[cohortTable]]))) {
    n <- glue::glue("cohort_definition_id column not in cdm[['{cohortTable}']]")
    rlang::abort(m)
  }

  subjects <- cdm[[cohortTable]]

  if (!is.null(cohortId)) {
    subjects <- subjects %>%
      dplyr::filter(.data$cohort_definition_id %in% .env$cohortId)
  }

  n_subjects <- subjects %>%
    dplyr::distinct(.data$subject_id) %>%
    dplyr::tally() %>%
    dplyr::pull("n")

  if (n_subjects == 0 && verbose) {
    rlang::inform("Selected cohorts are empty. No subsetting will be done.")
    return(cdm)
  } else if (verbose) {
    rlang::inform(glue::glue("Subsetting cdm to {n_subjects} persons"))
  }

  n_subjects_person_table <- subjects %>%
    dplyr::inner_join(cdm$person, by = c("subject_id" = "person_id")) %>%
    distinct(.data$subject_id) %>%
    dplyr::tally() %>%
    dplyr::pull("n")

  if (n_subjects != n_subjects_person_table) {
    rlang::warn(glue::glue(
      "Not all cohort subjects are present in person table.
       - N cohort subjects: {n_subjects}
       - N cohort subjects in cdm person table: {n_subjects_person_table}"))
  }

  for (i in seq_along(cdm)) {
    if ("person_id" %in% colnames(cdm[[i]])) {
      cdm[[i]] <- cdm[[i]] %>%
        dplyr::semi_join(subjects, by = c("person_id" = "subject_id"))
    }
  }

  return(cdm)
}

#' Subset a cdm object to a random sample of individuals
#'
#' `cdmSample` takes a cdm object and returns a new cdm that includes only a
#' random sample of persons in the cdm. Only `person_id`s in both the person
#' table and observation_period table will be considered.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @param cdm A cdm_reference object
#' @param n Number of persons to include in the cdm
#'
#' @return A modified cdm_reference object where all clinical tables are lazy
#' queries pointing to subset
#'
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#'
#' cdm <- cdm_from_con(con, cdm_schema = "main")
#'
#' cdmSampled <- cdmSample(cdm, n = 2)
#'
#' cdmSampled$person %>%
#'   select(person_id)
#' #> # Source:   SQL [2 x 1]
#' #> # Database: DuckDB 0.6.1
#' #>   person_id
#' #>       <dbl>
#' #> 1       155
#' #> 2      3422
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmSample <- function(cdm,
                      n = 1000) {
  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertIntegerish(n, len = 1, lower = 1, upper = 1e9)

  assert_tables(cdm, "person")

  # take a random sample from the person table
  personSample <- cdm[["person"]] %>%
    dplyr::distinct(.data$person_id) %>%
    dplyr::slice_sample(n = n) %>%
    computeQuery()

  for (i in seq_along(cdm)) {
    if ("person_id" %in% colnames(cdm[[i]])) {
      cdm[[i]] <- cdm[[i]] %>%
        dplyr::semi_join(personSample, by = "person_id")
    } else if ("subject_id" %in% colnames(cdm[[i]])) {
      cdm[[i]] <- cdm[[i]] %>%
        dplyr::semi_join(personSample, by = c("subject_id" = "person_id"))
    }
  }

  cdm[["person_sample"]] <- personSample

  return(cdm)
}

#' Subset a cdm object to a set of persons
#'
#' `cdmSubset` takes a cdm object and a list of person IDs as input. It
#' returns a new cdm that includes data only for persons matching the provided
#' person IDs. Generated cohorts in the cdm will also be subset to
#' the IDs provided.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @param cdm A cdm_reference object
#' @param personId A numeric vector of person IDs to include in the cdm
#'
#' @return A modified cdm_reference object where all clinical tables are lazy
#' queries pointing to subset
#'
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#'
#' cdm <- cdm_from_con(con, cdm_schema = "main")
#'
#' cdm2 <- cdmSubset(cdm, personId = c(2, 18, 42))
#'
#' cdm2$person %>%
#'   select(1:3)
#' #> # Source:   SQL [3 x 3]
#' #> # Database: DuckDB 0.6.1
#' #>   person_id gender_concept_id year_of_birth
#' #>       <dbl>             <dbl>         <dbl>
#' #> 1         2              8532          1920
#' #> 2        18              8532          1965
#' #> 3        42              8532          1909
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmSubset <- function(cdm,
                      personId) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertIntegerish(personId,
                              min.len = 1,
                              max.len = 1e5,
                              null.ok = FALSE)

  personId <- as.integer(personId)

  for (i in seq_along(cdm)) {
    if ("person_id" %in% colnames(cdm[[i]])) {
      cdm[[i]] <- cdm[[i]] %>%
        dplyr::filter(.data$person_id %in% .env$personId)
    } else if ("subject_id" %in% colnames(cdm[[i]])) {
      cdm[[i]] <- cdm[[i]] %>%
        dplyr::filter(.data$subject_id %in% .env$personId)
    }
  }

  return(cdm)
}


#' Flatten a cdm into a single observation table
#'
#' This experimental function transforms the OMOP CDM into a single observation
#' table. This is only recommended for use with a filtered CDM or a cdm that is
#' small in size.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @param cdm A cdm_reference object
#' @param domain Domains to include. Must be a subset of "condition", "drug",
#' "procedure", "measurement", "visit", "death", "observation".
#' @param includeConceptName Should concept_name and type_concept_name be
#' include in the output table? TRUE (default) or FALSE
#'
#' @return A lazy query that when evaluated will result in a single cdm table
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#'
#' cdm <- cdm_from_con(con, cdm_schema = "main")
#'
#' all_observations <- cdmSubset(cdm, personId = c(2, 18, 42)) %>%
#'   cdmFlatten() %>%
#'   collect()
#'
#' all_observations
#' #> # A tibble: 213 × 8
#' #>    person_id observation_…¹ start_date end_date   type_…² domain obser…³ type_…⁴
#' #>        <dbl>          <dbl> <date>     <date>       <dbl> <chr>  <chr>   <chr>
#' #>  1         2       40213201 1986-09-09 1986-09-09  5.81e5 drug   pneumo… <NA>
#' #>  2        18        4116491 1997-11-09 1998-01-09  3.20e4 condi… Escher… <NA>
#' #>  3        18       40213227 2017-01-04 2017-01-04  5.81e5 drug   tetanu… <NA>
#' #>  4        42        4156265 1974-06-13 1974-06-27  3.20e4 condi… Facial… <NA>
#' #>  5        18       40213160 1966-02-23 1966-02-23  5.81e5 drug   poliov… <NA>
#' #>  6        42        4198190 1933-10-29 1933-10-29  3.80e7 proce… Append… <NA>
#' #>  7         2        4109685 1952-07-13 1952-07-27  3.20e4 condi… Lacera… <NA>
#' #>  8        18       40213260 2017-01-04 2017-01-04  5.81e5 drug   zoster… <NA>
#' #>  9        42        4151422 1985-02-03 1985-02-03  3.80e7 proce… Sputum… <NA>
#' #> 10         2        4163872 1993-03-29 1993-03-29  3.80e7 proce… Plain … <NA>
#' #> # … with 203 more rows, and abbreviated variable names ¹observation_concept_id,
#' #> #   ²type_concept_id, ³observation_concept_name, ⁴type_concept_name
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmFlatten <- function(cdm,
                       domain = c("condition", "drug", "procedure"),
                       includeConceptName = TRUE) {


  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(domain, min.len = 1)
  checkmate::assertSubset(domain, choices =  c("condition",
                                               "drug",
                                               "procedure",
                                               "measurement",
                                               "visit",
                                               "death",
                                               "observation"))

  checkmate::assertLogical(includeConceptName, len = 1)

  queryList <- list()

  if ("condition" %in% domain) {
    assert_tables(cdm, "condition_occurrence")
    queryList[["condition"]] <- cdm$condition_occurrence %>%
      dplyr::select(
        person_id,
        observation_concept_id = .data$condition_concept_id,
        start_date = .data$condition_start_date,
        end_date = .data$condition_end_date,
        type_concept_id = .data$condition_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "condition")
  }

  if ("drug" %in% domain) {
    assert_tables(cdm, "drug_exposure")
    queryList[["drug"]] <- cdm$drug_exposure %>%
      dplyr::select(
        person_id,
        observation_concept_id = drug_concept_id,
        start_date = drug_exposure_start_date,
        end_date = drug_exposure_end_date,
        type_concept_id = drug_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "drug")
  }

  if ("procedure" %in% domain) {
    assert_tables(cdm, "procedure_occurrence")
    queryList[["procedure"]] <- cdm$procedure_occurrence %>%
      dplyr::select(
        person_id,
        observation_concept_id = procedure_concept_id,
        start_date = procedure_date,
        end_date = procedure_date,
        type_concept_id = procedure_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "procedure")
  }

  if ("measurement" %in% domain) {
    assert_tables(cdm, "measurement")
    queryList[["measurement"]] <- cdm$measurement %>%
      dplyr::select(
        person_id,
        observation_concept_id = measurement_concept_id,
        start_date = measurement_date,
        end_date = measurement_date,
        type_concept_id = measurement_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr:: mutate(domain = "measurement")
  }

  if ("visit" %in% domain) {
    assert_tables(cdm, "visit")
    queryList[["visit"]] <- cdm$visit_occurrence %>%
      dplyr::select(
        person_id,
        observation_concept_id = visit_concept_id,
        start_date = visit_start_date,
        end_date = visit_end_date,
        type_concept_id = visit_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "visit")
  }

  if ("death" %in% domain) {
    assert_tables(cdm, "death")
    queryList[["death"]] <- cdm$death %>%
      dplyr::select(
        person_id,
        observation_concept_id = cause_concept_id,
        start_date = death_date,
        end_date = death_date,
        type_concept_id = death_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "death")
  }

  if ("observation" %in% domain) {
    assert_tables(cdm, "observation")
    queryList[["death"]] <- cdm$observation %>%
      dplyr::select(
        person_id,
        observation_concept_id =  observation_concept_id,
        start_date =  observation_date,
        end_date = observation_date,
        type_concept_id = observation_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "observation")
  }

  if (includeConceptName) {
    assert_tables(cdm, "concept")
    out <- queryList %>%
      purrr::reduce(dplyr::union) %>%
      dplyr::left_join(dplyr::select(cdm$concept,
                       observation_concept_id = .data$concept_id,
                       observation_concept_name = .data$concept_name),
                by = "observation_concept_id") %>%
      dplyr::left_join(dplyr::select(cdm$concept,
                       type_concept_id = .data$concept_id,
                       type_concept_name = .data$concept_name),
                by = "type_concept_id") %>%
      dplyr::ungroup() %>%
      dplyr::distinct()

  } else {
    out <- purrr::reduce(queryList, dplyr::union) %>%
      dplyr::ungroup() %>%
      dplyr::distinct()
  }

  # collect?
  return(out)
}


