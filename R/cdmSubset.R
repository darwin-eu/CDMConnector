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

# internal function to add a filter query to all tables in a cdm
# person_subset should be a tbl_sql reference to a
# table in the database with one column "person_id".
# There should be no duplicated rows in this table.
# The person_subset table should be a temporary table in the database.
# These requirements are not checked but assumed to be true.
cdmSamplePerson <- function(cdm, person_subset) {
  checkmate::assert_class(cdm, "cdm_reference")
  checkmate::assert_class(person_subset, "tbl_sql")

  for (nm in names(cdm)) {
    if ("person_id" %in% colnames(cdm[[nm]])) {
      cdm[[nm]] <- dplyr::inner_join(cdm[[nm]], person_subset, by = "person_id")
    } else if ("subject_id" %in% colnames(cdm[[nm]])) {
      cdm[[nm]] <- dplyr::inner_join(cdm[[nm]], person_subset, by = c("subject_id" = "person_id"))
    }
  }
  return(cdm)
}

#' Subset a cdm to the individuals in one or more cohorts
#'
#' `cdmSubset` will return a new cdm object that contains lazy queries pointing
#' to each of the cdm tables but subset to individuals in a generated cohort.
#' Since the cdm tables are lazy queries, the subset operation will only be
#' done when the tables are used. `computeQuery` can be used to run the SQL
#' used to subset a cdm table and store it as a new table in the database.
#'
#' @param cdm A cdm_reference object
#' @param cohortTable The name of a cohort table in the cdm reference
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
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
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
  checkmate::assertClass(cdm[[cohortTable]], "cohort_table")
  checkmate::assertIntegerish(cohortId, min.len = 1, null.ok = TRUE)
  checkmate::assertLogical(verbose, len = 1)

  cohort_colnames <- colnames(cdm[[cohortTable]])
  if (!("subject_id" %in% cohort_colnames)) {
    rlang::abort(glue::glue("subject_id column is not in cdm[['{cohortTable}']] table!"))
  }

  if (!("cohort_definition_id" %in% cohort_colnames)) {
    rlang::abort(glue::glue("cohort_definition_id column is not in cdm[['{cohortTable}']] table!"))
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
    dplyr::distinct(.data$subject_id) %>%
    dplyr::tally() %>%
    dplyr::pull("n")

  if (n_subjects != n_subjects_person_table) {
    rlang::warn(glue::glue(
      "Not all cohort subjects are present in person table.
       - N cohort subjects: {n_subjects}
       - N cohort subjects in cdm person table: {n_subjects_person_table}"))
  }


  prefix <- unique_prefix()

  person_subset <- subjects %>%
    dplyr::select(person_id = "subject_id") %>%
    dplyr::distinct() %>%
    dplyr::compute(name = glue::glue("person_sample{prefix}_"), temporary = FALSE)

  cdmSamplePerson(cdm, person_subset)
}

#' Subset a cdm object to a random sample of individuals
#'
#' `cdmSample` takes a cdm object and returns a new cdm that includes only a
#' random sample of persons in the cdm. Only `person_id`s in both the person
#' table and observation_period table will be considered.
#'
#' @param cdm A cdm_reference object.
#' @param n Number of persons to include in the cdm.
#' @param seed Seed for the random number generator.
#' @param name Name of the table that will contain the sample of persons.
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
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'
#' cdm <- cdmFromCon(con, cdmSchema = "main")
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
                      n,
                      seed = sample.int(1e6, 1),
                      name = "person_sample") {
  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertIntegerish(n, len = 1, lower = 1, upper = 1e9, null.ok = FALSE)
  checkmate::assertIntegerish(seed, len = 1, lower = 1, null.ok = FALSE)
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE)

  subset <- cdm[["person"]] |>
    dplyr::pull("person_id") |>
    unique() |>
    sort()

  if (length(subset) > n) {
    set.seed(seed)
    subset <- sample(x = subset, size = n, replace = FALSE)
  }

  subset <- dplyr::tibble("person_id" = subset)

  cdm <- omopgenerics::insertTable(cdm = cdm, name = name, table = subset)

  cdmSamplePerson(cdm, cdm[[name]])
}

#' Subset a cdm object to a set of persons
#'
#' `cdmSubset` takes a cdm object and a list of person IDs as input. It
#' returns a new cdm that includes data only for persons matching the provided
#' person IDs. Generated cohorts in the cdm will also be subset to
#' the IDs provided.
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
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'
#' cdm <- cdmFromCon(con, cdmSchema = "main")
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
cdmSubset <- function(cdm, personId) {

  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertIntegerish(personId,
                              min.len = 1,
                              max.len = 1e6,
                              null.ok = FALSE)

  writeSchema <- cdmWriteSchema(cdm)
  if (is.null(writeSchema)) rlang::abort("write_schema is required for subsetting a cdm!")
  con <- cdmCon(cdm)

  prefix <- unique_prefix()
  DBI::dbWriteTable(con,
                    name = .inSchema(writeSchema, glue::glue("temp{prefix}_"), dbms(con)),
                    value = data.frame(person_id = as.integer(personId)),
                    overwrite = TRUE)

  # Note temporary = TRUE in dbWriteTable does not work on all dbms but we want a temp table here.

  person_subset <- dplyr::tbl(con, .inSchema(writeSchema, glue::glue("temp{prefix}_"), dbms(con))) %>%
    dplyr::rename_all(tolower) %>% # just in case
    compute(name = glue::glue("person_subset_{prefix}"), temporary = TRUE)

  if (dbms(cdmCon(cdm)) != "spark") {
    # on spark the temp table seems to need this permanent table it was derived from to work.
    DBI::dbRemoveTable(con, .inSchema(writeSchema, glue::glue("temp{prefix}_"), dbms(con)))
  }

  cdmSamplePerson(cdm, person_subset)
}

#' Flatten a cdm into a single observation table
#'
#' This experimental function transforms the OMOP CDM into a single observation
#' table. This is only recommended for use with a filtered CDM or a cdm that is
#' small in size.
#'
#' @param cdm A cdm_reference object
#' @param domain Domains to include. Must be a subset of "condition_occurrence", "drug_exposure",
#' "procedure_occurrence", "measurement", "visit_occurrence", "death", "observation"
#' @param includeConceptName Should concept_name and type_concept_name be
#' include in the output table? TRUE (default) or FALSE
#'
#' @return A lazy query that when evaluated will result in a single table
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'
#' cdm <- cdmFromCon(con, cdmSchema = "main")
#'
#' all_observations <- cdmSubset(cdm, personId = c(2, 18, 42)) %>%
#'   cdmFlatten() %>%
#'   collect()
#'
#' all_observations
#' #> # A tibble: 213 × 8
#' #>    person_id observation_.  start_date end_date   type_.  domain obser.  type_.
#' #>        <dbl>          <dbl> <date>     <date>       <dbl> <chr>  <chr>   <chr>
#' #>  1         2       40213201 1986-09-09 1986-09-09  5.81e5 drug   pneumo  <NA>
#' #>  2        18        4116491 1997-11-09 1998-01-09  3.20e4 condi  Escher  <NA>
#' #>  3        18       40213227 2017-01-04 2017-01-04  5.81e5 drug   tetanu  <NA>
#' #>  4        42        4156265 1974-06-13 1974-06-27  3.20e4 condi  Facial  <NA>
#' #>  5        18       40213160 1966-02-23 1966-02-23  5.81e5 drug   poliov  <NA>
#' #>  6        42        4198190 1933-10-29 1933-10-29  3.80e7 proce  Append  <NA>
#' #>  7         2        4109685 1952-07-13 1952-07-27  3.20e4 condi  Lacera  <NA>
#' #>  8        18       40213260 2017-01-04 2017-01-04  5.81e5 drug   zoster  <NA>
#' #>  9        42        4151422 1985-02-03 1985-02-03  3.80e7 proce  Sputum  <NA>
#' #> 10         2        4163872 1993-03-29 1993-03-29  3.80e7 proce  Plain   <NA>
#' #> # ... with 203 more rows, and abbreviated variable names observation_concept_id,
#' #> #   type_concept_id, observation_concept_name, type_concept_name
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmFlatten <- function(cdm,
                       domain = c("condition_occurrence", "drug_exposure", "procedure_occurrence"),
                       includeConceptName = TRUE) {


  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertCharacter(domain, min.len = 1)
  checkmate::assertSubset(domain, choices =  c("condition_occurrence",
                                               "drug_exposure",
                                               "procedure_occurrence",
                                               "measurement",
                                               "visit_occurrence",
                                               "death",
                                               "observation"))

  checkmate::assertLogical(includeConceptName, len = 1)

  queryList <- list()

  if ("condition_occurrence" %in% domain) {
    checkmate::assertTRUE("condition_occurrence" %in% names(cdm))
    queryList[["condition_occurrence"]] <- cdm$condition_occurrence %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id = .data$condition_concept_id,
        start_date = .data$condition_start_date,
        end_date = .data$condition_end_date,
        type_concept_id = .data$condition_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "condition_occurrence")
  }

  if ("drug_exposure" %in% domain) {
    checkmate::assertTRUE("drug_exposure" %in% names(cdm))
    queryList[["drug_exposure"]] <- cdm$drug_exposure %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id = .data$drug_concept_id,
        start_date = .data$drug_exposure_start_date,
        end_date = .data$drug_exposure_end_date,
        type_concept_id = .data$drug_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "drug_exposure")
  }

  if ("procedure_occurrence" %in% domain) {
    checkmate::assertTRUE("procedure_occurrence" %in% names(cdm))
    queryList[["procedure_occurrence"]] <- cdm$procedure_occurrence %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id = .data$procedure_concept_id,
        start_date = .data$procedure_date,
        end_date = .data$procedure_date,
        type_concept_id = .data$procedure_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "procedure_occurrence")
  }

  if ("measurement" %in% domain) {
    checkmate::assertTRUE("measurement" %in% names(cdm))
    queryList[["measurement"]] <- cdm$measurement %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id = .data$measurement_concept_id,
        start_date = .data$measurement_date,
        end_date = .data$measurement_date,
        type_concept_id = .data$measurement_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr:: mutate(domain = "measurement")
  }

  if ("visit_occurrence" %in% domain) {
    checkmate::assertTRUE("visit_occurrence" %in% names(cdm))
    queryList[["visit_occurrence"]] <- cdm$visit_occurrence %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id = .data$visit_concept_id,
        start_date = .data$visit_start_date,
        end_date = .data$visit_end_date,
        type_concept_id = .data$visit_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "visit_occurrence")
  }

  if ("death" %in% domain) {
    checkmate::assertTRUE("death" %in% names(cdm))
    queryList[["death"]] <- cdm$death %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id = .data$cause_concept_id,
        start_date = .data$death_date,
        end_date = .data$death_date,
        type_concept_id = .data$death_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "death")
  }

  if ("observation" %in% domain) {
    checkmate::assertTRUE("observation" %in% names(cdm))
    queryList[["death"]] <- cdm$observation %>%
      dplyr::transmute(
        person_id = .data$person_id,
        observation_concept_id =  .data$observation_concept_id,
        start_date =  .data$observation_date,
        end_date = .data$observation_date,
        type_concept_id = .data$observation_type_concept_id) %>%
      dplyr::distinct() %>%
      dplyr::mutate(domain = "observation")
  }

  if (includeConceptName) {
    checkmate::assertTRUE("concept" %in% names(cdm))
    out <- queryList %>%
      purrr::reduce(dplyr::union) %>%
      dplyr::left_join(dplyr::transmute(cdm$concept,
                       observation_concept_id = .data$concept_id,
                       observation_concept_name = .data$concept_name),
                by = "observation_concept_id") %>%
      dplyr::left_join(dplyr::transmute(cdm$concept,
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

  # compute?
  return(out)
}

