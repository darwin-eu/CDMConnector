#' @importFrom omopgenerics cohortCount
#' @export
omopgenerics::cohortCount

#' @importFrom omopgenerics settings
#' @export
omopgenerics::settings

#' @importFrom omopgenerics attrition
#' @export
omopgenerics::attrition

#' @importFrom omopgenerics newCohortTable
#' @export
omopgenerics::newCohortTable

#' @importFrom omopgenerics insertTable
#' @export
omopgenerics::insertTable

#' @importFrom omopgenerics dropTable
#' @export
omopgenerics::dropTable

#' @importFrom omopgenerics readSourceTable
#' @export
omopgenerics::readSourceTable

#' @importFrom omopgenerics dropSourceTable
#' @export
omopgenerics::dropSourceTable

#' @importFrom omopgenerics listSourceTables
#' @export
omopgenerics::listSourceTables

#' @importFrom omopgenerics bind
#' @export
omopgenerics::bind

#' @importFrom omopgenerics cdmVersion
#' @export
omopgenerics::cdmVersion

#' @importFrom omopgenerics cdmFromTables
#' @export
omopgenerics::cdmFromTables

#' @importFrom omopgenerics cohortCodelist
#' @export
omopgenerics::cohortCodelist

#' @importFrom omopgenerics cdmName
#' @export
omopgenerics::cdmName

#' @importFrom omopgenerics recordCohortAttrition
#' @export
omopgenerics::recordCohortAttrition

#' @importFrom omopgenerics uniqueTableName
#' @export
omopgenerics::uniqueTableName

#' Create a cdm object from local tables
#'
#' `r lifecycle::badge("deprecated")`
#'
#' @param tables List of tables to be part of the cdm object.
#' @param cdm_name Name of the cdm object.
#' @param cohort_tables List of tables that contains cohort, cohort_set and
#' cohort_attrition can be provided as attributes.
#' @param cdm_version Version of the cdm_reference
#'
#' @return A `cdm_reference` object.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#'
#' person <- dplyr::tibble(
#'   person_id = 1, gender_concept_id = 0, year_of_birth = 1990,
#'   race_concept_id = 0, ethnicity_concept_id = 0
#' )
#' observation_period <- dplyr::tibble(
#'   observation_period_id = 1, person_id = 1,
#'   observation_period_start_date = as.Date("2000-01-01"),
#'   observation_period_end_date = as.Date("2025-12-31"),
#'   period_type_concept_id = 0
#' )
#' cdm <- cdmFromTables(
#'   tables = list("person" = person, "observation_period" = observation_period),
#'   cdmName = "test"
#' )
#'}
cdm_from_tables <- function(tables,
                            cdm_name,
                            cohort_tables = list(),
                            cdm_version = NULL) {
  lifecycle::deprecate_soft("1.7.0", "cdm_from_tables()", "cdmFromTables()")

  omopgenerics::cdmFromTables(
    tables = tables,
    cdmName = cdm_name,
    cohortTables = cohort_tables,
    cdmVersion = cdm_version
  )
}
