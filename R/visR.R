
#' Create an attrition diagram from a generated cohort set
#'
#' @param x A GeneratedCohortSet object
#' @param ... Not used
#'
#' `r lifecycle::badge("experimental")`
#'
#' @return No return value. This function will create one attrition plot for each generated cohort.
#' @export
#' @importFrom visR visr
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con, "main", "main")
#' cohort_set <- read_cohort_set(system.file("cohorts2", package = "CDMConnector"))
#' cdm <- generate_cohort_set(cdm, cohort_set, name = "cohort", overwrite = T)
#'
#' cohort_attrition(cdm$cohort) %>%
#'   dplyr::filter(cohort_definition_id == 3) %>%
#'   visR::visr()
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
#'
visr.omop_attrition <- function(x, ...) {
  if (!rlang::is_installed("visR")) cli::cli_abort("Please install the visR package.")

  ids <- unique(x$cohort_definition_id)
  if (length(ids) > 1) {
    cli::cli_abort("Cannot create attrition diagram because your cohort set has more than one cohort ID ({paste(ids, collapse = ', ')}). \nFirst make sure there is only one cohort_definition_id in the cohort set.")
  }
  checkmate::assertIntegerish(ids, len = 1, lower = 1, any.missing = FALSE)

  x <- x %>%
    dplyr::select(Criteria = .data$reason, `Remaining N` = .data$number_subjects)

  NextMethod(x)
}
