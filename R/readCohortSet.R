
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

  if (!dir.exists(path)) {
    rlang::abort(glue::glue("The directory {path} does not exist!"))
  }

  if (file.exists(file.path(path, "CohortsToCreate.csv"))) {
    cohortsToCreate <- readr::read_csv(file.path(path, "CohortsToCreate.csv"), show_col_types = FALSE) %>%
      dplyr::mutate(cohort = purrr::map(.data$jsonPath, jsonlite::read_json)) %>%
      dplyr::mutate(json = purrr::map(.data$jsonPath, readr::read_file)) %>%
      dplyr::mutate(cohort_definition_id = cohortId, cohort_name = cohortName)
  } else {
    jsonFiles <- sort(list.files(path, pattern = "\\.json$", full.names = TRUE))
    cohortsToCreate <- dplyr::tibble(
      cohort_definition_id = seq_along(jsonFiles),
      cohort_name = tools::file_path_sans_ext(basename(jsonFiles)),
      json_path = jsonFiles) %>%
      dplyr::mutate(cohort = purrr::map(.data$json_path, jsonlite::read_json)) %>%
      dplyr::mutate(json = purrr::map(.data$json_path, readr::read_file))
  }

  cohortsToCreate <- dplyr::select(cohortsToCreate, "cohort_definition_id", "cohort_name", "cohort", "json")
  class(cohortsToCreate) <- c("CohortSet", class(cohortsToCreate))
  return(cohortsToCreate)
}

