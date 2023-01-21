
# Temporarily add this function to CDMConnector

#' Read a set of cohort definitions into R
#'
#' A "cohort set" is a collection of cohort definitions. In R this is stored in a dataframe with cohortId, cohortName, cohort, and sql columns.
#' On disk this is stored as a folder with a CohortsToCreate.csv file and one or more json files.
#' If the CohortsToCreate.csv file is missing then all of the json files in the folder will be used,
#' cohortIds will be automatically assigned in alphabetical order, and cohortNames will match the file names.
#'
#' @param path The path to a folder containing Circe cohort definition json files
#' and optionally a csv file named CohortsToCreate.csv with columns cohortId, cohortName, and jsonPath.
#' @importFrom jsonlite read_json
#' @importFrom dplyr tibble
#' @importFrom SqlRender render
#' @export
readCohortSet <- function(path) {
  if (!rlang::is_installed("CirceR")) {
    rlang::abort("CirceR is required to use `readCohortSet`\n
                 Install it with `devtools::install_github('OHDSI/CirceR')`")
  }

  if (file.exists(file.path(path, "CohortsToCreate.csv"))) {
    cohortsToCreate <- readr::read_csv(file.path(path, "CohortsToCreate.csv"), show_col_types = FALSE) %>%
      dplyr::mutate(sql = NA_character_, cohort = purrr::map(.data$jsonPath, jsonlite::read_json))
  } else {
    jsonFiles <- sort(list.files(path, pattern = "\\.json$", full.names = TRUE))
    cohortsToCreate <- dplyr::tibble(
      cohortId = seq_along(jsonFiles),
      cohortName = tools::file_path_sans_ext(basename(jsonFiles)),
      jsonPath = jsonFiles
    ) %>%
      dplyr::mutate(sql = NA_character_, cohort = purrr::map(.data$jsonPath, jsonlite::read_json))
  }

  if (nrow(cohortsToCreate) == 0) return(cohortsToCreate)

  for (i in 1:nrow(cohortsToCreate)) {
    cohortJson <- readr::read_file(cohortsToCreate$jsonPath[i])
    cohortDef <- jsonlite::read_json(cohortsToCreate$jsonPath[i]) # change to Capr::readCohort

    cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
    cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
    cohortSql <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE) # pre-render sql to remove extraneous code
    cohortsToCreate$sql[i] <- cohortSql
  }

  cohortsToCreate <- dplyr::select(cohortsToCreate, "cohortId", "cohortName", "cohort", "sql")
  class(cohortsToCreate) <- c("CohortSet", class(cohortsToCreate))
  return(cohortsToCreate)
}

