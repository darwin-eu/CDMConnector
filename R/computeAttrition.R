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


#' Compute the attrition for a set of cohorts
#'
#' @description This function computes the attrition for a set of cohorts. It
#' uses the inclusion_result table so the cohort should be previously generated
#' using stats = TRUE.
#'
#' @param cdm A cdm reference created by CDMConnector.
#' @param cohortStem Stem for the cohort tables.
#' @param cohortSet Cohort set of the generated tables.
#' @param cohortId Cohort definition id of the cohorts that we want to generate
#' the attrition. If NULL all cohorts from cohort set will be used.
computeAttrition <- function(cdm,
                             cohortStem,
                             cohortSet,
                             cohortId = NULL) {
  # Do we have to add more checks? do we want to remove them? I assume that this
  # depends if we want to export the function or not
  checkmate::assertDataFrame(cohortSet, min.rows = 0, col.names = "named")
  checkmate::assertNames(
    colnames(cohortSet),
    must.include = c("cohortId", "cohortName", "cohort")
  )
  if (is.null(cohortId)) {
    cohortId <- cohortSet$cohortId
  }
  checkmate::assertNumeric(
    cohortId, null.ok = TRUE, any.missing = FALSE, min.len = 1
  )
  checkmate::assertTRUE(all(cohortId %in% cohortSet$cohortId))
  checkmate::assertTRUE(length(cohortId) > 0)

  con <- attr(cdm, "dbcon")
  inclusionResultTableName <- paste0(cohortStem, "_inclusion_result")
  schema <- attr(cdm, "write_schema")

  # Bring the inclusion result table to R memory
  inclusionResult <- readTable(con, schema, inclusionResultTableName) %>%
    dplyr::collect() %>%
    dplyr::mutate(inclusion_rule_mask = as.numeric(inclusion_rule_mask))

  attrition <- lapply(cohortId, function(id) {
    inclusionName <- getInclusionName(cohortSet, id)
    numberInclusion <- length(inclusionName)
    if (numberInclusion == 0) {
      #cohortTableName <- paste0(cohortStem, "_cohort")
      cohortTableName <- cohortStem
      attrition <- dplyr::tibble(
        cohort_definition_id = id,
        number_observations = readTable(con, schema, cohortTableName) %>%
          dplyr::filter(.data$cohort_definition_id == id) %>%
          dplyr::tally() %>%
          dplyr::pull("n") %>%
          as.numeric(),
        number_subjects = readTable(con, schema, cohortTableName) %>%
          dplyr::filter(.data$cohort_definition_id == id) %>%
          dplyr::select("subject_id") %>%
          dplyr::distinct() %>%
          dplyr::tally() %>%
          dplyr::pull("n") %>%
          as.numeric(),
        reason_id = 1,
        reason = "Qualifying initial events",
        excluded_observations = NA,
        excluded_subjects = NA
      )
    } else {
      inclusionMaskId <- getInclusionMaskId(numberInclusion)
      inclusionName <- c("Qualifying initial events", inclusionName)
      attrition <- list()
      for (k in 1:(numberInclusion + 1)) {
        attrition[[k]] <- dplyr::tibble(
          cohort_definition_id = id,
          number_observations = inclusionResult %>%
            dplyr::filter(.data$mode_id == 0) %>%
            dplyr::filter(.data$inclusion_rule_mask %in% inclusionMaskId[[k]]) %>%
            dplyr::pull("person_count") %>%
            base::sum() %>%
            as.numeric(),
          number_subjects = inclusionResult %>%
            dplyr::filter(.data$mode_id == 1) %>%
            dplyr::filter(.data$inclusion_rule_mask %in% inclusionMaskId[[k]]) %>%
            dplyr::pull("person_count") %>%
            base::sum() %>%
            as.numeric(),
          reason_id = k,
          reason = inclusionName[k]
        )
      }
      attrition <- attrition %>%
        dplyr::bind_rows() %>%
        dplyr::mutate(
          excluded_observations =
            dplyr::lag(.data$number_observations, 1, order_by = .data$reason_id) -
            .data$number_observations
        ) %>%
        dplyr::mutate(
          excluded_subjects =
            dplyr::lag(.data$number_subjects, 1, order_by = .data$reason_id) -
            .data$number_subjects
        )
    }
    return(attrition)
  }) %>%
    dplyr::bind_rows() %>%
    computePermanent(paste0(cohortStem, "_cohort_attrition"), schema, TRUE)

  return(attrition)
}

getInclusionName <- function(cohortSet, cohortId) {
  lapply(
    cohortSet$cohort[[cohortId]]$InclusionRules,
    function(x) {
      x$name
    }
  ) %>%
    unlist()
}
getInclusionMaskMatrix <- function(numberInclusion) {
  inclusionMaskMatrix <- dplyr::tibble(
    inclusion_rule_mask = 0:(2^numberInclusion - 1)
  )
  for (k in 0:(numberInclusion - 1)) {
    inclusionMaskMatrix <- inclusionMaskMatrix %>%
      dplyr::mutate(
        !!paste0("inclusion_", k) :=
          rep(c(rep(0, 2^k), rep(1, 2^k)), 2^(numberInclusion - k - 1))
      )
  }
  return(inclusionMaskMatrix)
}
getInclusionMaskId <- function(numberInclusion) {
  inclusionMaskMatrix <- getInclusionMaskMatrix(numberInclusion)
  inclusionId <- lapply(-1:(numberInclusion - 1), function(x) {
    if (x == -1) {
      return(inclusionMaskMatrix$inclusion_rule_mask)
    } else {
      inclusionMaskMatrix <- inclusionMaskMatrix
      for (k in 0:x) {
        inclusionMaskMatrix <- inclusionMaskMatrix %>%
          dplyr::filter(.data[[paste0("inclusion_", k)]] == 1)
      }
      return(inclusionMaskMatrix$inclusion_rule_mask)
    }
  })
}
readTable <- function(con, schema, tableName) {
  if (dbms(con) == "duckdb") {
    writeSchema <- glue::glue_sql_collapse(
      DBI::dbQuoteIdentifier(con, schema),
      sep = "."
    )
    return(dplyr::tbl(
      con,
      paste(c(writeSchema, tableName), collapse = ".")
    ))
  } else if (length(schema) == 2) {
    return(dplyr::tbl(
      con,
      dbplyr::in_catalog(schema[[1]], schema[[2]], tableName)
    ))
  } else if (length(schema) == 1) {
    return(dplyr::tbl(
      con,
      dbplyr::in_schema(schema, tableName)
    ))
  }
}
