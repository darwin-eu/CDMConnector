#' Create or update the GeneratedCohortSet attributes to a cohort object
#'
#' @param cohort A generated cohort set
#' @param attritionReason The reason for attrition as a character string.
#' @param cohortSet tbl to update the cohort_set attribute.
#'
#' @return The cohort object with the attributes created or updated.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(dplyr)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con = con, cdm_schema = "main", write_schema = "main")
#' cdm <- generateConceptCohortSet(
#'   cdm = cdm, conceptSet = list(pharyngitis = 4112343), name = "new_cohort"
#' )
#'
#' cohortSet(cdm$new_cohort)
#' cohortCount(cdm$new_cohort)
#' cohortAttrition(cdm$new_cohort)
#'
#' cdm$new_cohort <- cdm$new_cohort %>%
#'   filter(cohort_start_date >= as.Date("2010-01-01"))
#'
#' cdm$new_cohort <- appendCohortAttributes(
#'   cohort = cdm$new_cohort, reason = "Only events after 2010"
#' )
#'
#' cohortSet(cdm$new_cohort)
#' cohortCount(cdm$new_cohort)
#' cohortAttrition(cdm$new_cohort)
#' }
#'
appendCohortAttributes <- function(cohort,
                                   attritionReason = "Qualifying initial records",
                                   cohortSet = cohortSet(cohort)) {

  checkmate::assertClass(cohort, "GeneratedCohortSet")
  name <- attr(cohort, "tbl_name")
  checkmate::assertCharacter(name, len = 1, min.chars = 1)

  checkmate::assertCharacter(
    attritionReason, min.chars = 1, len = 1, any.missing = FALSE
  )
  checkmate::assertClass(cohortSet, "tbl", null.ok = TRUE)
  cdm <- attr(cohort, "cdm_reference")
  checkmate::assertClass(cdm, "cdm_reference")
  if (!is.null(cohortSet)) {
    checkmate::assertTRUE(all(
      c("cohort_definition_id", "cohort_name") %in% colnames(cohortSet)
    ))

    if (!("tbl_sql" %in% class(cohortSet))) {
      ref <- inSchema(schema = attr(cdm, "write_schema"),
                      table = paste0(name, "_set"))

      DBI::dbWriteTable(
        conn = attr(cdm, "dbcon"),
        name = ref,
        value = cohortSet,
        overwrite = TRUE)

      cohortSet <- dplyr::tbl(attr(cdm, "dbcon"), ref)
    }
  } else {
    cohortSet <- cohort %>%
      dplyr::select("cohort_definition_id") %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        cohort_name = paste0("cohort_", .data$cohort_definition_id)
      ) %>%
      computeQuery(
        name = paste0(name, "_set"),
        temporary = getOption("cohort_as_temp", FALSE),
        schema = attr(cdm, "write_schema"), overwrite = TRUE
      )
  }

  # update cohort_set
  attr(cohort, "cohort_set") <- cohortSet

  # update cohort_count
  attr(cohort, "cohort_count") <- cohort %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(
      number_records = dplyr::n(),
      number_subjects = dplyr::n_distinct(.data$subject_id)
    ) %>%
    dplyr::right_join(
      attr(cohort, "cohort_set") %>% dplyr::select("cohort_definition_id"),
      by = "cohort_definition_id"
    ) %>%
    dplyr::mutate(
      number_records = dplyr::if_else(
        is.na(.data$number_records), 0, .data$number_records
      ), number_subjects = dplyr::if_else(
        is.na(.data$number_subjects), 0, .data$number_subjects
      )
    ) %>%
    computeQuery(
      name = paste0(name, "_count"),
      temporary = getOption("cohort_as_temp", FALSE),
      schema = attr(cdm, "write_schema"), overwrite = TRUE
    )

  # new line of attrition
  attrition <- attr(cohort, "cohort_count") %>%
    dplyr::mutate(
      reason = .env$attritionReason,
      number_records = dplyr::if_else(
        is.na(.data$number_records), 0, .data$number_records
      ),
      number_subjects = dplyr::if_else(
        is.na(.data$number_subjects), 0, .data$number_subjects
      )
    ) %>%
    computeQuery(
      temporary = getOption("intermediate_as_temp", TRUE),
      schema = attr(cdm, "write_schema"), overwrite = TRUE
    )

  # append line if already exists
  if (!is.null(attr(cohort, "cohort_attrition"))) {
    reasonId <- attr(cohort, "cohort_attrition") %>%
      dplyr::pull("reason_id") %>%
      max()
    attr(cohort, "cohort_attrition")  <- attr(cohort, "cohort_attrition") %>%
      dplyr::union_all(
        attrition %>%
          dplyr::mutate(reason_id = .env$reasonId) %>%
          dplyr::inner_join(
            attr(cohort, "cohort_attrition") %>%
              dplyr::select(
                "cohort_definition_id", "reason_id",
                "previous_records" = "number_records",
                "previous_subjects" = "number_subjects"
              ),
            by = c("cohort_definition_id", "reason_id")
          ) %>%
          dplyr::mutate(
            reason_id = .env$reasonId + 1, reason = .env$attritionReason,
            excluded_records = .data$previous_records - .data$number_records,
            excluded_subjects = .data$previous_subjects - .data$number_subjects
          ) %>%
          dplyr::select(
            "cohort_definition_id", "number_records", "number_subjects",
            "reason_id", "reason", "excluded_records", "excluded_subjects"
          )
      )
  } else {
    attr(cohort, "cohort_attrition") <- attrition %>%
      dplyr::mutate(
        reason_id = 1, excluded_records = 0, excluded_subjects = 0
      ) %>%
      dplyr::select(
        "cohort_definition_id", "number_records", "number_subjects",
        "reason_id", "reason", "excluded_records", "excluded_subjects"
      )
  }
  attr(cohort, "cohort_attrition") <- attr(cohort, "cohort_attrition") %>%
    computeQuery(
      name = paste0(name, "_attrition"),
      temporary = getOption("cohort_as_temp", FALSE),
      schema = attr(cdm, "write_schema"), overwrite = TRUE
    )

  # this function has to be updated with the behavior of the computes
  #  according new modifications and insert cohortSet if it is local

  return(cohort)
}
