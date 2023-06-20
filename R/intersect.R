
#' Union all cohorts in a single cohort table
#'
#' @param x A tbl reference to a cohort table
#' @param cohort_definition_id A number to use for the new cohort_definition_id
#'
#' `r lifecycle::badge("experimental")`
#'
#' @return A lazy query that when executed will resolve to a new cohort table with
#' one cohort_definition_id resulting from the union of all cohorts in the original
#' cohort table
#' @export
union_cohorts <- function(x, cohort_definition_id = 1L) {
  checkmate::assert_class(x, "tbl")
  checkmate::assert_integerish(cohort_definition_id, len = 1, lower = 0)
  checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), names(x))
  cohort_definition_id <- as.integer(cohort_definition_id)

  event_date <- event_type <- NA # to remove r check error initialize these variables to NA

  x %>%
    dplyr::select("subject_id", event_date = "cohort_start_date") %>%
    dplyr::group_by(.data$subject_id) %>%
    dplyr::mutate(event_type = -1L, start_ordinal = dplyr::row_number(.data$event_date)) %>%
    dplyr::union_all(dplyr::transmute(x, .data$subject_id, event_date = .data$cohort_end_date, event_type = 1L, start_ordinal = NULL)) %>%
    {if ("tbl_sql" %in% class(.)) dbplyr::window_order(., event_date, event_type) else dplyr::arrange(., .data$event_date, .data$event_type)} %>%
    dplyr::mutate(start_ordinal = cummax(.data$start_ordinal), overall_ordinal = dplyr::row_number()) %>%
    dplyr::filter((2 * .data$start_ordinal) == .data$overall_ordinal) %>%
    dplyr::transmute(.data$subject_id, end_date = .data$event_date) %>%
    dplyr::distinct() %>%
    dplyr::inner_join(x, by = "subject_id") %>%
    dplyr::filter(.data$end_date >= .data$cohort_start_date) %>%
    dplyr::group_by(.data$subject_id, .data$cohort_start_date) %>%
    dplyr::summarise(cohort_end_date = min(.data$end_date, na.rm = TRUE), .groups = "drop") %>%
    dplyr::group_by(.data$subject_id, .data$cohort_end_date) %>%
    dplyr::summarise(cohort_start_date = min(.data$cohort_start_date, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(cohort_definition_id = .env$cohort_definition_id) %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
}

#' Intersect all cohorts in a single cohort table
#'
#' @param x A tbl reference to a cohort table
#' @param cohort_definition_id A number to use for the new cohort_definition_id
#'
#' `r lifecycle::badge("experimental")`
#'
#' @return A lazy query that when executed will resolve to a new cohort table with
#' one cohort_definition_id resulting from the intersection of all cohorts in the original
#' cohort table
#' @export
intersect_cohorts <- function(x, cohort_definition_id = 1L) {
  checkmate::assert_class(x, "tbl")
  checkmate::assert_integerish(cohort_definition_id, len = 1, lower = 0)
  checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), names(x))
  cohort_definition_id <- as.integer(cohort_definition_id)

  # get the total number of cohorts we are intersecting together
  n_cohorts_to_intersect <- x %>%
    dplyr::distinct(.data$cohort_definition_id) %>%
    dplyr::tally(name = "n") %>%
    dplyr::pull("n")

  checkmate::checkIntegerish(n_cohorts_to_intersect, len = 1)

  # create every possible interval
  candidate_intervals <- x %>%
    dplyr::select("subject_id", cohort_date = "cohort_start_date") %>%
    dplyr::union_all(dplyr::select(x, "subject_id", cohort_date = "cohort_end_date")) %>%
    dplyr::group_by(.data$subject_id) %>%
    dplyr::mutate(cohort_date_seq = dplyr::row_number(.data$cohort_date)) %>%
    dplyr::mutate(candidate_start_date = .data$cohort_date,
                  candidate_end_date = dplyr::lead(.data$cohort_date, order_by = c("cohort_date", "cohort_date_seq")))

  # get intervals that are contained within all of the cohorts
  x %>%
    dplyr::inner_join(candidate_intervals, by = "subject_id") %>%
    dplyr::filter(.data$candidate_start_date >= .data$cohort_start_date,
                  .data$candidate_end_date <= .data$cohort_end_date) %>%
    dplyr::distinct(.data$cohort_definition_id,
                    .data$subject_id,
                    .data$candidate_start_date,
                    .data$candidate_end_date) %>%
    dplyr::group_by(.data$subject_id,
                    .data$candidate_start_date,
                    .data$candidate_end_date) %>%
    dplyr::summarise(n_cohorts_interval_is_inside = dplyr::n(), .groups = "drop") %>%
    # only keep intervals that are inside all cohorts we want to intersect (i.e. all cohorts in the input cohort table)
    dplyr::filter(.data$n_cohorts_interval_is_inside == .env$n_cohorts_to_intersect) %>%
    dplyr::mutate(cohort_definition_id = .env$cohort_definition_id) %>%
    dplyr::select("cohort_definition_id",
                  "subject_id",
                  cohort_start_date = "candidate_start_date",
                  cohort_end_date = "candidate_end_date") %>%
    union_cohorts(cohort_definition_id = cohort_definition_id)
}

#' @rdname intersect_cohorts
#' @export
intersectCohorts <- intersect_cohorts

#' @rdname union_cohorts
#' @export
unionCohorts <- union_cohorts

# Internal function to remove overlapping periods in cohorts
# This is used as a helper inside other cohort manipulation functions
# @param x A cohort table (dataframe, tbl_dbi, arrow table...) that may have overlapping periods
# @return A dplyr query that collapses any overlapping periods. This is very similar to union.
collapse_cohorts <- function(x) {
  checkmate::assert_true(methods::is(x, "tbl") || methods::is(x, "data.frame"))
  checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), names(x))

  event_date <- event_type <- NA # to remove r check error initialize these variables to NA

  x %>%
    # Create "long" table with one row per date. event_type identifies start (-1) vs. end (1). start_ordinal numbers start dates.
    dplyr::ungroup() %>%
    dplyr::select("subject_id", "cohort_definition_id", event_date = "cohort_start_date") %>%
    dplyr::group_by(.data$subject_id, .data$cohort_definition_id) %>%
    dplyr::mutate(event_type = -1L, start_ordinal = dplyr::row_number(.data$event_date)) %>%
    dplyr::union_all(dplyr::transmute(x, .data$subject_id, .data$cohort_definition_id, event_date = .data$cohort_end_date, event_type = 1L, start_ordinal = NULL)) %>%
    # Identify "valid" end dates
    {if ("tbl_sql" %in% class(.)) dbplyr::window_order(., event_date, event_type) else dplyr::arrange(., .data$event_date, .data$event_type)} %>%
    dplyr::mutate(start_ordinal = cummax(.data$start_ordinal), overall_ordinal = dplyr::row_number()) %>%
    dplyr::filter((2*.data$start_ordinal) == .data$overall_ordinal) %>%
    dplyr::transmute(.data$subject_id, .data$cohort_definition_id, end_date = .data$event_date) %>%
    dplyr::distinct() %>%
    # Identify the earliest end date for each start date
    dplyr::inner_join(x, by = c("subject_id", "cohort_definition_id")) %>%
    dplyr::filter(.data$end_date >= .data$cohort_start_date) %>%
    dplyr::group_by(.data$subject_id, .data$cohort_definition_id, .data$cohort_start_date) %>%
    dplyr::summarise(cohort_end_date = min(.data$end_date, na.rm = TRUE), .groups = "drop") %>%
    # Identify the earliest start date for each end date
    dplyr::group_by(.data$subject_id, .data$cohort_definition_id, .data$cohort_end_date) %>%
    dplyr::summarise(cohort_start_date = min(.data$cohort_start_date, na.rm = TRUE), .groups = "drop") %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
}


timeline <- function(cdm, x = NULL) {
  if (is.null(x)) {
    assert_tables(cdm, "observation_period")

    cohort <- cdm$observation_period %>%
      dplyr::select(
        cohort_definition_id = 1L,
        subject_id = "person_id",
        cohort_start_date = "observation_period_start_date",
        cohort_end_date = "observation_period_end_date"
      )
    return(cohort)
  }

  # capr concept set

  if (is.list(x)) {
    checkmate::assert_named(x)
    nms <- names(x)
    # check names. # should these be cohort names or ids?



  }
}


# con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#
# cdm <- cdm_from_con(con, "main")
# concept_ids = c(35208414, 1118088)
# concept_ids = c(35208414)
#
# cohort_from_concept_list(cdm, concept_ids) {
#   checkmate::assert_class(cdm, "cdm_reference")
#   checkmate::assert_integerish(concept_ids)
#
#   assert_tables(cdm, "concept")
#   domains <- cdm$concept %>%
#     filter(concept_id %in% local(concept_ids)) %>%
#     select(concept_id, "domain_id")
#
# }



