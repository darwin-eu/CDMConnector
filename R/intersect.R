
union_cohorts <- function(x, cohort_definition_id = 1) {
  x %>%
    dplyr::select(subject_id, event_date = cohort_start_date) %>%
    dplyr::group_by(subject_id) %>%
    dplyr::mutate(event_type = -1L, start_ordinal = row_number(event_date)) %>%
    dplyr::union_all(dplyr::transmute(x, subject_id, event_date = cohort_end_date, event_type = 1L, start_ordinal = NULL)) %>%
    dbplyr::window_order(event_date, event_type) %>%
    dplyr::mutate(start_ordinal = cummax(start_ordinal), overall_ordinal = row_number()) %>%
    dplyr::filter((2 * start_ordinal) == overall_ordinal) %>%
    dplyr::distinct(subject_id, end_date = event_date) %>%
    dplyr::inner_join(x, by = "subject_id") %>%
    dplyr::filter(end_date >= cohort_start_date) %>%
    dplyr::group_by(subject_id, cohort_start_date) %>%
    dplyr::summarise(cohort_end_date = min(end_date, na.rm = TRUE), .groups = "drop") %>%
    dplyr::group_by(subject_id, cohort_end_date) %>%
    dplyr::summarise(cohort_start_date = min(cohort_start_date, na.rm = TRUE), .groups = "drop") %>%
    dplyr::transmute(cohort_definition_id = local(cohort_definition_id), subject_id, cohort_start_date, cohort_end_date)
}

unionCohorts <- union_cohorts



intersect_cohorts <- function(x, cohort_definition_id = 1) {

  nms <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")

  checkmate::assert_subset(nms, names(x))

  # get the total number of cohorts we are intersecting together
  n_cohorts_to_intersect <- x %>%
    dplyr::distinct(.data$cohort_definition_id) %>%
    dplyr::tally() %>%
    dplyr::pull("n")

  checkmate::checkIntegerish(n_cohorts_to_intersect, len = 1)

  # create every possible interval
  candidate_intervals <- x %>%
    dplyr::select(subject_id, cohort_date = cohort_start_date) %>%
    dplyr::union_all(dplyr::select(x, subject_id, cohort_date = cohort_end_date)) %>%
    dplyr::group_by(.data$subject_id) %>%
    dplyr::mutate(cohort_date_seq = dplyr::row_number(cohort_date)) %>%
    dplyr::mutate(candidate_start_date = cohort_date,
                  candidate_end_date = dplyr::lead(cohort_date, order_by = c("cohort_date", "cohort_date_seq")))

  # get intervals that are contained within all of the cohorts
  x %>%
    dplyr::inner_join(candidate_intervals, by = "subject_id") %>%
    dplyr::filter(.data$candidate_start_date >= .data$cohort_start_date,
                  .data$candidate_end_date <= .data$cohort_end_date) %>%
    dplyr::distinct(cohort_definition_id,
                    subject_id,
                    candidate_start_date,
                    candidate_end_date) %>%
    dplyr::group_by(subject_id,
                    candidate_start_date,
                    candidate_end_date) %>%
    dplyr::summarise(n_cohorts_interval_is_inside = n(), .groups = "drop") %>%
    # only keep intervals that are inside all cohorts we want to intersect
    dplyr::filter(n_cohorts_interval_is_inside == local(n_cohorts_to_intersect)) %>%
    dplyr::mutate(cohort_definition_id = local(cohort_definition_id)) %>%
    dplyr::select(cohort_definition_id,
                  subject_id,
                  cohort_start_date = candidate_start_date,
                  cohort_end_date = candidate_end_date) %>%
    union_cohorts(cohort_definition_id = cohort_definition_id)
}

intersectCohorts <- intersect_cohorts


