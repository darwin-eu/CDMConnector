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

# Internal function to remove overlapping periods in cohorts
# This is used as a helper inside other cohort manipulation functions
# @param x A cohort table (dataframe, tbl_dbi, arrow table...) that may have overlapping periods
# @return A dplyr query that collapses any overlapping periods. This is very similar to union.
cohort_collapse <- function(x) {
  checkmate::assert_true(methods::is(x, "tbl_dbi"))
  checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), colnames(x))
  checkmate::assertTRUE(DBI::dbIsValid(x$src$con))


  # note this assumes all columns are fully populated and cohort_end_date >= cohort_start_date
  # TODO do we need to confirm this assumption?

  con <- x$src$con
  min_start_sql <- dbplyr::sql(glue::glue('min({DBI::dbQuoteIdentifier(con, "cohort_start_date")})'))
  max_start_sql <- dbplyr::sql(glue::glue('max({DBI::dbQuoteIdentifier(con, "cohort_start_date")})'))
  max_end_sql <- dbplyr::sql(glue::glue('max({DBI::dbQuoteIdentifier(con, "cohort_end_date")})'))

  x <- x %>%
    dplyr::distinct() %>%
    dplyr::mutate(dur = !!datediff("cohort_start_date",
                                   "cohort_end_date")) %>%
    dplyr::group_by(.data$cohort_definition_id, .data$subject_id, .add = FALSE) %>%
    dbplyr::window_order(.data$cohort_start_date, .data$cohort_end_date,
                         .data$dur) %>%
    dplyr::mutate(
      prev_start = dplyr::coalesce(
        !!dbplyr::win_over(
          max_start_sql,
          partition = c("cohort_definition_id", "subject_id"),
          frame = c(-Inf, -1),
          order = c("cohort_start_date", "dur"),
          con = con),
        as.Date(NA)),
      prev_end = dplyr::coalesce(
        !!dbplyr::win_over(
          max_end_sql,
          partition = c("cohort_definition_id", "subject_id"),
          frame = c(-Inf, -1),
          order = c("cohort_start_date", "dur"),
          con = con),
        as.Date(NA)),
     next_start = dplyr::coalesce(
          !!dbplyr::win_over(
            min_start_sql,
            partition = c("cohort_definition_id", "subject_id"),
            frame = c(1, Inf),
            order = c("cohort_start_date", "dur"),
            con = con),
          as.Date(NA))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::compute()

  x <- x %>%
    dplyr::group_by(.data$cohort_definition_id, .data$subject_id, .add = FALSE) %>%
    dbplyr::window_order(.data$cohort_definition_id, .data$subject_id,
                         .data$cohort_start_date, .data$dur) %>%
    dplyr::mutate(groups = cumsum(
      dplyr::case_when(is.na(.data$prev_start)  ~ NA,
        !is.na(.data$prev_start)  &&
        .data$prev_start <= .data$cohort_start_date &&
          .data$cohort_start_date <= .data$prev_end ~ 0L,
        TRUE ~ 1L)
      ))

  x <- x  %>%
    dplyr::mutate(groups = dplyr::if_else(
      is.na(.data$groups)  &&
        .data$cohort_end_date >= .data$next_start,
      0L, .data$groups
    ))

  x <- x %>%
    dplyr::group_by(.data$cohort_definition_id,
                    .data$subject_id, .data$groups, .add = FALSE) %>%
    dplyr::summarize(cohort_start_date = min(.data$cohort_start_date, na.rm = TRUE),
                     cohort_end_date = max(.data$cohort_end_date, na.rm = TRUE),
                     .groups = "drop") %>%
    dplyr::select("cohort_definition_id", "subject_id",
                  "cohort_start_date", "cohort_end_date")

  x
}

#' Union all cohorts in a cohort set with cohorts in a second cohort set
#'
#` r lifecycle::badge("deprecated")`
#'
#' @param x A tbl reference to a cohort table with one or more generated cohorts
#' @param y A tbl reference to a cohort table with one generated cohort
#'
#' @return A lazy query that when executed will resolve to a new cohort table with
#' one the same cohort_definitions_ids in x resulting from the union of all cohorts
#' in x with the single cohort in y cohort table
#' @export
cohortUnion <- function(x, y) {
  lifecycle::deprecate_soft("1.7.0", "CDMConnector::cohortUnion()")
  checkmate::assert_class(x, "tbl")
  checkmate::assert_class(y, "tbl")
  checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), colnames(x))
  checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), colnames(y))

  cohort_id <- y %>%
    dplyr::distinct(.data$cohort_definition_id) %>%
    dplyr::pull(1)

  if (length(cohort_id) != 1) {
    rlang::abort("cohort table y can only contain one cohort when performing an union!")
  }

  y %>%
    dplyr::distinct(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::cross_join(dplyr::distinct(x, .data$cohort_definition_id)) %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::union_all(x) %>%
    cohort_collapse()
}

#' `r lifecycle::badge("deprecated")`
#' @rdname cohortUnion
#' @export
cohort_union <-  function(x, y) {
  lifecycle::deprecate_soft("1.7.0", "CDMConnector::cohort_union()")
  cohortUnion(x = x, y = y)
}

# Intersect all cohorts in a single cohort table
#
# @param x A tbl reference to a cohort table with one or more cohorts
# @param y A tbl reference to a cohort table with one cohort
#
# @return A lazy query that when executed will resolve to a new cohort table with
# one cohort_definition_id resulting from the intersection of all cohorts x with the cohort in y
# @export
#
#

# TODO rewrite cohort_intersect and add tests

# cohort_intersect <- function(x, y) {
#   checkmate::assert_class(x, "tbl")
#   checkmate::assert_class(y, "tbl")
#   checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), colnames(x))
#   checkmate::assert_subset(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"), colnames(y))
#
#   # collapse cohorts just
#   x <- cohort_collapse(x) %>%
#     compute(temporary = TRUE)
#
#   y <- y %>%
#     dplyr::mutate(cohort_definition_id = -1) %>%
#     cohort_collapse() %>%
#     dplyr::select(-"cohort_definition_id") %>%
#     computeQuery(temporary = TRUE)
#
#
#   # collapse cohort table y into a single cohort
#   # for each interval in y, create a record for each cohort id x
#   x <- y %>%
#     dplyr::cross_join(dplyr::distinct(x, .data$cohort_definition_id)) %>%
#     dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
#     dplyr::union_all(x)
#
#   # create every possible interval
#   candidate_intervals <- x %>%
#     dplyr::select("cohort_definition_id", "subject_id", cohort_date = "cohort_start_date") %>%
#     dplyr::union_all(dplyr::select(x, "cohort_definition_id", "subject_id", cohort_date = "cohort_end_date")) %>%
#     dplyr::group_by(.data$cohort_definition_id, .data$subject_id) %>%
#     dplyr::mutate(cohort_date_seq = dplyr::row_number(.data$cohort_date)) %>%
#     dplyr::mutate(candidate_start_date = .data$cohort_date,
#                   candidate_end_date = dplyr::lead(.data$cohort_date, order_by = c("cohort_date", "cohort_date_seq")))
#
#   # get intervals that are contained within all of the cohorts
#   x %>%
#     dplyr::inner_join(candidate_intervals, by = "subject_id") %>%
#     dplyr::filter(.data$candidate_start_date >= .data$cohort_start_date,
#                   .data$candidate_end_date <= .data$cohort_end_date) %>%
#     dplyr::distinct(.data$cohort_definition_id,
#                     .data$subject_id,
#                     .data$candidate_start_date,
#                     .data$candidate_end_date) %>%
#     dplyr::group_by(.data$subject_id,
#                     .data$candidate_start_date,
#                     .data$candidate_end_date) %>%
#     dplyr::summarise(n_cohorts_interval_is_inside = dplyr::n(), .groups = "drop") %>%
#     # only keep intervals that are inside all cohorts we want to intersect (i.e. all cohorts in the input cohort table)
#     dplyr::filter(.data$n_cohorts_interval_is_inside == 2) %>%
#     dplyr::mutate(cohort_definition_id = .env$id) %>%
#     dplyr::select("cohort_definition_id",
#                   "subject_id",
#                   cohort_start_date = "candidate_start_date",
#                   cohort_end_date = "candidate_end_date") %>%
#     cohort_collapse()
# }


# Keep only the earliest record for each person in a cohort
#
# @param x A generated cohort set
#
# @return A lazy query on a generated cohort set
cohort_first <- function(x) {
  cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)

  x %>%
    dplyr::group_by(.data$subject_id, .data$cohort_definition_id, .add = FALSE) %>%
    dplyr::slice_min(.data$cohort_start_date, order_by = "cohort_start_date", n = 1, with_ties = FALSE) %>%
    dplyr::ungroup()
}

# Keep only the latest record for each person in a cohort
#
# @param x A generated cohort set
#
# @return A lazy query on a generated cohort set
cohort_last <- function(x) {
  cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)

  x %>%
    dplyr::group_by(.data$subject_id, .data$cohort_definition_id, .add = FALSE) %>%
    dplyr::slice_max(.data$cohort_start_date, order_by = "cohort_start_date", n = 1, with_ties = FALSE) %>%
    dplyr::ungroup()
}

# Add or subtract days from the start or end of a cohort set
#
# @param x A generated cohort set table reference
# @param days The number of days to add. Can by any positive or negative integer
# @param from Reference date to add or subtract days to. "start" or "end" (default)
#
# @return A lazy tbl query on a the cohort table
cohort_pad_end <- function(x, days = NULL, from = "end") {
  cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)
  checkmate::check_integerish(days, len = 1, null.ok = TRUE)
  checkmate::check_choice(from, choices = c("start", "end"))

  if (is.null(days)) {
    return(x)
  }

  if (from == "start" && days < 0) {
    rlang::abort("cohort_end_date cannot be before cohort_start_date!")
  }

  date_col <- paste0("cohort_", from, "_date")

  x %>%
    dplyr::ungroup() %>%
    dplyr::mutate(cohort_end_date = !!CDMConnector::dateadd(date = date_col, number = days, interval = "day")) %>%
    cohort_collapse() %>% # TODO what if end < start, remove row?
    dplyr::filter(.data$cohort_start_date <= .data$cohort_end_date)
}

# Add or subtract days from the start or end of a cohort set
#
# @param x A generated cohort set table reference
# @param days The number of days to add. Can by any positive or negative integer
# @param from Reference date to add or subtract days to. "start" or "end" (default)
#
# @return A lazy tbl query on a the cohort table
cohort_pad_start <- function(x, days = NULL, from = "start") {
  cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)
  checkmate::check_integerish(days, len = 1, null.ok = TRUE)
  checkmate::check_choice(from, choices = c("start", "end"))

  if (is.null(days)) {
    return(x)
  }

  if (from == "end" && days > 0) {
    rlang::abort("cohort_start_date cannot be after cohort_end_date!")
  }

  date_col <- paste0("cohort_", from, "_date")

  x %>%
    dplyr::mutate(cohort_start_date = !!CDMConnector::dateadd(date = date_col, number = days, interval = "day")) %>%
    dplyr::ungroup() %>%
    cohort_collapse() %>%
    dplyr::filter(.data$cohort_start_date <= .data$cohort_end_date)
}

#' Collapse cohort records within a certain number of days
#'
#'
#' @param x A generated cohort set
#' @param gap When two cohort records are 'gap' days apart or less the periods will be
#' collapsed into a single record
#'
#' @return A lazy query on a generated cohort set
#' @export
cohortErafy <- function(x, gap) {
  lifecycle::deprecate_soft("1.7.0", "CDMConnector::cohortErafy()")
  checkmate::assert_class(x, "tbl")
  checkmate::assertIntegerish(gap, len = 1)
  cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)

  checkmate::assertIntegerish(gap, len = 1)
  x %>%
    cohort_pad_end(gap, from = "end") %>%
    cohort_collapse() %>%
    cohort_pad_end(-gap, from = "end")
}

#' `r lifecycle::badge("deprecated")`
#' @rdname cohortErafy
#' @export
cohort_erafy <- function(x, gap) {
  lifecycle::deprecate_soft("1.7.0", "CDMConnector::cohort_erafy()")
  cohortErafy(x = x, gap = gap)
}

# cohort_under_observation <- function(.data) {
#   checkmate::assert_class(.data, "tbl")
#   cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
#
#   cdm <- attr(tbl, "cdm_reference")
#   checkmate::assert_class(.data, "cdm_reference")
#   assertTables(cdm, "observation_period", empty.ok = FALSE)
#
#   .data %>%
#     dplyr::left_join(cdm$observation_period, by = c("cohort_id" = "person_id")) %>%
#     dplyr::filter((.data$observation_period_start_date <= .data$cohort_start_date && .data$cohort_start_date <= .data$observation_period_end_date) ||
#                   (.data$observation_period_start_date <= .data$cohort_end_date   && .data$cohort_end_date   <= .data$observation_period_end_date)) %>%
#     dplyr::mutate(cohort_start_date = ifelse(cohort_start_date < observation_period_start_date, observation_period_start_date, cohort_start_date),
#                   cohort_end_date = ifelse(observation_period_end_date < cohort_end_date, observation_period_end_date, cohort_end_date)) %>%
#     cohort_collapse()
# }

# #' @rdname cohort_under_observation
# #' @export
# cohortUnderObservation <- cohort_under_observation

# cohort_setdiff <- function(x, y) {
#   checkmate::assert_class(x, "tbl")
#   checkmate::assert_class(y, "tbl")
#   cols <-  c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
#   checkmate::assert_true(all(cols %in% colnames(x)))
#   checkmate::assert_true(all(cols %in% colnames(y)))
#
#   # remove days in the second cohort table from the first cohort table
#   x %>%
#     dplyr::left_join(dplyr::distinct(dplyr::select(y, "subject_id", remove_start = "cohort_start_date", remove_end = "cohort_end_date")), by = "subject_id") %>%
#     dplyr::mutate(
#       cohort_start_date = dplyr::case_when(
#         # cohort x is inside cohort y interval
#         remove_start <= cohort_start_date && cohort_start_date <= remove_end &&
#         remove_start <= cohort_end_date && cohort_end_date <= remove_end
#         ~ NULL,
#         # cohort x starts inside y and ends later than y
#         remove_start <= cohort_start_date && cohort_start_date <= remove_end &&
#         cohort_end_date > remove_end
#         ~ !!dateadd("remove_end", 1L),
#         # cohort x is entirely before cohort y
#         cohort_start_date < remove_start && cohort_start_date <= remove_end && # start is inside remove interval
#           remove_start <= cohort_end_date && cohort_end_date <= remove_end, # end is inside remove interval
#         cohort_start_date <= observation_period_start_date && observation_period_start_date <= cohort_end_date ~ !!dateadd("cohort_end_date", 1)
#       ),
#       cohort_end_date = dplyr::case_when(
#         observation_period_start_date < cohort_start_date || cohort_end_date < observation_period_start_date ~ observation_period_start_date,
#         cohort_start_date <= observation_period_start_date && observation_period_start_date <= cohort_end_date ~ !!dateadd("cohort_end_date", 1)
#       )
#     ) %>%
#     cohort_collapse()
# }






