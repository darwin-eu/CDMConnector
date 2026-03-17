# Copyright 2026 DARWIN EU(R)
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

# Internal compatibility helpers for legacy cohort era operations.

cohort_collapse <- function(x) {
  cohortCollapse(x)
}

cohort_pad_end <- function(x, days = NULL, from = "end") {
  cols <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)
  checkmate::assert_integerish(days, len = 1, null.ok = TRUE)
  checkmate::assert_choice(from, choices = c("start", "end"))

  if (is.null(days)) {
    return(x)
  }

  if (from == "start" && days < 0) {
    rlang::abort("cohort_end_date cannot be before cohort_start_date!")
  }

  date_col <- paste0("cohort_", from, "_date")

  x %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      cohort_end_date = !!CDMConnector::dateadd(
        date = date_col,
        number = days,
        interval = "day"
      )
    ) %>%
    cohortCollapse() %>%
    dplyr::filter(.data$cohort_start_date <= .data$cohort_end_date)
}

cohortErafy <- function(x, gap) {
  checkmate::assert_class(x, "tbl")
  checkmate::assert_integerish(gap, len = 1)
  cols <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  checkmate::assert_subset(colnames(x), cols)

  x %>%
    cohort_pad_end(gap, from = "end") %>%
    cohortCollapse() %>%
    cohort_pad_end(-gap, from = "end")
}

cohort_erafy <- function(x, gap) {
  cohortErafy(x = x, gap = gap)
}
