# Copyright 2025 DARWIN EU®
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

#' Run benchmark of tasks using CDMConnector
#'
#' @param cdm A CDM reference object
#'
#' @return a tibble with time taken for different analyses
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
#' benchmarkCDMConnector(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
benchmarkCDMConnector <- function(cdm) {
  checkmate::assertClass(cdm, "cdm_reference")

  # will add timings to list
  timings <- list()

  # first set of queries are just with the vocabulary tables
  # these should be similar between databases

  task <- "distinct count of concept relationship table"
  cli::cli_inform("Getting {task}")
  startTime <- Sys.time()
  cdm[["concept_relationship"]] |>
    dplyr::distinct() |>
    dplyr::tally() |>
    dplyr::pull("n")
  endTime <- Sys.time()
  timings[[task]] <- dplyr::tibble(
    task = .env$task,
    time_taken_secs = as.numeric(difftime(endTime, startTime, units = "secs"))
  )

  task <- "count of different relationship IDs in concept relationship table"
  cli::cli_inform("Getting {task}")
  startTime <- Sys.time()
  cdm[["concept_relationship"]] |>
    dplyr::group_by(.data$relationship_id) |>
    dplyr::tally() |>
    dplyr::collect()
  endTime <- Sys.time()
  timings[[task]] <- dplyr::tibble(
    task = .env$task,
    time_taken_secs = as.numeric(difftime(endTime, startTime, units = "secs"))
  )

  task <- "join of concept and concept class computed to a temp table"
  cli::cli_inform("Getting {task}")
  startTime <- Sys.time()
  cdm[["concept"]] |>
    dplyr::left_join(cdm[["concept_class"]],
      by = c("concept_id" = "concept_class_concept_id"),
      suffix = c("_x", "_y")
    ) |>
    dplyr::compute()
  endTime <- Sys.time()
  timings[[task]] <- dplyr::tibble(
    task = .env$task,
    time_taken_secs = as.numeric(difftime(endTime, startTime, units = "secs"))
  )

  task <- "concept table collected into memory"
  cli::cli_inform("Getting {task}")
  startTime <- Sys.time()
  cdm[["concept"]] |>
    dplyr::collect()
  endTime <- Sys.time()
  timings[[task]] <- dplyr::tibble(
    task = .env$task,
    time_taken_secs = as.numeric(difftime(endTime, startTime, units = "secs"))
  )

  # second set of queries are with clinical tables
  # these will differ substantially by database

  task <- "join of person and observation period collected into memory"
  cli::cli_inform("Getting {task}")
  startTime <- Sys.time()
  cdm[["person"]] |>
    dplyr::inner_join(cdm[["observation_period"]],
      by = "person_id"
    ) |>
    dplyr::collect()
  endTime <- Sys.time()
  timings[[task]] <- dplyr::tibble(
    task = .env$task,
    time_taken_secs = as.numeric(difftime(endTime, startTime, units = "secs"))
  )

  task <- "summary of observation period start and end dates by gender concept id"
  cli::cli_inform("Getting {task}")
  startTime <- Sys.time()
  cdm[["person"]] |>
    dplyr::inner_join(cdm[["observation_period"]],
      by = "person_id"
    ) |>
    dplyr::group_by(.data$gender_concept_id) |>
    dplyr::summarise(
      max = max(.data$observation_period_end_date, na.rm = TRUE),
      min = min(.data$observation_period_start_date, na.rm = TRUE)
    ) %>%
    dplyr::collect()
  endTime <- Sys.time()
  timings[[task]] <- dplyr::tibble(
    task = .env$task,
    time_taken_secs = as.numeric(difftime(endTime, startTime, units = "secs"))
  )

  # combine results
  timings <- dplyr::bind_rows(timings) %>%
    dplyr::mutate(time_taken_mins = round(.data$time_taken_secs / 60, 2)) %>%
    dplyr::mutate(dbms = attr(attr(cdm, "cdm_source"), "source_type")) %>%
    dplyr::mutate(person_n = cdm$person %>%
      dplyr::count() %>%
      dplyr::pull())


  return(timings)
}
