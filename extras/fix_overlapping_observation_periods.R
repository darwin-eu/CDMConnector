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

#' Fix overlapping observation periods in a CDM
#'
#' Reads the observation_period table from a cdm reference, finds rows that
#' overlap within the same person_id, collapses them into one row per person
#' using min(observation_period_start_date) and max(observation_period_end_date),
#' caps dates to Sys.Date(), and overwrites the table with the fixed data.
#'
#' Usage:
#'   library(CDMConnector)
#'   cdm <- cdmFromCon(DBI::dbConnect(duckdb::duckdb(), "path/to/cdm.duckdb"), ...)
#'   source("extras/fix_overlapping_observation_periods.R")
#'   fix_overlapping_observation_periods(cdm)
#'
#' @param cdm A cdm reference (e.g. from cdmFromCon or cdmFromCohortSet).
#' @return The cdm (invisibly). The observation_period table is modified in place.
#' @noRd
fix_overlapping_observation_periods <- function(cdm) {
  if (!requireNamespace("CDMConnector", quietly = TRUE)) {
    stop("Package CDMConnector is required.")
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package dplyr is required for collect().")
  }
  if (!"observation_period" %in% names(cdm)) {
    stop("cdm does not contain observation_period.")
  }

  con <- CDMConnector::cdmCon(cdm)
  schema <- attr(cdm, "cdm_schema")
  if (is.null(schema)) {
    schema <- CDMConnector::cdmWriteSchema(cdm)
  }
  dbms <- CDMConnector::dbms(con)

  # Read current observation_period
  op <- dplyr::collect(cdm$observation_period)
  if (nrow(op) == 0) {
    message("observation_period is empty; nothing to fix.")
    return(invisible(cdm))
  }

  # Collapse overlapping periods by person_id: one row per person with min(start), max(end)
  today <- Sys.Date()
  op$observation_period_start_date <- pmin(as.Date(op$observation_period_start_date), today)
  op$observation_period_end_date <- pmin(as.Date(op$observation_period_end_date), today)

  agg_min <- stats::aggregate(observation_period_start_date ~ person_id, data = op, FUN = min)
  agg_max <- stats::aggregate(observation_period_end_date ~ person_id, data = op, FUN = max)
  op_collapsed <- merge(agg_min, agg_max, by = "person_id", all = TRUE)
  op_collapsed$observation_period_id <- seq_len(nrow(op_collapsed))
  op_collapsed$period_type_concept_id <- 0L

  # Preserve other columns if present (e.g. period_type_concept_id from first row per person)
  want <- c("observation_period_id", "person_id", "observation_period_start_date",
            "observation_period_end_date", "period_type_concept_id")
  op_collapsed <- op_collapsed[, want[want %in% names(op_collapsed)], drop = FALSE]
  if (!"period_type_concept_id" %in% names(op_collapsed)) {
    op_collapsed$period_type_concept_id <- 0L
  }

  # Overwrite table: delete all rows, then insert fixed rows
  tbl_name <- CDMConnector::inSchema(schema, "observation_period", dbms = dbms)
  schema_name <- if (is.character(schema) && length(schema) == 1) schema else if ("schema" %in% names(schema)) schema[["schema"]] else schema[[1]]
  qualified_sql <- paste0(DBI::dbQuoteIdentifier(con, schema_name), ".", DBI::dbQuoteIdentifier(con, "observation_period"))
  DBI::dbExecute(con, paste0("DELETE FROM ", qualified_sql))

  DBI::dbWriteTable(con, name = tbl_name, value = op_collapsed, append = TRUE)

  n_before <- nrow(op)
  n_after <- nrow(op_collapsed)
  message("observation_period: ", n_before, " row(s) -> ", n_after, " row(s) (overlapping periods collapsed by person_id).")
  invisible(cdm)
}

# Example (run when sourced with a cdm in scope, or pass cdm as argument):
# fix_overlapping_observation_periods(cdm)
