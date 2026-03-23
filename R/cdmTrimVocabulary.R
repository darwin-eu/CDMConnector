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

#' Trim vocabulary tables to the minimum needed for the CDM
#'
#' Reduces vocabulary tables to only the concepts referenced in the CDM's
#' clinical data plus their ancestors in the concept_ancestor hierarchy. Tables
#' like concept_synonym and source_to_concept_map are truncated (emptied).
#' This makes the CDM much smaller and faster to upload to a remote test
#' database via \code{\link{copyCdmTo}}.
#'
#' The trimmed vocabulary is "graph-complete": every concept_id referenced in
#' concept_relationship and concept_ancestor also exists in concept. Most
#' relationships are trimmed out but the ancestor hierarchy is preserved for
#' all concepts in the CDM.
#'
#' Only works on local (DuckDB or data frame based) CDMs since it is intended
#' for test data preparation.
#'
#' @param cdm A cdm_reference object (DuckDB or local)
#' @return A cdm_reference with trimmed vocabulary tables
#' @export
cdmTrimVocabulary <- function(cdm) {

  checkmate::assertClass(cdm, "cdm_reference")

  # Only works on duckdb or local (data frame) CDMs

  backend <- dbms(cdm)
  if (!backend %in% c("duckdb", "local")) {
    cli::cli_abort(c(
      "cdmTrimVocabulary() only works on DuckDB or local CDMs.",
      i = "Current backend: {backend}",
      i = "This function is intended for test data preparation before uploading with copyCdmTo()."
    ))
  }

  is_db <- !is.null(attr(cdm, "dbcon"))

  # Step 1: Collect all concept_ids used anywhere in the CDM
  vocab_tables <- tblGroup("vocab")
  used_ids <- integer(0)

  for (nm in names(cdm)) {
    if (nm %in% vocab_tables) next
    tbl <- cdm[[nm]]
    concept_cols <- grep("concept_id", colnames(tbl), value = TRUE,
                         ignore.case = TRUE)
    for (col in concept_cols) {
      ids <- tbl |>
        dplyr::select(dplyr::all_of(col)) |>
        dplyr::distinct() |>
        dplyr::collect() |>
        dplyr::pull(1)
      used_ids <- c(used_ids, ids)
    }
  }

  used_ids <- unique(used_ids)
  used_ids <- used_ids[!is.na(used_ids) & used_ids != 0L]

  message(paste0("Found ", length(used_ids), " distinct concept_ids in clinical tables."))

  # Step 2: Expand with ancestors
  if ("concept_ancestor" %in% names(cdm)) {
    ancestor_ids <- cdm[["concept_ancestor"]] |>
      dplyr::filter(.data$descendant_concept_id %in% !!used_ids) |>
      dplyr::select("ancestor_concept_id") |>
      dplyr::distinct() |>
      dplyr::collect() |>
      dplyr::pull(1)

    used_ids <- unique(c(used_ids, ancestor_ids))
    message(paste0("After adding ancestors: ", length(used_ids), " concept_ids."))
  }

  # Helper to materialize a trimmed table back into the database
  .computeTrimmed <- function(tbl, name) {
    if (is_db) {
      tbl |> dplyr::compute(name = name, temporary = FALSE, overwrite = TRUE)
    } else {
      tbl |> dplyr::collect()
    }
  }

  # Step 3: Trim vocabulary tables

  # concept - filter to used_ids (intersected with what actually exists)
  if ("concept" %in% names(cdm)) {
    cdm[["concept"]] <- cdm[["concept"]] |>
      dplyr::filter(.data$concept_id %in% !!used_ids) |>
      .computeTrimmed("concept")
  }

  # For graph completeness, join against the trimmed concept table
  # so we never reference concept_ids that don't exist in concept
  concept_ref <- cdm[["concept"]] |> dplyr::select("concept_id")

  # concept_ancestor - both endpoints must exist in trimmed concept
  if ("concept_ancestor" %in% names(cdm)) {
    cdm[["concept_ancestor"]] <- cdm[["concept_ancestor"]] |>
      dplyr::semi_join(
        concept_ref, by = c("ancestor_concept_id" = "concept_id")
      ) |>
      dplyr::semi_join(
        concept_ref, by = c("descendant_concept_id" = "concept_id")
      ) |>
      .computeTrimmed("concept_ancestor")
  }

  # concept_relationship - both endpoints must exist in trimmed concept
  if ("concept_relationship" %in% names(cdm)) {
    cdm[["concept_relationship"]] <- cdm[["concept_relationship"]] |>
      dplyr::semi_join(
        concept_ref, by = c("concept_id_1" = "concept_id")
      ) |>
      dplyr::semi_join(
        concept_ref, by = c("concept_id_2" = "concept_id")
      ) |>
      .computeTrimmed("concept_relationship")
  }

  # concept_synonym - truncate
  if ("concept_synonym" %in% names(cdm)) {
    cdm[["concept_synonym"]] <- cdm[["concept_synonym"]] |>
      dplyr::filter(FALSE) |>
      .computeTrimmed("concept_synonym")
  }

  # source_to_concept_map - truncate
  if ("source_to_concept_map" %in% names(cdm)) {
    cdm[["source_to_concept_map"]] <- cdm[["source_to_concept_map"]] |>
      dplyr::filter(FALSE) |>
      .computeTrimmed("source_to_concept_map")
  }

  # drug_strength
  if ("drug_strength" %in% names(cdm)) {
    cdm[["drug_strength"]] <- cdm[["drug_strength"]] |>
      dplyr::filter(.data$drug_concept_id %in% !!used_ids) |>
      .computeTrimmed("drug_strength")
  }

  # vocabulary - keep only vocabularies referenced in trimmed concept table
  if ("vocabulary" %in% names(cdm) && "concept" %in% names(cdm)) {
    vocab_ids <- cdm[["concept"]] |>
      dplyr::select("vocabulary_id") |>
      dplyr::distinct()

    cdm[["vocabulary"]] <- cdm[["vocabulary"]] |>
      dplyr::semi_join(vocab_ids, by = "vocabulary_id") |>
      .computeTrimmed("vocabulary")
  }

  # domain - keep only domains referenced in trimmed concept table
  if ("domain" %in% names(cdm) && "concept" %in% names(cdm)) {
    domain_ids <- cdm[["concept"]] |>
      dplyr::select("domain_id") |>
      dplyr::distinct()

    cdm[["domain"]] <- cdm[["domain"]] |>
      dplyr::semi_join(domain_ids, by = "domain_id") |>
      .computeTrimmed("domain")
  }

  # concept_class - keep only classes referenced in trimmed concept table
  if ("concept_class" %in% names(cdm) && "concept" %in% names(cdm)) {
    class_ids <- cdm[["concept"]] |>
      dplyr::select("concept_class_id") |>
      dplyr::distinct()

    cdm[["concept_class"]] <- cdm[["concept_class"]] |>
      dplyr::semi_join(class_ids, by = "concept_class_id") |>
      .computeTrimmed("concept_class")
  }

  # relationship - keep only relationships in trimmed concept_relationship
  if ("relationship" %in% names(cdm) && "concept_relationship" %in% names(cdm)) {
    rel_ids <- cdm[["concept_relationship"]] |>
      dplyr::select("relationship_id") |>
      dplyr::distinct()

    cdm[["relationship"]] <- cdm[["relationship"]] |>
      dplyr::semi_join(rel_ids, by = "relationship_id") |>
      .computeTrimmed("relationship")
  }

  message("Vocabulary trimming complete.")
  return(cdm)
}
