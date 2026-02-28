# Copyright 2025 DARWIN EU®
#
# This file is part of CDMConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use it except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get similar concepts from Columbia Open Health Data (COHD) API
#'
#' Queries the COHD API association/relativeFrequency endpoint to return concepts
#' that co-occur with the given concept(s), ranked by relative frequency. Useful for
#' finding clinically related conditions, drugs, or procedures based on EHR prevalence.
#' When given multiple concept IDs, returns concepts that co-occur with the input set,
#' ranked by how many input concepts they co-occur with and by mean relative frequency.
#'
#' @param conceptId Integer or character vector. One or more OMOP concept IDs to find
#'   similar concepts for (e.g. conditions, drugs, or procedures).
#' @param datasetId Integer. COHD dataset ID (1 = 5-year, 2 = lifetime; default 1).
#' @param baseUrl Character. Base URL of the COHD API (default points to public
#'   tranSMART Foundation instance).
#' @param topN Integer. Maximum number of similar concepts to return (default 50).
#'   For a single concept, this limits rows by strength; for multiple concepts, this
#'   limits the aggregated result.
#' @param timeoutSec Numeric. Request timeout in seconds (default 30).
#'
#' @return
#'   A data frame with one row per similar concept, or `NULL` if the API is
#'   unavailable or the request fails. When successful:
#'   - **Single concept**: data frame contains `concept_id_1`, `concept_id_2`,
#'     `concept_count_1`, `concept_count_2`, `concept_count`, `relative_frequency`,
#'     and `other_concept_id`; rows sorted by `relative_frequency` descending.
#'   - **Multiple concepts**: data frame contains `other_concept_id`, `n_concepts`
#'     (how many input concepts co-occur with this one), and `mean_rf` (mean
#'     relative frequency); rows sorted by `n_concepts` descending then `mean_rf`
#'     descending.
#'   If no results or an error occurs, returns `NULL` and a message is printed.
#'
#' @references
#'   Ta, Casey N.; Dumontier, Michel; Hripcsak, George; P. Tatonetti, Nicholas;
#'   Weng, Chunhua (2018). Columbia Open Health Data, a database of EHR prevalence
#'   and co-occurrence of conditions, drugs, and procedures. figshare. Collection.
#'   \url{https://doi.org/10.6084/m9.figshare.c.4151252.v1}
#'
#'   COHD collection: \url{https://figshare.com/collections/Columbia_Open_Health_Data_a_database_of_EHR_prevalence_and_co-occurrence_of_conditions_drugs_and_procedures/4151252}
#'
#' @importFrom httr GET content timeout http_error status_code
#' @importFrom jsonlite fromJSON
#' @importFrom utils head
#'
#' @examples
#' \dontrun{
#' # Single concept: top 25 similar to concept 201826 (Type 2 diabetes)
#' cohdSimilarConcepts(201826, datasetId = 1, topN = 25)
#'
#' # Multiple concepts: concepts likely to co-occur with this set
#' cohdSimilarConcepts(c(201826, 316866, 255573), datasetId = 1, topN = 50)
#' }
#'
#' @export
cohdSimilarConcepts <- function(conceptId,
                               datasetId = 1,
                               baseUrl = "https://cohd-api.ci.transltr.io/api",
                               topN = 50,
                               timeoutSec = 30) {

  conceptId <- as.integer(conceptId)
  stopifnot(length(conceptId) >= 1L, all(!is.na(conceptId)))

  getJson <- function(path, query) {
    url <- paste0(sub("/+$", "", baseUrl), path)
    resp <- httr::GET(url, query = query, httr::timeout(timeoutSec))
    if (httr::http_error(resp)) {
      return(NULL)
    }
    jsonlite::fromJSON(
      httr::content(resp, as = "text", encoding = "UTF-8"),
      flatten = TRUE
    )
  }

  fetchOne <- function(cid) {
    res <- tryCatch(
      getJson(
        path = "/association/relativeFrequency",
        query = list(dataset_id = datasetId, concept_id_1 = cid)
      ),
      error = function(e) {
        message("COHD API request failed: ", conditionMessage(e))
        return(NULL)
      }
    )
    if (is.null(res) || is.null(res$results) || length(res$results) == 0) {
      return(NULL)
    }
    df <- as.data.frame(res$results, stringsAsFactors = FALSE)
    if (nrow(df) == 0) return(NULL)
    df$other_concept_id <- ifelse(df$concept_id_1 == cid, df$concept_id_2, df$concept_id_1)
    df$source_concept_id <- cid
    df[!is.na(df$relative_frequency), ]
  }

  if (length(conceptId) == 1L) {
    out <- fetchOne(conceptId[1L])
    if (is.null(out) || nrow(out) == 0) {
      message("COHD API is not available or request failed. Returning NULL.")
      return(NULL)
    }
    out <- out[order(out$relative_frequency, decreasing = TRUE), ]
    return(head(out, topN))
  }

  # Multiple concepts: get similar for each, then aggregate
  results <- lapply(conceptId, fetchOne)
  results <- results[!sapply(results, is.null)]
  if (length(results) == 0L) {
    message("COHD API is not available or request failed. Returning NULL.")
    return(NULL)
  }
  results <- do.call(rbind, results)
  if (is.null(results) || nrow(results) == 0L) {
    message("No results from COHD API.")
    return(NULL)
  }

  n_per_other <- aggregate(
    source_concept_id ~ other_concept_id,
    data = results,
    FUN = function(x) length(unique(x))
  )
  mean_rf_per_other <- aggregate(
    relative_frequency ~ other_concept_id,
    data = results,
    FUN = mean
  )
  agg <- merge(n_per_other, mean_rf_per_other, by = "other_concept_id")
  names(agg) <- c("other_concept_id", "n_concepts", "mean_rf")
  agg <- agg[order(-agg$n_concepts, -agg$mean_rf), ]
  head(agg, topN)
}
