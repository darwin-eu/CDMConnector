cohd_similar_concepts <- function(concept_id,
                                  dataset_id = 1,
                                  base_url = "https://cohd-api.ci.transltr.io/api",
                                  top_n = 50,
                                  timeout_sec = 30) {

  stopifnot(is.numeric(concept_id) || grepl("^[0-9]+$", concept_id))
  concept_id <- as.integer(concept_id)

  library(httr)
  library(jsonlite)

  get_json <- function(path, query) {
    url <- paste0(sub("/+$", "", base_url), path)
    resp <- GET(url, query = query, timeout(timeout_sec))
    if (http_error(resp)) {
      stop("COHD API request failed: ", status_code(resp), "\n",
           content(resp, as = "text", encoding = "UTF-8"))
    }
    fromJSON(content(resp, as = "text", encoding = "UTF-8"), flatten = TRUE)
  }

  # Use association endpoint directly
  res <- get_json(
    path = "/association/relativeFrequency",
    query = list(
      dataset_id = dataset_id,
      concept_id_1 = concept_id
    )
  )

  df <- as.data.frame(res$results, stringsAsFactors = FALSE)
  if (nrow(df) == 0) return(df)

  # Identify the "other" concept
  df$other_concept_id <- ifelse(df$concept_id_1 == concept_id,
                                df$concept_id_2,
                                df$concept_id_1)

  # Keep only meaningful associations
  df <- df[!is.na(df$relative_frequency), ]

  # Sort by relative frequency
  df <- df[order(df$relative_frequency, decreasing = TRUE), ]

  head(df, top_n)
}

res <- cohd_similar_concepts(201826, dataset_id = 1, top_n = 25)
print(res)
