#!/usr/bin/env Rscript

# Download all files from a Figshare COLLECTION (public) by collection_id
# COHD collection: 4151252

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(tools)
})

collection_id <- 4151252
api_base <- "https://api.figshare.com/v2"
out_dir <- file.path(getwd(), paste0("figshare_collection_", collection_id))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ---- helpers ----

http_get_json <- function(url, query = list()) {
  resp <- httr::GET(url, query = query, httr::timeout(60))
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  if (httr::http_error(resp)) {
    stop(sprintf("GET failed [%s] %s\n%s", httr::status_code(resp), url, txt))
  }
  if (nchar(txt) == 0) return(list())
  jsonlite::fromJSON(txt, simplifyVector = TRUE)
}

safe_name <- function(x, max_len = 120) {
  x <- gsub("[/\\\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  if (nchar(x) > max_len) x <- substr(x, 1, max_len)
  x
}

download_file <- function(url, dest) {
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(dest) && file.info(dest)$size > 0) {
    message("SKIP (exists): ", dest)
    return(invisible(TRUE))
  }
  message("GET  ", url)
  ok <- tryCatch({
    resp <- httr::GET(url, httr::write_disk(dest, overwrite = TRUE), httr::timeout(600))
    if (httr::http_error(resp)) {
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      stop(sprintf("Download failed [%s]\n%s", httr::status_code(resp), txt))
    }
    TRUE
  }, error = function(e) {
    message("ERROR: ", conditionMessage(e))
    FALSE
  })
  ok
}

# ---- 1) list articles in collection (public endpoint first) ----
list_collection_articles <- function(collection_id) {
  # Public endpoint (works for public collections): /collections/{id}/articles
  # Fallback: /account/collections/{id}/articles (often needs auth; but sometimes works)
  endpoints <- c(
    sprintf("%s/collections/%s/articles", api_base, collection_id),
    sprintf("%s/account/collections/%s/articles", api_base, collection_id)
  )

  for (ep in endpoints) {
    message("Listing articles via: ", ep)
    page <- 1
    all <- list()

    repeat {
      # Figshare supports page-based pagination :contentReference[oaicite:1]{index=1}
      res <- tryCatch(
        http_get_json(ep, query = list(page = page, page_size = 1000)),
        error = function(e) NULL
      )

      if (is.null(res)) {
        message("  failed endpoint: ", ep)
        all <- NULL
        break
      }

      if (length(res) == 0) break
      all[[length(all) + 1]] <- res
      message("  page ", page, ": ", length(res), " articles")
      page <- page + 1
    }

    if (!is.null(all)) {
      return(do.call(rbind, all))
    }
  }

  stop("Could not list collection articles. If this is a private collection you need an API token.")
}

articles <- list_collection_articles(collection_id)

if (!("id" %in% names(articles))) {
  stop("Unexpected API response: article list did not include 'id'.")
}

message("Total articles found: ", nrow(articles))

# ---- 2) for each article, fetch files and download ----
manifest <- data.frame(
  collection_id = integer(),
  article_id = integer(),
  article_title = character(),
  file_id = integer(),
  file_name = character(),
  file_size = numeric(),
  download_url = character(),
  saved_to = character(),
  ok = logical(),
  stringsAsFactors = FALSE
)

for (i in seq_len(nrow(articles))) {
  article_id <- articles$id[i]
  article_title <- if ("title" %in% names(articles)) articles$title[i] else paste0("article_", article_id)
  article_dir <- file.path(out_dir, paste0(article_id, "__", safe_name(article_title)))
  dir.create(article_dir, recursive = TRUE, showWarnings = FALSE)

  article_url <- sprintf("%s/articles/%s", api_base, article_id)
  message("\nArticle ", i, "/", nrow(articles), ": ", article_id, " — ", article_title)
  art <- http_get_json(article_url)

  if (is.null(art$files) || length(art$files) == 0) {
    message("  No files listed for article ", article_id)
    next
  }

  files <- art$files
  # Normalize to data.frame
  files_df <- as.data.frame(files, stringsAsFactors = FALSE)

  for (j in seq_len(nrow(files_df))) {
    fid <- files_df$id[j]
    fname <- files_df$name[j]

    # Prefer API-provided download_url; fallback to standard ndownloader/files/{file_id} :contentReference[oaicite:2]{index=2}
    durl <- NA_character_
    if ("download_url" %in% names(files_df) && !is.na(files_df$download_url[j]) && nzchar(files_df$download_url[j])) {
      durl <- files_df$download_url[j]
    } else {
      durl <- sprintf("https://figshare.com/ndownloader/files/%s", fid)
    }

    dest <- file.path(article_dir, fname)
    ok <- download_file(durl, dest)

    manifest <- rbind(manifest, data.frame(
      collection_id = collection_id,
      article_id = article_id,
      article_title = article_title,
      file_id = fid,
      file_name = fname,
      file_size = if ("size" %in% names(files_df)) files_df$size[j] else NA_real_,
      download_url = durl,
      saved_to = dest,
      ok = ok,
      stringsAsFactors = FALSE
    ))
  }
}

# ---- 3) write manifest ----
manifest_path <- file.path(out_dir, "download_manifest.csv")
write.csv(manifest, manifest_path, row.names = FALSE)
message("\nDone.")
message("Saved files under: ", out_dir)
message("Manifest: ", manifest_path)

# Exit non-zero if any failed downloads
if (any(!manifest$ok)) {
  quit(status = 2)
}
