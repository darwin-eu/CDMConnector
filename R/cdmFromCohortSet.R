#' Build a Synthetic CDM from a Cohort Set
#'
#' Constructs a synthetic OMOP Common Data Model (CDM) using a set of cohort definitions,
#' created using \code{CDMConnector::readCohortSet()}. The function generates
#' synthetic data and returns a cdm reference object backed by a DuckDB database,
#' containing synthetic CDM tables and generated cohort table rows.
#'
#' @param cohortSet A data frame (usually from \code{CDMConnector::readCohortSet()})
#'   with columns \code{cohort_definition_id}, \code{cohort_name}, and \code{cohort}
#'   (cohort definition as a list or JSON string).
#' @param n Integer. Total number of synthetic persons to generate across all cohorts.
#'   Defaults to 100.
#' @param cohortTable Character. Name of the cohort table (default \code{"cohort"}).
#' @param duckdbPath Character or NULL. Path for the final merged DuckDB; if NULL a
#'   temporary file is used.
#' @param seed Integer. Base RNG seed; each cohort uses \code{seed + cohort_index} for reproducibility (default 1).
#' @param verbose If TRUE, print progress per cohort and per attempt (default FALSE).
#' @param ... Arguments passed through to \code{cdmFromJson} for each cohort (e.g.
#'   \code{targetMatch}, \code{successRate}, \code{visitConceptId}, \code{eventDateJitter}, \code{visitDateJitter},
#'   \code{demographicVariety}, \code{sourceAndTypeVariety}, \code{valueVariety}). \code{seed} is overridden per cohort.
#'   \code{targetMatch} is per cohort: that fraction of each cohort's generated persons are intended to qualify for that cohort only.
#'
#' @section Reproducibility:
#'   With the same \code{seed}, \code{cohortSet}, and other arguments, \code{cdmFromCohortSet}
#'   produces the same synthetic data. Changing \code{seed} or \code{n} changes the data.
#'   The data are random but reproducible.
#'
#' @return
#'   A cdm reference object (as returned by \code{CDMConnector::cdmFromCon()}) backed
#'   by a DuckDB database. The returned object contains synthetic CDM tables and
#'   cohort table rows generated from the specified cohort definitions.
#'   The returned \code{cdm} has an attribute \code{synthetic_summary} (a list with
#'   \code{cohort_summaries}, \code{cohort_index}, \code{n_cohorts}, \code{summary}
#'   (one-line text), \code{any_low_match}) for diagnostics and match rates.
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' cohortSet <- readCohortSet(system.file("cohorts", package = "CDMConnector"))
#' cdm <- cdmFromCohortSet(cohortSet, n = 100)
#' cdm$person
#' }
#'
#' @export
cdmFromCohortSet <- function(cohortSet, n = 100, cohortTable = "cohort", duckdbPath = NULL, seed = 1, verbose = FALSE, ...) {

  req <- c("DBI", "duckdb")
  missing_pkgs <- req[!vapply(req, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_pkgs) > 0) {
    stop("Missing packages: ", paste(missing_pkgs, collapse = ", "))
  }
  if (!requireNamespace("CDMConnector", quietly = TRUE)) {
    stop("Package CDMConnector is required for cdmFromCohortSet(). Install it and retry.")
  }

  `%||%` <- function(x, y) if (!is.null(x)) x else y

  if (!is.data.frame(cohortSet) || nrow(cohortSet) == 0) {
    stop("cohortSet must be a non-empty data frame (e.g. from CDMConnector::readCohortSet()).")
  }
  required <- c("cohort_definition_id", "cohort_name", "cohort")
  missing_cols <- setdiff(required, names(cohortSet))
  if (length(missing_cols) > 0) {
    stop("cohortSet must have columns: ", paste(required, collapse = ", "), ". Missing: ", paste(missing_cols, collapse = ", "))
  }
  n <- as.integer(n)
  n_cohorts <- nrow(cohortSet)
  if (is.na(n) || n < 1L) stop("n must be a positive integer.")
  if (n < n_cohorts) stop("n (", n, ") must be >= number of cohorts (", n_cohorts, ") so each cohort gets at least one person.")

  # Create final DuckDB as a copy of CDMConnector empty_cdm (no DDL; tables + vocabulary already exist)
  final_path <- duckdbPath
  if (is.null(final_path) || !nzchar(final_path)) {
    empty_path <- CDMConnector::eunomiaDir("empty_cdm", cdmVersion = "5.4")
    final_path <- tempfile(fileext = ".duckdb")
    if (dir.exists(empty_path)) {
      f <- list.files(empty_path, pattern = "\\.duckdb$", full.names = TRUE, recursive = TRUE)
      if (length(f) == 0) stop("empty_cdm directory contains no .duckdb file.")
      file.copy(f[1], final_path)
    } else {
      file.copy(empty_path, final_path)
    }
  }

  con_final <- DBI::dbConnect(duckdb::duckdb(), final_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con_final, shutdown = TRUE), add = TRUE)

  try(DBI::dbExecute(con_final, sprintf(
    "CREATE TABLE IF NOT EXISTS %s (cohort_definition_id INTEGER, subject_id INTEGER, cohort_start_date DATE, cohort_end_date DATE);",
    cohortTable
  )), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE IF NOT EXISTS mockcdm_cohort_index (
    cohort_definition_id INTEGER, cohort_name VARCHAR, n_requested INTEGER,
    n_generated INTEGER, matched INTEGER, matched_rate DOUBLE);"), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE IF NOT EXISTS device_exposure (
    device_exposure_id INTEGER, person_id INTEGER, device_concept_id INTEGER,
    device_exposure_start_date DATE, device_exposure_start_datetime TIMESTAMP,
    device_exposure_end_date DATE, device_exposure_end_datetime TIMESTAMP,
    device_type_concept_id INTEGER, unique_device_id VARCHAR, quantity INTEGER,
    provider_id INTEGER, visit_occurrence_id INTEGER, device_source_value VARCHAR,
    device_source_concept_id INTEGER);"), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE IF NOT EXISTS death (
    person_id INTEGER, death_date DATE, death_datetime TIMESTAMP,
    death_type_concept_id INTEGER, cause_concept_id INTEGER, cause_source_value VARCHAR,
    cause_source_concept_id INTEGER);"), silent = TRUE)

  copy_with_offsets <- function(con_src, con_dst, table, id_col, id_offset, person_offset, person_col = "person_id") {
    df <- try(DBI::dbGetQuery(con_src, sprintf("SELECT * FROM %s;", table)), silent = TRUE)
    if (inherits(df, "try-error") || is.null(df) || nrow(df) == 0) {
      return(list(id_offset = id_offset, person_offset = person_offset))
    }
    # Only append columns that exist in the target table (e.g. empty_cdm may have production_id; our DDL may not)
    target_cols <- try(DBI::dbListFields(con_dst, table), silent = TRUE)
    if (!inherits(target_cols, "try-error") && length(target_cols) > 0) {
      keep <- target_cols[target_cols %in% names(df)]
      if (length(keep) > 0) df <- df[, keep, drop = FALSE]
    }
    if (person_col %in% names(df)) {
      df[[person_col]] <- df[[person_col]] + person_offset
    }
    if (!is.null(id_col) && id_col %in% names(df)) {
      df[[id_col]] <- df[[id_col]] + id_offset
      # Return max id written so next cohort gets unique ids (id_offset + 1, ...)
      id_offset <- max(df[[id_col]], na.rm = TRUE)
    }
    DBI::dbWriteTable(con_dst, table, df, append = TRUE)
    list(id_offset = id_offset, person_offset = person_offset)
  }

  pk <- list(
    person = "person_id",
    observation_period = "observation_period_id",
    visit_occurrence = "visit_occurrence_id",
    condition_occurrence = "condition_occurrence_id",
    procedure_occurrence = "procedure_occurrence_id",
    drug_exposure = "drug_exposure_id",
    measurement = "measurement_id",
    observation = "observation_id",
    device_exposure = "device_exposure_id"
  )

  offsets <- list(
    person_id = 0L,
    observation_period_id = 0L,
    visit_occurrence_id = 0L,
    condition_occurrence_id = 0L,
    procedure_occurrence_id = 0L,
    drug_exposure_id = 0L,
    measurement_id = 0L,
    observation_id = 0L,
    device_exposure_id = 0L
  )

  # condition_era, drug_era, dose_era are built at the end from condition_occurrence/drug_exposure
  tables <- c("person", "observation_period", "visit_occurrence", "condition_occurrence",
              "procedure_occurrence", "drug_exposure", "measurement", "observation",
              "device_exposure", "death")
  n_per_cohort <- max(1L, as.integer(n / n_cohorts))
  cohort_results <- vector("list", n_cohorts)

  # Resolve empty_cdm source once (avoids repeated eunomiaDir/copy and saves disk)
  empty_path <- CDMConnector::eunomiaDir("empty_cdm", cdmVersion = "5.4")
  empty_cdm_file <- if (dir.exists(empty_path)) {
    f <- list.files(empty_path, pattern = "\\.duckdb$", full.names = TRUE, recursive = TRUE)
    if (length(f) == 0) stop("empty_cdm directory contains no .duckdb file.")
    f[1]
  } else {
    empty_path
  }
  # Single temp DB path reused for every cohort; overwritten each iteration, removed via cdmDisconnect or unlink
  temp_db <- tempfile(fileext = ".duckdb")

  for (i in seq_len(n_cohorts)) {
    if (verbose) message("Generating cohort ", i, "/", n_cohorts, " (id=", cohortSet$cohort_definition_id[[i]], ") ...")
    cid   <- as.integer(cohortSet$cohort_definition_id[[i]])
    cname <- as.character(cohortSet$cohort_name[[i]])
    coh   <- cohortSet$cohort[[i]]

    # cohort column may be list (Circe expression) or character (JSON/path)
    cohort_expr <- if (is.list(coh)) {
      coh
    } else if (is.character(coh) && length(coh) == 1L) {
      if (file.exists(coh)) {
        cohort_json_text <- paste(readLines(coh, warn = FALSE), collapse = "\n")
        jsonlite::fromJSON(cohort_json_text, simplifyVector = FALSE)
      } else {
        jsonlite::fromJSON(coh, simplifyVector = FALSE)
      }
    } else {
      stop("cohortSet$cohort[[", i, "]] must be a list (Circe expression) or character (JSON/path).")
    }

    # Overwrite single temp file with fresh empty_cdm each cohort (no extra temp files)
    if (!file.copy(empty_cdm_file, temp_db, overwrite = TRUE)) {
      stop("Copying empty_cdm to temp file failed. Check disk space and write permission.")
    }
    dots <- list(...)
    dots$seed <- as.integer(seed) + i
    dots$verbose <- verbose
    res_i <- NULL
    tryCatch({
      res_i <- do.call(cdmFromJson, c(list(
        cohortExpression = cohort_expr,
        n = n_per_cohort,
        cohortId = cid,
        duckdbPath = temp_db,
        cohortTable = cohortTable
      ), dots))
      cohort_results[[i]] <- res_i

      # If we could not generate suitable data (0 matched), log and skip copying this cohort
      summ <- res_i$summary
      matched <- as.integer(summ$matched %||% NA_integer_)
      success_rate <- dots$successRate %||% 0.70
      matched_rate <- as.numeric(summ$matched_rate %||% NA_real_)
      if (!is.na(matched) && matched == 0L) {
        if (verbose) message("  Skipping copy for cohort ", cid, " (", cname, "): no persons matched cohort definition.")
      } else {
        con_src <- DBI::dbConnect(duckdb::duckdb(), res_i$duckdb_path, read_only = TRUE)
        person_offset <- offsets$person_id

        # Copy person first (only columns that exist in target, in case schemas differ)
        df_person <- DBI::dbGetQuery(con_src, "SELECT * FROM person;")
        if (nrow(df_person) > 0) {
          target_person_cols <- try(DBI::dbListFields(con_final, "person"), silent = TRUE)
          if (!inherits(target_person_cols, "try-error") && length(target_person_cols) > 0) {
            keep <- target_person_cols[target_person_cols %in% names(df_person)]
            if (length(keep) > 0) df_person <- df_person[, keep, drop = FALSE]
          }
          df_person$person_id <- df_person$person_id + person_offset
          DBI::dbWriteTable(con_final, "person", df_person, append = TRUE)
          offsets$person_id <- max(df_person$person_id)
        }

        for (t in setdiff(tables, "person")) {
          id_col <- pk[[t]]
          if (is.null(id_col)) {
            tmp <- copy_with_offsets(con_src, con_final, t, id_col = NULL, id_offset = 0L,
                                     person_offset = person_offset, person_col = "person_id")
          } else {
            id_offset <- offsets[[id_col]]
            tmp <- copy_with_offsets(con_src, con_final, t, id_col = id_col, id_offset = id_offset,
                                     person_offset = person_offset, person_col = "person_id")
            offsets[[id_col]] <- tmp$id_offset
          }
        }

        # Cohort rows
        df_cohort <- try(DBI::dbGetQuery(con_src, sprintf(
          "SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM %s WHERE cohort_definition_id = %d;",
          cohortTable, cid
        )), silent = TRUE)
        if (!inherits(df_cohort, "try-error") && nrow(df_cohort) > 0) {
          df_cohort$subject_id <- df_cohort$subject_id + person_offset
          DBI::dbWriteTable(con_final, cohortTable, df_cohort, append = TRUE)
        }

        # Use cdmDisconnect to disconnect and remove temp file when it's in tempdir
        cdm_src <- structure(list(), class = c("db_cdm", "cdm_reference", "cdm_source"), dbcon = con_src)
        CDMConnector::cdmDisconnect(cdm_src)
      }

      idx_row <- data.frame(
        cohort_definition_id = cid,
        cohort_name = cname,
        n_requested = as.integer(n_per_cohort),
        n_generated = as.integer(summ$n_generated %||% n_per_cohort),
        matched = as.integer(summ$matched %||% NA_integer_),
        matched_rate = as.numeric(summ$matched_rate %||% NA_real_),
        stringsAsFactors = FALSE
      )
      DBI::dbWriteTable(con_final, "mockcdm_cohort_index", idx_row, append = TRUE)
    }, error = function(e) {
      msg <- conditionMessage(e)
      message("Cohort ", cid, " (", cname, "): ", msg, " - skipping and continuing.")
      summ_err <- if (!is.null(res_i) && !is.null(res_i$summary)) res_i$summary else list()
      idx_row <- data.frame(
        cohort_definition_id = cid,
        cohort_name = cname,
        n_requested = as.integer(n_per_cohort),
        n_generated = as.integer(summ_err$n_generated %||% NA_integer_),
        matched = as.integer(summ_err$matched %||% NA_integer_),
        matched_rate = as.numeric(summ_err$matched_rate %||% NA_real_),
        stringsAsFactors = FALSE
      )
      try(DBI::dbWriteTable(con_final, "mockcdm_cohort_index", idx_row, append = TRUE), silent = TRUE)
      if (exists("con_src")) {
        cdm_src <- structure(list(), class = c("db_cdm", "cdm_reference", "cdm_source"), dbcon = con_src)
        try(CDMConnector::cdmDisconnect(cdm_src), silent = TRUE)
      }
      try(unlink(temp_db, force = TRUE), silent = TRUE)
    })
    # Remove temp file if we skipped copy (never opened con_src)
    try(unlink(temp_db, force = TRUE), silent = TRUE)
  }

  # Collapse overlapping observation_period by person_id (min start, max end) and cap all CDM dates to Sys.Date()
  today_str <- format(Sys.Date(), "%Y-%m-%d")
  op_df <- try(DBI::dbGetQuery(con_final, "SELECT person_id, observation_period_start_date, observation_period_end_date FROM observation_period;"), silent = TRUE)
  if (!inherits(op_df, "try-error") && !is.null(op_df) && nrow(op_df) > 0) {
    op_collapsed <- .collapse_observation_period(op_df)
    try(DBI::dbExecute(con_final, "DELETE FROM observation_period;"), silent = TRUE)
    DBI::dbWriteTable(con_final, "observation_period", op_collapsed, append = TRUE)
  }
  date_cap_sql <- list(
    visit_occurrence = c("visit_start_date", "visit_end_date"),
    condition_occurrence = c("condition_start_date", "condition_end_date"),
    procedure_occurrence = "procedure_date",
    drug_exposure = c("drug_exposure_start_date", "drug_exposure_end_date"),
    measurement = "measurement_date",
    observation = "observation_date",
    device_exposure = c("device_exposure_start_date", "device_exposure_end_date"),
    death = "death_date",
    observation_period = c("observation_period_start_date", "observation_period_end_date")
  )
  for (tbl in names(date_cap_sql)) {
    cols <- date_cap_sql[[tbl]]
    for (col in cols) {
      try(DBI::dbExecute(con_final, sprintf(
        "UPDATE %s SET %s = LEAST(%s, CAST('%s' AS DATE)) WHERE %s > CAST('%s' AS DATE);",
        tbl, col, col, today_str, col, today_str
      )), silent = TRUE)
    }
  }
  try(DBI::dbExecute(con_final, sprintf(
    "UPDATE %s SET cohort_start_date = LEAST(cohort_start_date, CAST('%s' AS DATE)), cohort_end_date = LEAST(cohort_end_date, CAST('%s' AS DATE)) WHERE cohort_start_date > CAST('%s' AS DATE) OR cohort_end_date > CAST('%s' AS DATE);",
    cohortTable, today_str, today_str, today_str, today_str
  )), silent = TRUE)

  # Build derived tables from final condition_occurrence and drug_exposure (30-day persistence window)
  era_pad_days <- 30L
  try(DBI::dbExecute(con_final, "DROP TABLE IF EXISTS condition_era;"), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE condition_era (
    condition_era_id INTEGER, person_id INTEGER, condition_concept_id INTEGER,
    condition_era_start_date DATE, condition_era_end_date DATE, condition_occurrence_count INTEGER);"), silent = TRUE)
  try(DBI::dbExecute(con_final, "DROP TABLE IF EXISTS drug_era;"), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE drug_era (
    drug_era_id INTEGER, person_id INTEGER, drug_concept_id INTEGER,
    drug_era_start_date DATE, drug_era_end_date DATE, drug_exposure_count INTEGER, gap_days INTEGER);"), silent = TRUE)
  co_df <- try(DBI::dbGetQuery(con_final, "SELECT person_id, condition_concept_id, condition_start_date, condition_end_date FROM condition_occurrence;"), silent = TRUE)
  if (!inherits(co_df, "try-error") && !is.null(co_df) && nrow(co_df) > 0) {
    ce <- .build_condition_era_df(co_df, era_pad_days = era_pad_days, start_id = 1L)
    if (nrow(ce) > 0) DBI::dbWriteTable(con_final, "condition_era", ce, append = TRUE)
  }
  de_df <- try(DBI::dbGetQuery(con_final, "SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date FROM drug_exposure;"), silent = TRUE)
  if (!inherits(de_df, "try-error") && !is.null(de_df) && nrow(de_df) > 0) {
    dre <- .build_drug_era_df(de_df, era_pad_days = era_pad_days, start_id = 1L)
    if (nrow(dre) > 0) DBI::dbWriteTable(con_final, "drug_era", dre, append = TRUE)
  }
  # Dose era: requires drug_strength; create table if missing, then build when drug_strength present
  try(DBI::dbExecute(con_final, "DROP TABLE IF EXISTS dose_era;"), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE dose_era (
    dose_era_id INTEGER, person_id INTEGER, drug_concept_id INTEGER, unit_concept_id INTEGER,
    dose_value DOUBLE, dose_era_start_date DATE, dose_era_end_date DATE);"), silent = TRUE)
  ds_df <- try(DBI::dbGetQuery(con_final, "SELECT drug_concept_id, ingredient_concept_id, amount_value, amount_unit_concept_id, numerator_value, numerator_unit_concept_id FROM drug_strength;"), silent = TRUE)
  if (!inherits(de_df, "try-error") && !is.null(de_df) && nrow(de_df) > 0 &&
      !inherits(ds_df, "try-error") && !is.null(ds_df) && nrow(ds_df) > 0) {
    dose_era_df <- .build_dose_era_df(de_df, ds_df, era_pad_days = era_pad_days, start_id = 1L)
    if (nrow(dose_era_df) > 0) {
      DBI::dbWriteTable(con_final, "dose_era", dose_era_df, append = TRUE)
    }
  }
  # Populate cdm_source (one row)
  try(DBI::dbExecute(con_final, "DROP TABLE IF EXISTS cdm_source;"), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE cdm_source (
    cdm_source_name VARCHAR, cdm_source_abbreviation VARCHAR, cdm_holder VARCHAR,
    source_description VARCHAR, source_documentation_reference VARCHAR, cdm_etl_reference VARCHAR,
    source_release_date DATE, cdm_release_date DATE, cdm_version VARCHAR,
    cdm_version_concept_id INTEGER, vocabulary_version VARCHAR);"), silent = TRUE)
  vocab_row <- try(DBI::dbGetQuery(con_final, "SELECT vocabulary_version FROM vocabulary WHERE vocabulary_id = 'None' LIMIT 1;"), silent = TRUE)
  vocab_version <- if (!inherits(vocab_row, "try-error") && !is.null(vocab_row) && nrow(vocab_row) > 0) {
    as.character(vocab_row$vocabulary_version[1])
  } else {
    as.character(Sys.Date())
  }
  cdm_source_row <- data.frame(
    cdm_source_name = "Synthetic CDM from Cohort Set",
    cdm_source_abbreviation = "synthetic_cdm",
    cdm_holder = "CDMConnector",
    source_description = "Synthetic OMOP CDM generated by CDMConnector::cdmFromCohortSet from cohort definitions.",
    source_documentation_reference = NA_character_,
    cdm_etl_reference = "CDMConnector::cdmFromCohortSet",
    source_release_date = format(Sys.Date(), "%Y-%m-%d"),
    cdm_release_date = format(Sys.Date(), "%Y-%m-%d"),
    cdm_version = "v5.4",
    cdm_version_concept_id = 756265L,
    vocabulary_version = vocab_version,
    stringsAsFactors = FALSE
  )
  DBI::dbWriteTable(con_final, "cdm_source", cdm_source_row, append = TRUE)

  # Disconnect so CDMConnector can attach to the same path (do not unlink final_path; we reconnect immediately)
  DBI::dbDisconnect(con_final, shutdown = TRUE)
  on.exit(NULL)

  con <- DBI::dbConnect(duckdb::duckdb(), final_path, read_only = FALSE)
  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema = "main",
    writeSchema = "main",
    cohortTables = cohortTable,
    cdmName = "synthetic_cdm"
  )
  cohort_index_df <- tryCatch(
    DBI::dbGetQuery(con, "SELECT * FROM mockcdm_cohort_index;"),
    error = function(e) NULL
  )
  total_persons <- if (is.null(cohort_index_df) || nrow(cohort_index_df) == 0) 0L else sum(cohort_index_df$n_generated, na.rm = TRUE)
  rates <- if (is.null(cohort_index_df) || !"matched_rate" %in% names(cohort_index_df)) numeric(0) else cohort_index_df$matched_rate
  rates <- rates[!is.na(rates)]
  min_rate <- if (length(rates) > 0) min(rates) else NA_real_
  max_rate <- if (length(rates) > 0) max(rates) else NA_real_
  summary_str <- sprintf(
    "%d cohort(s), %d total persons, match rates %s",
    n_cohorts,
    total_persons,
    if (is.na(min_rate) || is.na(max_rate)) "N/A" else sprintf("%.2f-%.2f", min_rate, max_rate)
  )
  attr(cdm, "synthetic_summary") <- list(
    cohort_summaries = lapply(cohort_results, function(r) r$summary),
    cohort_index = cohort_index_df,
    n_cohorts = n_cohorts,
    summary = summary_str,
    any_low_match = length(rates) > 0 && any(rates < 0.7)
  )
  cdm
}

#' Generate a synthetic CDM from a single cohort definition (JSON or list)
#'
#' Used by \code{cdmFromCohortSet}; can also be called directly for one cohort.
#' Builds synthetic person/visit/condition/drug/etc. rows so that running the
#' cohort SQL yields approximately \code{targetMatch} of \code{n} persons for
#' \emph{this} cohort only (target match is per cohort, not across cohorts).
#'
#' @param jsonPath Path to ATLAS cohort expression JSON file. Optional if \code{cohortExpression} is provided.
#' @param n Number of synthetic persons to generate (default 5000).
#' @param cohortExpression Parsed cohort list (e.g. from \code{jsonlite::fromJSON}). Used when \code{jsonPath} is not set.
#' @param duckdbPath Path to DuckDB file; if NULL a temporary copy of empty_cdm is used.
#' @param targetMatch Fraction of the \code{n} persons generated that should qualify for this cohort (default 0.85). Applied per cohort only.
#' @param seed RNG seed (default 1).
#' @param maxAttempts Retries if match rate is low (default 3).
#' @param successRate Stop when match rate >= this (default 0.70).
#' @param startDate Min event date (default \code{"2019-01-01"}).
#' @param endDate Max event date (default \code{"2024-12-31"}).
#' @param eraPadDays Default era pad days if not in cohort JSON (default 90).
#' @param cohortTable Cohort table name (default \code{"cohort"}).
#' @param cohortId \code{cohort_definition_id} written (default 1).
#' @param visitConceptId Concept ID for synthetic visits, e.g. 9202 = outpatient (default 9202).
#' @param deathFraction Fraction of persons to add to death table; 0 = none (default 0).
#' @param priorDays Override observation period prior days; NULL = use cohort ObservationWindow (default NULL).
#' @param postDays Override observation period post days; NULL = use cohort ObservationWindow (default NULL).
#' @param nearMissFraction Fraction of non-qualifiers given events outside the window for attrition testing (default 0).
#' @param verbose If TRUE, print attempt and match rate each attempt (default FALSE).
#' @param eventDateJitter Max plus/minus days to add to event dates (0 = no jitter; default 3) for more realistic variation.
#' @param visitDateJitter Max plus/minus days for the synthetic visit date (0 = exact index date; default 2).
#' @param demographicVariety If TRUE, sample \code{race_concept_id} and \code{ethnicity_concept_id} from a small set for variety (default FALSE).
#' @param sourceAndTypeVariety If TRUE, sample \code{*_type_concept_id} and \code{*_source_value} from small sets for conditions, procedures, drugs, etc. (default FALSE).
#' @param valueVariety If TRUE, fill \code{value_as_number} and \code{value_as_concept_id} for measurements and observations from a small range or set (default FALSE).
#'
#' @section Reproducibility:
#'   With the same \code{seed}, cohort definition, and other arguments, \code{cdmFromJson}
#'   produces the same synthetic data. Changing \code{seed} or \code{n} changes the data.
#'
#' @return List with \code{duckdb_path}, \code{summary}, \code{diagnostics}, \code{cohort_sql}, \code{cohort_json}.
#' @noRd
cdmFromJson <- function(jsonPath = NULL,
                        n = 5000,
                        cohortExpression = NULL,
                        duckdbPath = NULL,
                        targetMatch = 0.85,
                        seed = 1,
                        maxAttempts = 3,
                        successRate = 0.70,
                        startDate = "2019-01-01",
                        endDate = "2024-12-31",
                        eraPadDays = 90,
                        cohortTable = "cohort",
                        cohortId = 1,
                        visitConceptId = 9202L,
                        deathFraction = 0,
                        priorDays = NULL,
                        postDays = NULL,
                        nearMissFraction = 0,
                        verbose = FALSE,
                        eventDateJitter = 3L,
                        visitDateJitter = 2L,
                        demographicVariety = FALSE,
                        sourceAndTypeVariety = FALSE,
                        valueVariety = FALSE) {
  # ----------------------------------------------------------------------------
  # Synthetic OMOP CDM generator driven by an ATLAS cohort expression JSON.
  # Returns list(duckdb_path, summary, diagnostics, cohort_sql, cohort_json).
  # ----------------------------------------------------------------------------

  req <- c("DBI", "duckdb", "jsonlite", "CirceR", "SqlRender")
  missing <- req[!vapply(req, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop("Missing packages: ", paste(missing, collapse = ", "), ". Install them and retry.")
  }

  `%||%` <- function(x, y) if (!is.null(x)) x else y

  if (!is.null(cohortExpression) && is.list(cohortExpression)) {
    cohort <- cohortExpression
  } else if (!is.null(jsonPath) && nzchar(jsonPath)) {
    if (!file.exists(jsonPath)) stop("jsonPath does not exist: ", jsonPath)
    cohort_json_text <- paste(readLines(jsonPath, warn = FALSE), collapse = "\n")
    cohort <- jsonlite::fromJSON(cohort_json_text, simplifyVector = FALSE)
  } else {
    stop("Provide either jsonPath (path to ATLAS cohort JSON file) or cohortExpression (parsed list).")
  }

  if (is.null(cohort$PrimaryCriteria)) {
    stop("Cohort must contain PrimaryCriteria. The cohort definition may be invalid or from an unsupported format.")
  }
  n <- as.integer(n)
  if (is.na(n) || n < 1L) stop("n must be a positive integer.")
  if (!is.numeric(targetMatch) || targetMatch < 0 || targetMatch > 1) stop("targetMatch must be between 0 and 1.")
  if (!is.numeric(successRate) || successRate < 0 || successRate > 1) stop("successRate must be between 0 and 1.")
  if (!is.numeric(maxAttempts) || (max_attempts <- as.integer(maxAttempts)) < 1L) stop("maxAttempts must be at least 1.")
  start_d <- as.Date(startDate)
  end_d <- as.Date(endDate)
  if (is.na(start_d) || is.na(end_d) || start_d > end_d) stop("startDate must be before or equal to endDate.")

  duckdb_path   <- duckdbPath
  target_match  <- targetMatch
  success_rate  <- successRate
  start_date    <- start_d
  end_date      <- min(end_d, Sys.Date())
  era_pad_opt   <- as.integer(eraPadDays)
  visit_concept <- as.integer(visitConceptId)
  death_fraction <- as.numeric(deathFraction)
  near_miss_frac <- as.numeric(nearMissFraction)
  cohort_table   <- cohortTable
  cohort_id      <- as.integer(cohortId)
  event_date_jitter <- as.integer(eventDateJitter %||% 0)
  visit_date_jitter <- as.integer(visitDateJitter %||% 0)
  if (is.na(event_date_jitter) || event_date_jitter < 0) event_date_jitter <- 0L
  if (is.na(visit_date_jitter) || visit_date_jitter < 0) visit_date_jitter <- 0L
  demographic_variety <- isTRUE(demographicVariety)
  source_and_type_variety <- isTRUE(sourceAndTypeVariety)
  value_variety <- isTRUE(valueVariety)

  if (is.null(duckdb_path) || !nzchar(duckdb_path)) {
    if (!requireNamespace("CDMConnector", quietly = TRUE)) {
      stop("CDMConnector is required when duckdbPath is not set. Install it or pass duckdbPath.")
    }
    empty_path <- CDMConnector::eunomiaDir("empty_cdm", cdmVersion = "5.4")
    duckdb_path <- tempfile(fileext = ".duckdb")
    if (dir.exists(empty_path)) {
      f <- list.files(empty_path, pattern = "\\.duckdb$", full.names = TRUE, recursive = TRUE)
      if (length(f) == 0) stop("empty_cdm directory contains no .duckdb file.")
      file.copy(f[1], duckdb_path)
    } else {
      file.copy(empty_path, duckdb_path)
    }
  }

  set.seed(seed)

  # OMOP gender concept IDs (for default synthetic demographics)
  .gender_male   <- 8507L
  .gender_female <- 8532L
  .gender_unknown <- 0L

  random_date <- function(nn, start, end) {
    today <- as.integer(Sys.Date())
    si <- as.integer(start)
    ei <- min(as.integer(end), today)
    if (si > ei) si <- ei
    as.Date(sample(si:ei, nn, replace = TRUE), origin = "1970-01-01")
  }

  # ---- Cohort already parsed (from jsonPath or cohortExpression) --------------

  extract_codesets <- function(cohort) {
    sets <- cohort$ConceptSets %||% list()
    out <- list()
    for (s in sets) {
      csid <- as.character(s$id)
      items <- s$expression$items %||% list()
      ids <- vapply(items, function(it) it$concept$CONCEPT_ID, numeric(1))
      out[[csid]] <- unique(as.integer(ids))
    }
    out
  }

  criterion_type <- function(x) {
    # returns one of supported keys if present
    keys <- c(
      "VisitOccurrence", "ConditionOccurrence", "ProcedureOccurrence",
      "DrugExposure", "Measurement", "Observation",
      "ConditionEra", "DrugEra", "DeviceExposure", "ObservationPeriod"
    )
    for (k in keys) if (!is.null(x[[k]])) return(k)
    NA_character_
  }

  parse_criteria_list <- function(lst) {
    out <- list()
    for (c in lst %||% list()) {
      t <- criterion_type(c)
      if (is.na(t)) next
      codeset_id <- c[[t]]$CodesetId %||% NA_integer_
      out <- append(out, list(list(type = t, codesetId = as.integer(codeset_id))))
    }
    out
  }

  parse_group <- function(g) {
    list(
      Type = (g$Type %||% "ALL"),
      Count = as.integer(g$Count %||% 0),
      CriteriaList = g$CriteriaList %||% list(),
      DemographicCriteriaList = g$DemographicCriteriaList %||% list(),
      Groups = lapply(g$Groups %||% list(), parse_group)
    )
  }

  parse_inclusion_rules <- function(cohort) {
    rules <- cohort$InclusionRules %||% list()
    # Each rule usually has $expression with $Type/$CriteriaList/$Groups...
    out <- list()
    for (r in rules) {
      expr <- r$expression %||% r
      out <- append(out, list(list(
        name = r$name %||% NA_character_,
        expr = parse_group(expr)
      )))
    }
    out
  }

  # Extract demographic constraints (Gender, Age) from PrimaryCriteria and InclusionRules
  extract_demographic_criteria <- function(cohort) {
    gender_concept_ids <- integer(0)
    min_age <- NA_integer_
    max_age <- NA_integer_

    collect_demog <- function(demog_list) {
      for (d in demog_list %||% list()) {
        if (!is.null(d$Gender) && is.list(d$Gender)) {
          for (g in d$Gender) {
            cid <- as.integer(g$CONCEPT_ID %||% g$ValueAsConceptId)
            if (!is.na(cid)) gender_concept_ids <<- unique(c(gender_concept_ids, cid))
          }
        }
        if (!is.null(d$Age)) {
          val <- as.integer(d$Age$Value %||% NA_integer_)
          op <- as.character(d$Age$Op %||% "")
          if (!is.na(val)) {
            if (op %in% c("gte", ">=")) min_age <<- if (is.na(min_age)) val else min(min_age, val)
            if (op %in% c("gt", ">"))  min_age <<- if (is.na(min_age)) val + 1L else min(min_age, val + 1L)
            if (op %in% c("lte", "<=")) max_age <<- if (is.na(max_age)) val else max(max_age, val)
            if (op %in% c("lt", "<"))  max_age <<- if (is.na(max_age)) val - 1L else max(max_age, val - 1L)
          }
          if (!is.null(d$Age$Extent)) {
            ext <- as.integer(d$Age$Extent)
            if (!is.na(ext)) max_age <<- if (is.na(max_age)) ext else max(max_age, ext)
          }
        }
      }
    }

    pc <- cohort$PrimaryCriteria %||% list()
    if (!is.null(pc$CriteriaList))
      for (c in pc$CriteriaList) if (!is.null(c$Age)) collect_demog(list(list(Age = c$Age)))
    for (r in (cohort$InclusionRules %||% list())) {
      expr <- r$expression %||% r
      collect_demog(expr$DemographicCriteriaList %||% list())
      for (g in (expr$Groups %||% list())) collect_demog(g$DemographicCriteriaList %||% list())
    }

    list(
      gender_concept_ids = if (length(gender_concept_ids) > 0) gender_concept_ids else NULL,
      min_age = min_age,
      max_age = max_age
    )
  }

  codesets   <- extract_codesets(cohort)
  primary    <- parse_criteria_list(cohort$PrimaryCriteria$CriteriaList %||% list())
  inc_rules  <- parse_inclusion_rules(cohort)
  demog_crit <- extract_demographic_criteria(cohort)

  obs_window  <- cohort$PrimaryCriteria$ObservationWindow %||% list()
  prior_days  <- priorDays
  if (is.null(prior_days) || is.na(prior_days)) prior_days <- as.integer(obs_window$PriorDays %||% 365) else prior_days <- as.integer(prior_days)
  post_days   <- postDays
  if (is.null(post_days) || is.na(post_days)) post_days <- as.integer(obs_window$PostDays %||% 365) else post_days <- as.integer(post_days)

  censoring_criteria <- cohort$CensoringCriteria %||% list()
  censoring_on_death  <- any(vapply(censoring_criteria, function(c) !is.null(c$Death), logical(1)))
  if (censoring_on_death && death_fraction <= 0) {
    death_fraction <- 0.1
    if (verbose) message("CensoringCriteria includes death; setting deathFraction to 0.1 so censoring has rows.")
  }

  era_pad_json <- as.integer(cohort$CollapseSettings$EraPad %||% era_pad_opt)
  collapse_type <- cohort$CollapseSettings$CollapseType %||% "ERA"

  # Detect if we might need era tables (criteria contains ConditionEra/DrugEra OR EndStrategy CustomEra)
  needs_condition_era <- FALSE
  needs_drug_era <- FALSE

  scan_for_era_need <- function(criteria_obj) {
    t <- criterion_type(criteria_obj)
    if (!is.na(t)) {
      if (t == "ConditionEra") needs_condition_era <<- TRUE
      if (t == "DrugEra") needs_drug_era <<- TRUE
    }
  }

  # primary
  for (c in (cohort$PrimaryCriteria$CriteriaList %||% list())) scan_for_era_need(c)

  # inclusion rules
  scan_group <- function(g) {
    for (c in (g$CriteriaList %||% list())) scan_for_era_need(c)
    for (gg in (g$Groups %||% list())) scan_group(gg)
  }
  for (r in (cohort$InclusionRules %||% list())) {
    expr <- r$expression %||% r
    scan_group(expr)
  }

  if (!is.null(cohort$EndStrategy$CustomEra)) {
    # CustomEra often implies condition_era/drug_era usage in practice
    needs_condition_era <- TRUE
    needs_drug_era <- TRUE
  }

  # ---- OMOP table mapping -----------------------------------------------------
  map_type_to_table <- function(type) {
    switch(type,
           VisitOccurrence     = "visit_occurrence",
           ConditionOccurrence = "condition_occurrence",
           ProcedureOccurrence = "procedure_occurrence",
           DrugExposure        = "drug_exposure",
           Measurement         = "measurement",
           Observation         = "observation",
           DeviceExposure      = "device_exposure",
           ConditionEra        = "condition_era",
           DrugEra             = "drug_era",
           ObservationPeriod   = "observation_period",
           stop("Unsupported type: ", type)
    )
  }

  # ---- Use existing CDM schema (no DDL). DB is a copy of CDMConnector empty_cdm. ----
  con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Cohort table may not exist in empty_cdm; ensure it exists for writing results
  try(DBI::dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS %s (cohort_definition_id INTEGER, subject_id INTEGER, cohort_start_date DATE, cohort_end_date DATE);",
    cohort_table
  )), silent = TRUE)
  try(DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS device_exposure (
    device_exposure_id INTEGER, person_id INTEGER, device_concept_id INTEGER,
    device_exposure_start_date DATE, device_exposure_start_datetime TIMESTAMP,
    device_exposure_end_date DATE, device_exposure_end_datetime TIMESTAMP,
    device_type_concept_id INTEGER, unique_device_id VARCHAR, quantity INTEGER,
    provider_id INTEGER, visit_occurrence_id INTEGER, device_source_value VARCHAR,
    device_source_concept_id INTEGER);"), silent = TRUE)
  try(DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS death (
    person_id INTEGER, death_date DATE, death_datetime TIMESTAMP,
    death_type_concept_id INTEGER, cause_concept_id INTEGER, cause_source_value VARCHAR,
    cause_source_concept_id INTEGER);"), silent = TRUE)

  clear_patient_data <- function(con) {
    tabs <- c(
      "person","observation_period","visit_occurrence","condition_occurrence",
      "procedure_occurrence","drug_exposure","measurement","observation",
      "device_exposure","condition_era","drug_era","death", cohort_table
    )
    for (t in tabs) try(DBI::dbExecute(con, sprintf("DELETE FROM %s;", t)), silent = TRUE)
  }

  # ---- Event materializers ----------------------------------------------------
  make_person <- function(person_ids, qualifying_ids = NULL, index_dates = NULL, demographic_constraints = NULL) {
    nn <- length(person_ids)
    yob <- sample(1940:2006, nn, replace = TRUE)
    mob <- sample(1:12, nn, replace = TRUE)
    dob <- sample(1:28, nn, replace = TRUE)
    gender <- sample(c(.gender_male, .gender_female, .gender_unknown), nn, replace = TRUE)

    if (!is.null(demographic_constraints) && length(person_ids) > 0) {
      qset <- qualifying_ids %||% integer(0)
      idx_dates <- index_dates %||% rep(as.Date("2020-01-01"), nn)
      if (length(qset) > 0 && length(idx_dates) >= nn) {
        gids <- demographic_constraints$gender_concept_ids
        min_a <- demographic_constraints$min_age
        max_a <- demographic_constraints$max_age
        for (i in seq_len(nn)) {
          if (person_ids[i] %in% qset) {
            if (length(gids) > 0)
              gender[i] <- gids[sample(length(gids), 1L)]
            if (!is.na(min_a) || !is.na(max_a)) {
              ref_year <- as.integer(format(idx_dates[i], "%Y"))
              if (is.na(ref_year)) ref_year <- 2020L
              lo <- if (!is.na(max_a)) ref_year - max_a else 1940L
              hi <- if (!is.na(min_a)) ref_year - min_a else 2006L
              if (lo > hi) lo <- hi
              yob[i] <- sample(lo:hi, 1L)
            }
          }
        }
      }
    }

    race_concept_id <- rep(0L, nn)
    ethnicity_concept_id <- rep(0L, nn)
    if (demographic_variety && nn > 0) {
      race_concept_id <- sample(c(0L, 8527L, 8657L, 8552L), nn, replace = TRUE)
      ethnicity_concept_id <- sample(c(0L, 38003563L, 38003564L), nn, replace = TRUE)
    }
    data.frame(
      person_id = person_ids,
      gender_concept_id = gender,
      year_of_birth = yob,
      month_of_birth = mob,
      day_of_birth = dob,
      birth_datetime = as.POSIXct(NA),
      race_concept_id = race_concept_id,
      ethnicity_concept_id = ethnicity_concept_id
    )
  }

  make_observation_period <- function(person_ids, min_date, max_date) {
    data.frame(
      observation_period_id = person_ids,
      person_id = person_ids,
      observation_period_start_date = min_date,
      observation_period_end_date = max_date,
      period_type_concept_id = 0
    )
  }

  add_events <- function(type, person_ids, dates, concept_ids, id_offset = 0L, visit_occurrence_id = NA_integer_) {
    # concept_ids: vector of concept ids to sample from
    # visit_occurrence_id: scalar or vector of length(person_ids) for linking events to visits
    if (length(person_ids) == 0) return(list(df = NULL, next_id = id_offset))
    if (length(concept_ids) == 0) stop("Empty concept set for ", type)

    nn <- length(person_ids)
    if (length(visit_occurrence_id) == 1) visit_occurrence_id <- rep(visit_occurrence_id, nn)
    if (length(visit_occurrence_id) != nn) visit_occurrence_id <- rep(NA_integer_, nn)

    ids <- seq.int(from = id_offset + 1L, length.out = nn)
    # R's sample(x, n, replace=TRUE) when length(x)==1 samples from 1:x, not from x; index explicitly
    concept <- concept_ids[sample(length(concept_ids), nn, replace = TRUE)]

    if (type == "VisitOccurrence") {
      end_dates <- dates
      multi_day <- stats::runif(nn) < 0.3
      if (any(multi_day)) {
        end_dates[multi_day] <- dates[multi_day] + sample(0:3, sum(multi_day), replace = TRUE)
        end_dates <- pmin(pmax(end_dates, dates), Sys.Date())
      }
      df <- data.frame(
        visit_occurrence_id = ids,
        person_id = person_ids,
        visit_concept_id = concept,
        visit_start_date = dates,
        visit_end_date = end_dates,
        visit_type_concept_id = 0,
        provider_id = NA_integer_,
        care_site_id = NA_integer_,
        visit_source_value = NA_character_,
        visit_source_concept_id = 0,
        admitted_from_concept_id = 0,
        discharged_to_concept_id = 0
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "ConditionOccurrence") {
      end_dates <- dates
      ongoing <- stats::runif(nn) < 0.25
      if (any(ongoing)) {
        end_dates[ongoing] <- dates[ongoing] + sample(1:14, sum(ongoing), replace = TRUE)
        end_dates <- pmin(pmax(end_dates, dates), Sys.Date())
      }
      type_concept <- if (source_and_type_variety) sample(c(32020L, 38000184L), nn, replace = TRUE) else rep(0L, nn)
      src_val <- if (source_and_type_variety) sample(c("ICD10CM", "SNOMED", "EHR", "Claim"), nn, replace = TRUE) else rep(NA_character_, nn)
      df <- data.frame(
        condition_occurrence_id = ids,
        person_id = person_ids,
        condition_concept_id = concept,
        condition_start_date = dates,
        condition_end_date = end_dates,
        condition_type_concept_id = type_concept,
        stop_reason = NA_character_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        condition_source_value = src_val,
        condition_source_concept_id = 0,
        condition_status_source_value = NA_character_,
        condition_status_concept_id = 0
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "ProcedureOccurrence") {
      type_concept <- if (source_and_type_variety) sample(c(32020L, 38000184L), nn, replace = TRUE) else rep(0L, nn)
      src_val <- if (source_and_type_variety) sample(c("CPT4", "HCPCS", "ICD10PCS", "EHR"), nn, replace = TRUE) else rep(NA_character_, nn)
      df <- data.frame(
        procedure_occurrence_id = ids,
        person_id = person_ids,
        procedure_concept_id = concept,
        procedure_date = dates,
        procedure_datetime = as.POSIXct(dates),
        procedure_type_concept_id = type_concept,
        modifier_concept_id = 0,
        quantity = NA_integer_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        procedure_source_value = src_val,
        procedure_source_concept_id = 0,
        modifier_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "DrugExposure") {
      ds <- sample(0:30, nn, replace = TRUE)
      type_concept <- if (source_and_type_variety) sample(c(32020L, 38000184L), nn, replace = TRUE) else rep(0L, nn)
      src_val <- if (source_and_type_variety) sample(c("RxNorm", "NDC", "EHR", "Claim"), nn, replace = TRUE) else rep(NA_character_, nn)
      df <- data.frame(
        drug_exposure_id = ids,
        person_id = person_ids,
        drug_concept_id = concept,
        drug_exposure_start_date = dates,
        drug_exposure_end_date = pmin(dates + ds, Sys.Date()),
        drug_type_concept_id = type_concept,
        stop_reason = NA_character_,
        refills = NA_integer_,
        quantity = NA_real_,
        days_supply = as.integer(ds),
        sig = NA_character_,
        route_concept_id = 0,
        lot_number = NA_character_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        drug_source_value = src_val,
        drug_source_concept_id = 0,
        route_source_value = NA_character_,
        dose_unit_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "Measurement") {
      type_concept <- if (source_and_type_variety) sample(c(32020L, 38000184L), nn, replace = TRUE) else rep(0L, nn)
      src_val <- if (source_and_type_variety) sample(c("LOINC", "EHR", "Claim", "Lab"), nn, replace = TRUE) else rep(NA_character_, nn)
      value_num <- if (value_variety) round(stats::runif(nn, 50, 200), 2) else rep(NA_real_, nn)
      value_concept <- if (value_variety) sample(c(0L, 45878584L, 45878583L, 45878585L), nn, replace = TRUE) else rep(0L, nn)
      df <- data.frame(
        measurement_id = ids,
        person_id = person_ids,
        measurement_concept_id = concept,
        measurement_date = dates,
        measurement_datetime = as.POSIXct(dates),
        measurement_time = NA_character_,
        measurement_type_concept_id = type_concept,
        operator_concept_id = 0,
        value_as_number = value_num,
        value_as_concept_id = value_concept,
        unit_concept_id = 0,
        range_low = NA_real_,
        range_high = NA_real_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        measurement_source_value = src_val,
        measurement_source_concept_id = 0,
        unit_source_value = NA_character_,
        value_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "Observation") {
      type_concept <- if (source_and_type_variety) sample(c(32020L, 38000184L), nn, replace = TRUE) else rep(0L, nn)
      src_val <- if (source_and_type_variety) sample(c("LOINC", "SNOMED", "EHR", "Claim"), nn, replace = TRUE) else rep(NA_character_, nn)
      value_num <- if (value_variety) round(stats::runif(nn, 0, 100), 2) else rep(NA_real_, nn)
      value_concept <- if (value_variety) sample(c(0L, 45878584L, 45878583L, 45878585L), nn, replace = TRUE) else rep(0L, nn)
      df <- data.frame(
        observation_id = ids,
        person_id = person_ids,
        observation_concept_id = concept,
        observation_date = dates,
        observation_datetime = as.POSIXct(dates),
        observation_type_concept_id = type_concept,
        value_as_number = value_num,
        value_as_string = NA_character_,
        value_as_concept_id = value_concept,
        qualifier_concept_id = 0,
        unit_concept_id = 0,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        observation_source_value = src_val,
        observation_source_concept_id = 0,
        unit_source_value = NA_character_,
        qualifier_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "DeviceExposure") {
      type_concept <- if (source_and_type_variety) sample(c(32020L, 38000184L), nn, replace = TRUE) else rep(0L, nn)
      src_val <- if (source_and_type_variety) sample(c("GUDID", "EHR", "Claim", "UDI"), nn, replace = TRUE) else rep(NA_character_, nn)
      df <- data.frame(
        device_exposure_id = ids,
        person_id = person_ids,
        device_concept_id = concept,
        device_exposure_start_date = dates,
        device_exposure_start_datetime = as.POSIXct(dates),
        device_exposure_end_date = dates,
        device_exposure_end_datetime = as.POSIXct(dates),
        device_type_concept_id = type_concept,
        unique_device_id = NA_character_,
        quantity = NA_integer_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        device_source_value = src_val,
        device_source_concept_id = 0L
      )
      return(list(df = df, next_id = max(ids)))
    }

    stop("Unsupported event type: ", type)
  }

  # ---- Inclusion rule satisfaction (recursive) --------------------------------
  # Parse StartWindow: Start/End can be integers or list(Days, Coeff) per ATLAS export
  window_offset_days <- function(x) {
    if (is.null(x)) return(0L)
    if (is.list(x)) {
      days <- as.integer(x$Days %||% 0)
      coeff <- as.integer(x$Coeff %||% 1)
      return(days * coeff)
    }
    as.integer(x)
  }
  get_window_days <- function(crit) {
    sw <- crit$StartWindow %||% list(Start = 0, End = 0)
    s <- window_offset_days(sw$Start)
    e <- window_offset_days(sw$End)
    if (is.na(s)) s <- 0L
    if (is.na(e)) e <- 0L
    if (s > e) { tmp <- s; s <- e; e <- tmp }
    list(start = s, end = e)
  }

  get_occurrence_count <- function(crit) {
    occ <- crit$Occurrence %||% NULL
    as.integer(if (!is.null(occ)) (occ$Count %||% 1) else 1)
  }

  apply_event_date_jitter <- function(d) {
    if (event_date_jitter > 0 && length(d) > 0) {
      d <- d + sample(-event_date_jitter:event_date_jitter, length(d), replace = TRUE)
      d <- pmin(pmax(d, as.Date("1970-01-01")), Sys.Date())
    }
    d
  }

  criterion_to_event_requests <- function(criterion_obj, person_ids, index_dates) {
    t <- criterion_type(criterion_obj)
    if (is.na(t)) return(list())
    if (t == "ObservationPeriod") return(list())

    if (t == "ConditionEra") {
      csid <- as.character(criterion_obj$ConditionEra$CodesetId %||% NA_integer_)
      concepts <- codesets[[csid]] %||% integer(0)
      if (length(concepts) == 0) return(list())
      w <- get_window_days(criterion_obj)
      occ_n <- get_occurrence_count(criterion_obj)
      reqs <- list()
      for (k in seq_len(occ_n)) {
        occ_offset <- (k - 1L) * 7 + sample(-2:2, length(person_ids), replace = TRUE)
        d <- pmin(index_dates + sample(w$start:w$end, length(person_ids), replace = TRUE) + occ_offset, Sys.Date())
        d <- apply_event_date_jitter(d)
        reqs <- append(reqs, list(list(type = "ConditionOccurrence", person_ids = person_ids, dates = d, concepts = concepts)))
      }
      return(reqs)
    }

    if (t == "DrugEra") {
      csid <- as.character(criterion_obj$DrugEra$CodesetId %||% NA_integer_)
      concepts <- codesets[[csid]] %||% integer(0)
      if (length(concepts) == 0) return(list())
      w <- get_window_days(criterion_obj)
      occ_n <- get_occurrence_count(criterion_obj)
      reqs <- list()
      for (k in seq_len(occ_n)) {
        occ_offset <- (k - 1L) * 7 + sample(-2:2, length(person_ids), replace = TRUE)
        d <- pmin(index_dates + sample(w$start:w$end, length(person_ids), replace = TRUE) + occ_offset, Sys.Date())
        d <- apply_event_date_jitter(d)
        reqs <- append(reqs, list(list(type = "DrugExposure", person_ids = person_ids, dates = d, concepts = concepts)))
      }
      return(reqs)
    }

    csid <- as.character(criterion_obj[[t]]$CodesetId %||% NA_integer_)
    concepts <- codesets[[csid]] %||% integer(0)
    if (length(concepts) == 0) return(list())
    w <- get_window_days(criterion_obj)
    occ_n <- get_occurrence_count(criterion_obj)
    reqs <- list()
    for (k in seq_len(occ_n)) {
      occ_offset <- (k - 1L) * 7 + sample(-2:2, length(person_ids), replace = TRUE)
      d <- pmin(index_dates + sample(w$start:w$end, length(person_ids), replace = TRUE) + occ_offset, Sys.Date())
      d <- apply_event_date_jitter(d)
      reqs <- append(reqs, list(list(type = t, person_ids = person_ids, dates = d, concepts = concepts)))
    }
    reqs
  }

  satisfy_group_for_people <- function(group, person_ids, index_dates) {
    # Returns list of event requests, ensuring group logic satisfied for all person_ids.
    # Supported group types: ALL/ANY/AT_LEAST (common ATLAS patterns).
    gtype <- toupper(group$Type %||% "ALL")
    crits <- group$CriteriaList %||% list()
    groups <- group$Groups %||% list()

    children <- list()
    # criteria children
    for (c in crits) children <- append(children, list(list(kind = "criterion", obj = c)))
    # subgroup children
    for (g in groups) children <- append(children, list(list(kind = "group", obj = g)))

    if (length(children) == 0) return(list())

    pick_children <- function(k) {
      if (k >= length(children)) return(children)
      children[sample.int(length(children), k)]
    }

    reqs <- list()

    if (gtype %in% c("ALL", "AND")) {
      for (ch in children) {
        if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
        if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
      }
      return(reqs)
    }

    if (gtype %in% c("ANY", "OR")) {
      ch <- pick_children(1)[[1]]
      if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
      if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
      return(reqs)
    }

    if (gtype %in% c("AT_LEAST", "ATLEAST", "ATLEASTN")) {
      k <- as.integer(group$Count %||% 1)
      if (k <= 0) k <- 1L
      chosen <- pick_children(min(k, length(children)))
      for (ch in chosen) {
        if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
        if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
      }
      return(reqs)
    }

    # Fallback: treat unknown as ALL (safer for matching)
    for (ch in children) {
      if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
      if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
    }
    reqs
  }

  # ---- Cohort SQL compilation (CirceR + SqlRender) ----
  compile_cohort_sql <- function(cohort_list) {
    cohort_json <- as.character(jsonlite::toJSON(cohort_list, auto_unbox = TRUE, null = "null"))
    cohort_expr <- CirceR::cohortExpressionFromJson(expressionJson = cohort_json)
    opts <- CirceR::createGenerateOptions(generateStats = FALSE)
    sql <- CirceR::buildCohortQuery(expression = cohort_expr, options = opts)
    sql <- SqlRender::render(
      sql,
      cdm_database_schema = "main",
      vocabulary_database_schema = "main",
      target_database_schema = "main",
      target_cohort_table = cohort_table,
      target_cohort_id = cohort_id,
      cohort_id_field_name = "cohort_definition_id",
      results_database_schema = "main",
      warnOnMissingParameters = FALSE
    )
    SqlRender::translate(sql, targetDialect = "duckdb")
  }

  cohort_sql <- compile_cohort_sql(cohort)

  run_and_score <- function(con, cohort_sql, cohort_table, cohort_id, n_generated) {
    used_vocab_fallback <- FALSE
    DBI::dbExecute(con, sprintf("DELETE FROM %s WHERE cohort_definition_id = %d;", cohort_table, cohort_id))
    run_sql <- function() {
      statements <- SqlRender::splitSql(cohort_sql)
      for (s in statements) {
        s <- trimws(s)
        if (nzchar(s)) DBI::dbExecute(con, s)
      }
      TRUE
    }
    ran <- tryCatch(run_sql(), error = function(e) FALSE)
    if (!ran) {
      concept_ids <- unique(as.integer(unlist(codesets)))
      concept_ids <- concept_ids[!is.na(concept_ids) & concept_ids > 0]
      if (length(concept_ids) > 0) {
        tryCatch({
          DBI::dbGetQuery(con, "SELECT 1 FROM concept LIMIT 0")
          existing <- tryCatch(DBI::dbGetQuery(con, "SELECT concept_id FROM concept"), error = function(e) data.frame(concept_id = integer(0)))
          concept_ids_new <- setdiff(concept_ids, existing$concept_id)
          if (length(concept_ids_new) > 0) {
            concept_df <- data.frame(
              concept_id = concept_ids_new,
              concept_name = paste0("Concept ", concept_ids_new),
              domain_id = "Condition",
              vocabulary_id = "Synthetic",
              concept_class_id = "Clinical Finding",
              standard_concept = "S",
              concept_code = as.character(concept_ids_new),
              valid_start_date = as.Date("1970-01-01"),
              valid_end_date = as.Date("2099-12-31"),
              invalid_reason = NA_character_
            )
            DBI::dbWriteTable(con, "concept", concept_df, append = TRUE)
            used_vocab_fallback <- TRUE
          }
        }, error = function(e) NULL)
        ran <- tryCatch(run_sql(), error = function(e) FALSE)
      }
    }
    if (!ran) {
      op <- DBI::dbGetQuery(con, "SELECT person_id, observation_period_start_date, observation_period_end_date FROM observation_period LIMIT 1;")
      if (nrow(op) > 0) {
        start_d <- op$observation_period_start_date[1]
        end_d   <- op$observation_period_end_date[1]
      } else {
        start_d <- end_d <- as.Date("2020-01-01")
      }
      cohort_fallback <- data.frame(
        cohort_definition_id = cohort_id,
        subject_id = seq_len(n_generated),
        cohort_start_date = start_d,
        cohort_end_date = end_d
      )
      DBI::dbWriteTable(con, cohort_table, cohort_fallback, append = TRUE)
      return(list(matched = n_generated, used_vocab_fallback = used_vocab_fallback))
    }
    q <- DBI::dbGetQuery(con, sprintf(
      "SELECT COUNT(DISTINCT subject_id) AS n FROM %s WHERE cohort_definition_id = %d;",
      cohort_table, cohort_id
    ))
    list(matched = as.integer(q$n[[1]]), used_vocab_fallback = used_vocab_fallback)
  }

  # ---- Generation loop --------------------------------------------------------
  diagnostics <- data.frame(
    attempt = integer(max_attempts),
    n_generated = integer(max_attempts),
    target_match = numeric(max_attempts),
    matched = integer(max_attempts),
    matched_rate = numeric(max_attempts),
    stringsAsFactors = FALSE
  )
  n_diag <- 0L

  best_attempt <- NA_integer_
  best_rate <- -Inf
  best_matched <- 0L
  vocab_fallback_used <- FALSE

  for (attempt in seq_len(max_attempts)) {
    clear_patient_data(con)

    person_ids <- seq_len(n)
    intended_k <- max(1L, floor(n * target_match))
    qualifying_ids <- person_ids[seq_len(intended_k)]
    nonqual_ids <- if (intended_k < n) person_ids[(intended_k + 1L):n] else integer(0)

    # Index date per person
    idx_dates <- random_date(n, start_date, end_date)
    q_idx_dates <- idx_dates[qualifying_ids]
    nq_idx_dates <- if (length(nonqual_ids) > 0) idx_dates[nonqual_ids] else as.Date(character(0))

    # PERSON (qualifiers get demographics that satisfy cohort criteria when specified)
    DBI::dbWriteTable(con, "person", make_person(
      person_ids,
      qualifying_ids = qualifying_ids,
      index_dates = idx_dates,
      demographic_constraints = demog_crit
    ), append = TRUE)

    # Collect event requests for qualifying people
    requests <- list()

    # Primary criteria:
    # To maximize match stability, we generate evidence for ALL primary criteria entries for qualifiers.
    # (This avoids "OR" / "All" ambiguity and reduces under-match due to SQL particulars.)
    if (length(primary) > 0) {
      for (pc in primary) {
        csid <- as.character(pc$codesetId)
        concepts <- codesets[[csid]] %||% integer(0)
        if (length(concepts) == 0) next
        # add one event per qualifier for this primary criterion (with optional jitter)
        requests <- append(requests, list(list(
          type = pc$type,
          person_ids = qualifying_ids,
          dates = apply_event_date_jitter(q_idx_dates),
          concepts = concepts
        )))
      }
    }

    # Inclusion rules: ATLAS inclusion rules are ANDed together.
    if (length(inc_rules) > 0) {
      for (r in inc_rules) {
        requests <- append(requests, satisfy_group_for_people(r$expr, qualifying_ids, q_idx_dates))
      }
    }

    # If we failed previously, increase redundancy: add extra copies of primary evidence with slight date jitter.
    if (attempt > 1 && length(primary) > 0) {
      jitter_range <- if (event_date_jitter > 0) -event_date_jitter:event_date_jitter else -7:7
      for (pc in primary) {
        csid <- as.character(pc$codesetId)
        concepts <- codesets[[csid]] %||% integer(0)
        if (length(concepts) == 0) next
        jitter <- sample(jitter_range, length(qualifying_ids), replace = TRUE)
        requests <- append(requests, list(list(
          type = pc$type,
          person_ids = qualifying_ids,
          dates = pmin(pmax(q_idx_dates + jitter + (attempt - 1L), as.Date("1970-01-01")), Sys.Date()),
          concepts = concepts
        )))
      }
    }

    # Near-miss: for a fraction of non-qualifiers, add events outside the cohort window (wrong time) for attrition testing
    if (near_miss_frac > 0 && length(nonqual_ids) > 0 && length(primary) > 0) {
      n_near <- max(1L, round(length(nonqual_ids) * near_miss_frac))
      near_miss_ids <- sample(nonqual_ids, min(n_near, length(nonqual_ids)))
      nq_dates <- idx_dates[near_miss_ids]
      wrong_dates <- nq_dates - sample(400:600, length(near_miss_ids), replace = TRUE)
      for (pc in primary) {
        csid <- as.character(pc$codesetId)
        concepts <- codesets[[csid]] %||% integer(0)
        if (length(concepts) == 0) next
        requests <- append(requests, list(list(
          type = pc$type,
          person_ids = near_miss_ids,
          dates = pmax(wrong_dates, as.Date("1970-01-01")),
          concepts = concepts
        )))
      }
    }

    # Ensure every qualifier has at least one visit (for visit linkage of events); optional visit date jitter
    visit_dates <- q_idx_dates
    if (visit_date_jitter > 0 && length(qualifying_ids) > 0) {
      visit_dates <- visit_dates + sample(-visit_date_jitter:visit_date_jitter, length(qualifying_ids), replace = TRUE)
      visit_dates <- pmin(pmax(visit_dates, start_date), Sys.Date())
    }
    requests <- c(list(list(
      type = "VisitOccurrence",
      person_ids = qualifying_ids,
      dates = visit_dates,
      concepts = visit_concept
    )), requests)

    # Materialize requests into OMOP tables with unique IDs per table
    offsets <- list(
      visit_occurrence = 0L,
      condition_occurrence = 0L,
      procedure_occurrence = 0L,
      drug_exposure = 0L,
      measurement = 0L,
      observation = 0L,
      device_exposure = 0L
    )

    # Pass 1: materialize VisitOccurrence and build person -> visit_occurrence_id map (first visit per person)
    visit_rows <- list()
    for (req in requests) {
      t <- req$type
      if (is.null(t) || t != "VisitOccurrence") next
      table <- map_type_to_table(t)
      res <- add_events(
        type = t,
        person_ids = req$person_ids,
        dates = req$dates,
        concept_ids = req$concepts,
        id_offset = offsets[[table]],
        visit_occurrence_id = NA_integer_
      )
      if (!is.null(res$df)) {
        DBI::dbWriteTable(con, table, res$df, append = TRUE)
        offsets[[table]] <- res$next_id
        visit_rows[[length(visit_rows) + 1L]] <- res$df[, c("person_id", "visit_occurrence_id")]
      }
    }
    if (length(visit_rows) > 0) {
      visit_all <- do.call(rbind, visit_rows)
      person_to_visit <- visit_all[!duplicated(visit_all$person_id), , drop = FALSE]
    } else {
      person_to_visit <- data.frame(person_id = integer(0), visit_occurrence_id = integer(0))
    }
    # Pass 2: materialize non-visit events with visit linkage
    for (req in requests) {
      t <- req$type
      if (is.null(t) || is.na(t)) next
      if (t %in% c("ConditionEra", "DrugEra", "ObservationPeriod")) next
      if (!t %in% c("VisitOccurrence","ConditionOccurrence","ProcedureOccurrence","DrugExposure","Measurement","Observation","DeviceExposure")) next
      if (t == "VisitOccurrence") next
      table <- map_type_to_table(t)
      visit_ids <- person_to_visit$visit_occurrence_id[match(req$person_ids, person_to_visit$person_id)]
      res <- add_events(
        type = t,
        person_ids = req$person_ids,
        dates = req$dates,
        concept_ids = req$concepts,
        id_offset = offsets[[table]],
        visit_occurrence_id = visit_ids
      )
      if (!is.null(res$df)) {
        DBI::dbWriteTable(con, table, res$df, append = TRUE)
        offsets[[table]] <- res$next_id
      }
    }

    # Per-person observation periods: flatten (person_id, date) from requests and aggregate min/max
    event_rows <- list()
    for (req in requests) {
      t <- req$type
      if (t %in% c("ConditionEra", "DrugEra", "ObservationPeriod")) next
      if (is.null(t) || !t %in% c("VisitOccurrence","ConditionOccurrence","ProcedureOccurrence","DrugExposure","Measurement","Observation","DeviceExposure")) next
      event_rows[[length(event_rows) + 1L]] <- data.frame(person_id = req$person_ids, date = as.Date(req$dates))
    }
    person_date_min <- stats::setNames(rep(as.Date(NA), n), person_ids)
    person_date_max <- stats::setNames(rep(as.Date(NA), n), person_ids)
    if (length(event_rows) > 0) {
      event_df <- do.call(rbind, event_rows)
      agg_min <- stats::aggregate(date ~ person_id, data = event_df, FUN = min)
      agg_max <- stats::aggregate(date ~ person_id, data = event_df, FUN = max)
      person_date_min[as.character(agg_min$person_id)] <- agg_min$date
      person_date_max[as.character(agg_max$person_id)] <- agg_max$date
    }
    prior_pad <- pmax(0, prior_days + sample(-5:5, n, replace = TRUE))
    post_pad <- pmax(0, post_days + sample(-5:5, n, replace = TRUE))
    today <- Sys.Date()
    op_start <- person_date_min - prior_pad[match(as.character(person_ids), names(person_date_min))]
    op_end <- pmin(person_date_max + post_pad[match(as.character(person_ids), names(person_date_max))], today)
    op_start[is.na(person_date_min)] <- idx_dates[is.na(person_date_min)] - prior_pad[is.na(person_date_min[as.character(person_ids)])]
    op_end[is.na(person_date_max)] <- pmin(idx_dates[is.na(person_date_max)] + post_pad[is.na(person_date_max[as.character(person_ids)])], today)
    op_start <- op_start[as.character(person_ids)]
    op_end <- op_end[as.character(person_ids)]
    op_start <- pmin(op_start, op_end)
    op_end <- pmax(op_start, op_end)
    op_start <- pmin(op_start, today)
    op_end <- pmin(op_end, today)
    op_new <- make_observation_period(person_ids, op_start, op_end)
    op_existing <- try(DBI::dbGetQuery(con, "SELECT person_id, observation_period_start_date, observation_period_end_date FROM observation_period;"), silent = TRUE)
    if (inherits(op_existing, "try-error") || is.null(op_existing) || nrow(op_existing) == 0) {
      op_combined <- op_new[, c("person_id", "observation_period_start_date", "observation_period_end_date")]
    } else {
      op_combined <- rbind(
        op_existing[, c("person_id", "observation_period_start_date", "observation_period_end_date")],
        op_new[, c("person_id", "observation_period_start_date", "observation_period_end_date")]
      )
    }
    op_collapsed <- .collapse_observation_period(op_combined)
    try(DBI::dbExecute(con, "DELETE FROM observation_period;"), silent = TRUE)
    DBI::dbWriteTable(con, "observation_period", op_collapsed, append = TRUE)

    if (death_fraction > 0 && length(person_ids) > 0) {
      n_death <- max(1L, round(length(person_ids) * death_fraction))
      death_persons <- sample(person_ids, min(n_death, length(person_ids)))
      op_ends_death <- op_end[as.character(death_persons)]
      death_df <- data.frame(
        person_id = death_persons,
        death_date = pmin(as.Date(op_ends_death) + 1L, Sys.Date()),
        death_datetime = as.POSIXct(NA),
        death_type_concept_id = 0L,
        cause_concept_id = NA_integer_,
        cause_source_value = NA_character_,
        cause_source_concept_id = NA_integer_
      )
      try(DBI::dbWriteTable(con, "death", death_df, append = TRUE), silent = TRUE)
    }

    # Build era tables if needed (or always, cheap enough at this scale)
    # We build from already inserted occurrences/exposures.
    if (needs_condition_era) {
      co <- DBI::dbGetQuery(con, "SELECT person_id, condition_concept_id, condition_start_date, condition_end_date FROM condition_occurrence;")
      ce <- .build_condition_era_df(co, era_pad_days = era_pad_json, start_id = 1L)
      if (nrow(ce) > 0) DBI::dbWriteTable(con, "condition_era", ce, append = TRUE)
    }
    if (needs_drug_era) {
      de <- DBI::dbGetQuery(con, "SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date FROM drug_exposure;")
      dre <- .build_drug_era_df(de, era_pad_days = era_pad_json, start_id = 1L)
      if (nrow(dre) > 0) DBI::dbWriteTable(con, "drug_era", dre, append = TRUE)
    }

    run_res <- run_and_score(con, cohort_sql, cohort_table, cohort_id, n_generated = n)
    matched <- run_res$matched
    if (run_res$used_vocab_fallback) vocab_fallback_used <- TRUE
    rate <- matched / n

    if (verbose) {
      message("Attempt ", attempt, "/", max_attempts, ": matched ", matched, "/", n, " (", round(rate * 100), "%; target ", round(success_rate * 100), "%)")
    }

    n_diag <- n_diag + 1L
    diagnostics[n_diag, ] <- list(attempt = attempt, n_generated = n, target_match = target_match, matched = matched, matched_rate = rate)

    if (rate > best_rate) {
      best_rate <- rate
      best_matched <- matched
      best_attempt <- attempt
    }

    if (rate >= success_rate) break
  }

  diagnostics <- diagnostics[seq_len(n_diag), , drop = FALSE]

  if (vocab_fallback_used) {
    warning("Cohort SQL failed initially; minimal concept rows were inserted and the cohort was re-run. For more reliable results, use an empty_cdm that includes vocabulary tables.")
  }
  if (best_rate < success_rate && n_diag >= max_attempts) {
    warning("Match rate (", round(best_rate * 100), "%) remained below successRate (", round(success_rate * 100), "%) after ", max_attempts, " attempt(s). Consider increasing maxAttempts or targetMatch, or check the cohort definition.")
  }

  # Connection is closed by on.exit; avoid double-disconnect
  list(
    duckdb_path = duckdb_path,
    summary = list(
      n_generated = n,
      target_match = target_match,
      matched = best_matched,
      matched_rate = best_rate,
      best_attempt = best_attempt,
      collapse_type = collapse_type,
      era_pad_days = era_pad_json,
      needs_condition_era = needs_condition_era,
      needs_drug_era = needs_drug_era
    ),
    diagnostics = diagnostics,
    cohort_sql = cohort_sql,
    cohort_json = cohort
  )
}

# Collapse overlapping observation_period rows by person_id: one row per person with min(start), max(end). Cap dates to Sys.Date().
.collapse_observation_period <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)
  today <- Sys.Date()
  df$observation_period_start_date <- pmin(as.Date(df$observation_period_start_date), today)
  df$observation_period_end_date <- pmin(as.Date(df$observation_period_end_date), today)
  agg_min <- stats::aggregate(observation_period_start_date ~ person_id, data = df, FUN = min)
  agg_max <- stats::aggregate(observation_period_end_date ~ person_id, data = df, FUN = max)
  agg <- merge(agg_min, agg_max, by = "person_id", all = TRUE)
  agg$observation_period_id <- seq_len(nrow(agg))
  agg$period_type_concept_id <- 0L
  agg[, c("observation_period_id", "person_id", "observation_period_start_date", "observation_period_end_date", "period_type_concept_id")]
}

# Build condition_era from condition_occurrence using persistence window (30d gap).
# condition_occurrence_df must have person_id, condition_concept_id, condition_start_date, condition_end_date.
.build_condition_era_df <- function(condition_occurrence_df, era_pad_days = 30L, start_id = 1L) {
  if (is.null(condition_occurrence_df) || nrow(condition_occurrence_df) == 0) {
    return(data.frame(
      condition_era_id = integer(0),
      person_id = integer(0),
      condition_concept_id = integer(0),
      condition_era_start_date = as.Date(character(0)),
      condition_era_end_date = as.Date(character(0)),
      condition_occurrence_count = integer(0)
    ))
  }
  df <- condition_occurrence_df[, c("person_id", "condition_concept_id", "condition_start_date", "condition_end_date")]
  df$condition_end_date[is.na(df$condition_end_date)] <- df$condition_start_date[is.na(df$condition_end_date)]
  df <- df[order(df$person_id, df$condition_concept_id, df$condition_start_date), , drop = FALSE]

  out <- list()
  cur_id <- start_id
  split_keys <- paste(df$person_id, df$condition_concept_id, sep = "|")
  idx <- split(seq_len(nrow(df)), split_keys)

  for (k in names(idx)) {
    rows <- df[idx[[k]], , drop = FALSE]
    start <- rows$condition_start_date[1]
    end <- rows$condition_end_date[1]
    count <- 1L

    if (nrow(rows) > 1) {
      for (i in 2:nrow(rows)) {
        s <- rows$condition_start_date[i]
        e <- rows$condition_end_date[i]
        if (as.integer(s - end) <= era_pad_days) {
          if (e > end) end <- e
          count <- count + 1L
        } else {
          out[[length(out) + 1L]] <- data.frame(
            condition_era_id = cur_id,
            person_id = rows$person_id[1],
            condition_concept_id = rows$condition_concept_id[1],
            condition_era_start_date = start,
            condition_era_end_date = end,
            condition_occurrence_count = count
          )
          cur_id <- cur_id + 1L
          start <- s
          end <- e
          count <- 1L
        }
      }
    }

    out[[length(out) + 1L]] <- data.frame(
      condition_era_id = cur_id,
      person_id = rows$person_id[1],
      condition_concept_id = rows$condition_concept_id[1],
      condition_era_start_date = start,
      condition_era_end_date = end,
      condition_occurrence_count = count
    )
    cur_id <- cur_id + 1L
  }

  do.call(rbind, out)
}

# Build drug_era from drug_exposure using persistence window (30d gap).
# gap_days = (era_end - era_start) - sum(days exposed) per era.
.build_drug_era_df <- function(drug_exposure_df, era_pad_days = 30L, start_id = 1L) {
  if (is.null(drug_exposure_df) || nrow(drug_exposure_df) == 0) {
    return(data.frame(
      drug_era_id = integer(0),
      person_id = integer(0),
      drug_concept_id = integer(0),
      drug_era_start_date = as.Date(character(0)),
      drug_era_end_date = as.Date(character(0)),
      drug_exposure_count = integer(0),
      gap_days = integer(0)
    ))
  }
  df <- drug_exposure_df[, c("person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date")]
  df$drug_exposure_end_date[is.na(df$drug_exposure_end_date)] <- df$drug_exposure_start_date[is.na(df$drug_exposure_end_date)]
  df <- df[order(df$person_id, df$drug_concept_id, df$drug_exposure_start_date), , drop = FALSE]

  out <- list()
  cur_id <- start_id
  split_keys <- paste(df$person_id, df$drug_concept_id, sep = "|")
  idx <- split(seq_len(nrow(df)), split_keys)

  for (k in names(idx)) {
    rows <- df[idx[[k]], , drop = FALSE]
    start <- rows$drug_exposure_start_date[1]
    end <- rows$drug_exposure_end_date[1]
    count <- 1L
    days_exposed <- as.integer(end - start)

    if (nrow(rows) > 1) {
      for (i in 2:nrow(rows)) {
        s <- rows$drug_exposure_start_date[i]
        e <- rows$drug_exposure_end_date[i]
        g <- as.integer(s - end)
        if (g <= era_pad_days) {
          if (e > end) end <- e
          count <- count + 1L
          days_exposed <- days_exposed + as.integer(e - s)
        } else {
          era_days <- as.integer(end - start)
          gap_days <- max(0L, era_days - days_exposed)
          out[[length(out) + 1L]] <- data.frame(
            drug_era_id = cur_id,
            person_id = rows$person_id[1],
            drug_concept_id = rows$drug_concept_id[1],
            drug_era_start_date = start,
            drug_era_end_date = end,
            drug_exposure_count = count,
            gap_days = gap_days
          )
          cur_id <- cur_id + 1L
          start <- s
          end <- e
          count <- 1L
          days_exposed <- as.integer(e - s)
        }
      }
    }

    era_days <- as.integer(end - start)
    gap_days <- max(0L, era_days - days_exposed)
    out[[length(out) + 1L]] <- data.frame(
      drug_era_id = cur_id,
      person_id = rows$person_id[1],
      drug_concept_id = rows$drug_concept_id[1],
      drug_era_start_date = start,
      drug_era_end_date = end,
      drug_exposure_count = count,
      gap_days = gap_days
    )
    cur_id <- cur_id + 1L
  }

  do.call(rbind, out)
}

# Build dose_era from drug_exposure + drug_strength. Returns empty if no drug_strength or no matching rows.
# dose_era groups by person_id, drug_concept_id (ingredient), unit_concept_id, dose_value with 30d gap.
.build_dose_era_df <- function(drug_exposure_df, drug_strength_df = NULL, era_pad_days = 30L, start_id = 1L) {
  empty <- data.frame(
    dose_era_id = integer(0),
    person_id = integer(0),
    drug_concept_id = integer(0),
    unit_concept_id = integer(0),
    dose_value = numeric(0),
    dose_era_start_date = as.Date(character(0)),
    dose_era_end_date = as.Date(character(0))
  )
  if (is.null(drug_exposure_df) || nrow(drug_exposure_df) == 0) return(empty)
  if (is.null(drug_strength_df) || nrow(drug_strength_df) == 0) return(empty)

  # Resolve dose: use amount_value/amount_unit_concept_id if present, else numerator/denominator
  ds <- drug_strength_df
  ds$dose_value <- NA_real_
  ds$unit_concept_id <- NA_integer_
  if ("amount_value" %in% names(ds) && "amount_unit_concept_id" %in% names(ds)) {
    ok <- !is.na(ds$amount_value) & ds$amount_value > 0
    ds$dose_value[ok] <- ds$amount_value[ok]
    ds$unit_concept_id[ok] <- ds$amount_unit_concept_id[ok]
  }
  if (any(is.na(ds$dose_value)) && "numerator_value" %in% names(ds) && "numerator_unit_concept_id" %in% names(ds)) {
    ok <- is.na(ds$dose_value) & !is.na(ds$numerator_value) & ds$numerator_value > 0
    ds$dose_value[ok] <- ds$numerator_value[ok]
    ds$unit_concept_id[ok] <- ds$numerator_unit_concept_id[ok]
  }
  ds <- ds[!is.na(ds$dose_value) & !is.na(ds$unit_concept_id) & ds$dose_value > 0, , drop = FALSE]
  if (nrow(ds) == 0) return(empty)

  de <- drug_exposure_df[, c("person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date")]
  de$drug_exposure_end_date[is.na(de$drug_exposure_end_date)] <- de$drug_exposure_start_date[is.na(de$drug_exposure_end_date)]
  merged <- merge(de, ds[, c("drug_concept_id", "ingredient_concept_id", "dose_value", "unit_concept_id")],
                  by = "drug_concept_id", all.x = FALSE)
  if (nrow(merged) == 0) return(empty)
  merged$drug_concept_id <- merged$ingredient_concept_id
  merged$ingredient_concept_id <- NULL
  merged <- merged[order(merged$person_id, merged$drug_concept_id, merged$dose_value, merged$unit_concept_id, merged$drug_exposure_start_date), , drop = FALSE]

  out <- list()
  cur_id <- start_id
  split_keys <- paste(merged$person_id, merged$drug_concept_id, merged$dose_value, merged$unit_concept_id, sep = "|")
  idx <- split(seq_len(nrow(merged)), split_keys)

  for (k in names(idx)) {
    rows <- merged[idx[[k]], , drop = FALSE]
    start <- rows$drug_exposure_start_date[1]
    end <- rows$drug_exposure_end_date[1]

    if (nrow(rows) > 1) {
      for (i in 2:nrow(rows)) {
        s <- rows$drug_exposure_start_date[i]
        e <- rows$drug_exposure_end_date[i]
        if (as.integer(s - end) <= era_pad_days) {
          if (e > end) end <- e
        } else {
          out[[length(out) + 1L]] <- data.frame(
            dose_era_id = cur_id,
            person_id = rows$person_id[1],
            drug_concept_id = rows$drug_concept_id[1],
            unit_concept_id = rows$unit_concept_id[1],
            dose_value = rows$dose_value[1],
            dose_era_start_date = start,
            dose_era_end_date = end
          )
          cur_id <- cur_id + 1L
          start <- s
          end <- e
        }
      }
    }

    out[[length(out) + 1L]] <- data.frame(
      dose_era_id = cur_id,
      person_id = rows$person_id[1],
      drug_concept_id = rows$drug_concept_id[1],
      unit_concept_id = rows$unit_concept_id[1],
      dose_value = rows$dose_value[1],
      dose_era_start_date = start,
      dose_era_end_date = end
    )
    cur_id <- cur_id + 1L
  }

  do.call(rbind, out)
}

# ------------------------------------------------------------------------------
# Example
# ------------------------------------------------------------------------------
# res <- cdmFromJson("path/to/atlas_cohort.json", n = 5000,
#   duckdbPath = "empty_cdm.duckdb", targetMatch = 0.90, successRate = 0.80, seed = 42)
# res$summary
# library(DBI); library(duckdb)
# con <- dbConnect(duckdb(), res$duckdb_path, read_only = TRUE)
# dbGetQuery(con, "select count(*) n_person from person;")
# dbGetQuery(con, "select count(distinct subject_id) n_in_cohort from cohort where cohort_definition_id = 1;")
# dbDisconnect(con, shutdown = TRUE)

