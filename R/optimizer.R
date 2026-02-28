# optimizer.R
# Batch cohort SQL generation using execution DAG.
# Uses build_execution_dag + emit_dag_sql from execution_graph.R for optimized batch SQL.
# Pure base R, SqlRender-friendly (@cdm_database_schema, @results_schema).

# ---- Domain configuration (single source of truth) ----
# Drives: filtered table creation, rewrite_to_domain_caches, event cache rendering.
# `columns` lists the maximal set of columns referenced by any criteria builder
# in circe.R or by CustomEra in execution_graph.R.  NOT currently used for
# column pruning — benchmarking showed explicit column lists cause a 1.8x regression
# on DuckDB (the columnar engine already reads only needed columns; explicit lists
# add overhead on the write path).  Kept as documentation for potential future use
# on row-store server DBs (PostgreSQL, SQL Server) where pruning may help.
DOMAIN_CONFIG <- list(
  list(table = "DRUG_EXPOSURE",        alias = "de", std_col = "drug_concept_id",        src_col = "drug_source_concept_id",        filtered = "drug_exposure_filtered",        ix_prefix = "IX_DE_F",
       columns = c("person_id", "drug_exposure_id", "drug_concept_id", "drug_source_concept_id",
                    "drug_exposure_start_date", "drug_exposure_end_date",
                    "drug_type_concept_id", "route_concept_id",
                    "days_supply", "quantity", "refills",
                    "provider_id", "visit_occurrence_id")),
  list(table = "CONDITION_OCCURRENCE",  alias = "co", std_col = "condition_concept_id",   src_col = "condition_source_concept_id",   filtered = "condition_occurrence_filtered",  ix_prefix = "IX_CO_F",
       columns = c("person_id", "condition_occurrence_id", "condition_concept_id", "condition_source_concept_id",
                    "condition_start_date", "condition_end_date",
                    "condition_type_concept_id", "stop_reason",
                    "provider_id", "visit_occurrence_id")),
  list(table = "PROCEDURE_OCCURRENCE",  alias = "po", std_col = "procedure_concept_id",   src_col = "procedure_source_concept_id",   filtered = "procedure_occurrence_filtered",  ix_prefix = "IX_PO_F",
       columns = c("person_id", "procedure_occurrence_id", "procedure_concept_id", "procedure_source_concept_id",
                    "procedure_date",
                    "procedure_type_concept_id", "modifier_concept_id",
                    "quantity", "provider_id", "visit_occurrence_id")),
  list(table = "OBSERVATION",           alias = "o",  std_col = "observation_concept_id",  src_col = "observation_source_concept_id",  filtered = "observation_filtered",           ix_prefix = "IX_OBS_F",
       columns = c("person_id", "observation_id", "observation_concept_id", "observation_source_concept_id",
                    "observation_date",
                    "value_as_number", "value_as_concept_id", "value_as_string",
                    "unit_concept_id", "provider_id", "visit_occurrence_id")),
  list(table = "MEASUREMENT",           alias = "m",  std_col = "measurement_concept_id",  src_col = "measurement_source_concept_id",  filtered = "measurement_filtered",           ix_prefix = "IX_MEAS_F",
       columns = c("person_id", "measurement_id", "measurement_concept_id", "measurement_source_concept_id",
                    "measurement_date",
                    "measurement_type_concept_id", "operator_concept_id",
                    "value_as_number", "value_as_concept_id",
                    "range_high", "range_low",
                    "unit_concept_id", "provider_id", "visit_occurrence_id")),
  list(table = "DEVICE_EXPOSURE",       alias = "d",  std_col = "device_concept_id",       src_col = "device_source_concept_id",       filtered = "device_exposure_filtered",       ix_prefix = "IX_DEV_F",
       columns = c("person_id", "device_exposure_id", "device_concept_id", "device_source_concept_id",
                    "device_exposure_start_date", "device_exposure_end_date",
                    "device_type_concept_id", "quantity",
                    "provider_id", "visit_occurrence_id")),
  list(table = "VISIT_OCCURRENCE",      alias = "v",  std_col = "visit_concept_id",        src_col = "visit_source_concept_id",        filtered = "visit_occurrence_filtered",      ix_prefix = "IX_VIS_F",
       columns = c("person_id", "visit_occurrence_id", "visit_concept_id", "visit_source_concept_id",
                    "visit_start_date", "visit_end_date",
                    "visit_type_concept_id", "provider_id"))
)

#' Rewrite @cdm_database_schema.<domain> to global filtered cache tables.
#' When options is provided, table names are qualified via qualify_table().
#' Uses stringi C-backed regex replacement for speed.
#' @noRd
rewrite_to_domain_caches <- function(sql, cdm_schema = "@cdm_database_schema", options = NULL) {
  schema_esc <- gsub("([.*+?^${}()|[\\]\\\\])", "\\\\\\1", cdm_schema, perl = TRUE)
  for (dc in DOMAIN_CONFIG) {
    replacement <- if (!is.null(options)) qualify_table(dc$filtered, options) else dc$filtered
    sql <- stringi::stri_replace_all_regex(sql, paste0("(?i)\\b", schema_esc, "\\.", dc$table, "\\b"), replacement)
  }
  # OBSERVATION_PERIOD is special (no concept filtering)
  op_replacement <- if (!is.null(options)) qualify_table("atlas_observation_period", options) else "atlas_observation_period"
  sql <- stringi::stri_replace_all_regex(sql, paste0("(?i)\\b", schema_esc, "\\.OBSERVATION_PERIOD\\b"), op_replacement)
  sql
}

# atlasCohortGenerator: single workflow JSON -> optimized SQL (standalone)

# CDM/vocabulary table names that may appear in cohort SQL (uppercase as in generated SQL)
CDM_TABLE_NAMES_FOR_QUOTE <- c(
  "OBSERVATION_PERIOD", "PERSON", "CONCEPT", "CONCEPT_ANCESTOR",
  "DRUG_EXPOSURE", "CONDITION_OCCURRENCE", "PROCEDURE_OCCURRENCE",
  "OBSERVATION", "MEASUREMENT", "DEVICE_EXPOSURE", "VISIT_OCCURRENCE",
  "DEATH", "CONDITION_ERA", "DRUG_ERA", "DOSE_ERA", "SPECIMEN",
  "VISIT_DETAIL", "PAYER_PLAN_PERIOD", "LOCATION_HISTORY", "LOCATION",
  "PROVIDER"
)

#' Resolve SqlRender-style {N != 0}?{then} conditionals with literal numbers.
#' @noRd
resolve_literal_conditionals <- function(sql) {
  # Find { cond }?{ then } or { cond }?{ then }:{ else } and expand based on literal condition
  repeat {
    start <- regexpr("\\{\\s*[0-9]+\\s*!=\\s*0", sql, perl = TRUE)
    if (start[1L] == -1L) break
    cond_start <- start[1L]
    # Find end of condition: }?{ (must appear after cond_start)
    rest <- substr(sql, cond_start, nchar(sql))
    cond_then_sep <- regexpr("\\}\\s*\\?\\s*\\{", rest, perl = TRUE)
    if (cond_then_sep[1L] == -1L) break
    cond_end <- cond_start + cond_then_sep[1L] - 1L
    then_start <- cond_end + attr(cond_then_sep, "match.length")
    # Condition is between cond_start+1 and cond_end-1 (e.g. "23 != 0" or "1 != 0 & 5 != 0")
    cond_str <- substr(sql, cond_start + 1L, cond_end - 1L)
    cond_str <- trimws(cond_str)
    # Evaluate: true iff every "N != 0" part has N != 0; extract only N (number before "!= 0"), not the literal 0
    nums <- as.integer(regmatches(cond_str, gregexpr("[0-9]+(?=\\s*!=\\s*0)", cond_str, perl = TRUE))[[1]])
    cond_true <- length(nums) > 0L && all(nums != 0L)
    # Find matching } for the "then" block (starts at then_start)
    depth <- 1L
    pos <- then_start
    nch <- nchar(sql)
    then_end <- NA_integer_
    while (pos <= nch) {
      c <- substr(sql, pos, pos)
      if (c == "{") depth <- depth + 1L
      else if (c == "}") {
        depth <- depth - 1L
        if (depth == 0L) { then_end <- pos; break }
      }
      pos <- pos + 1L
    }
    if (is.na(then_end)) break
    then_content <- substr(sql, then_start, then_end - 1L)
    # Check for :{ else } part
    rest_start <- then_end + 1L
    rest <- substr(sql, rest_start, nch)
    if (substr(rest, 1L, 2L) == ":{") {
      else_start <- rest_start + 2L
      depth <- 1L
      pos <- else_start
      else_end <- NA_integer_
      while (pos <= nch) {
        c <- substr(sql, pos, pos)
        if (c == "{") depth <- depth + 1L
        else if (c == "}") {
          depth <- depth - 1L
          if (depth == 0L) { else_end <- pos; break }
        }
        pos <- pos + 1L
      }
      if (!is.na(else_end)) {
        else_content <- substr(sql, else_start, else_end - 1L)
        replacement <- if (cond_true) then_content else else_content
        sql <- paste0(substr(sql, 1L, cond_start - 1L), replacement, substr(sql, else_end + 1L, nch))
      } else {
        replacement <- if (cond_true) then_content else ""
        sql <- paste0(substr(sql, 1L, cond_start - 1L), replacement, substr(sql, then_end + 1L, nch))
      }
    } else {
      replacement <- if (cond_true) then_content else ""
      sql <- paste0(substr(sql, 1L, cond_start - 1L), replacement, substr(sql, then_end + 1L, nch))
    }
  }
  sql
}

#' Replace schema.TABLE with quoted lowercase refs (e.g. "main"."observation_period").
#' Uses stringi C-backed regex replacement for speed on large strings.
#' Handles multi-part schema names (e.g. "CDMV5.dbo") by quoting each part individually.
#' @noRd
quote_cdm_table_refs <- function(sql, con, cdm_schema_str) {
  schema_esc <- gsub(".", "\\.", cdm_schema_str, fixed = TRUE)
  # Quote each schema part individually (e.g. "CDMV5.dbo" -> "CDMV5"."dbo")
  schema_parts <- strsplit(cdm_schema_str, ".", fixed = TRUE)[[1]]
  quoted_schema <- paste(vapply(schema_parts, function(p) {
    as.character(DBI::dbQuoteIdentifier(con, p))
  }, character(1)), collapse = ".")
  # PostgreSQL: quoted identifiers are case-sensitive and CDM tables are stored
  # as lowercase. Snowflake: stored as uppercase. DuckDB/SQL Server: insensitive.
  db <- dbms(con)
  is_pg <- db %in% c("postgresql", "postgres")
  for (table_upper in CDM_TABLE_NAMES_FOR_QUOTE) {
    table_name <- if (is_pg) tolower(table_upper) else table_upper
    ref <- paste0(quoted_schema, ".", DBI::dbQuoteIdentifier(con, table_name))
    pattern <- paste0("(?i)\\b", schema_esc, "\\.", table_upper, "\\b")
    sql <- stringi::stri_replace_all_regex(sql, pattern, ref)
  }
  sql
}

# Known static working table suffixes for the DAG path (for cleanup fallback).
# DAG also creates dynamic tables (pe_xxx, qe_xxx, etc.) that are covered by
# prefix-based pattern matching in drop_prefixed_tables.
OPTIMIZER_STATIC_TABLES <- c(
  "cohort_stage", "inclusion_events_stage", "inclusion_stats_stage",
  "all_concepts", "codesets",
  "atlas_observation_period",
  "drug_exposure_filtered", "condition_occurrence_filtered",
  "procedure_occurrence_filtered", "observation_filtered", "measurement_filtered",
  "device_exposure_filtered", "visit_occurrence_filtered"
)

#' Generate a unique prefix for optimizer working tables (GUID or timestamp-based).
#' @return Character string like \code{"atlas_a1b2c3d4_"}.
#' @noRd
atlas_unique_prefix <- function() {
  if (requireNamespace("uuid", quietly = TRUE))
    paste0("atlas_", substr(gsub("-", "", uuid::UUIDgenerate(), fixed = TRUE), 1, 8), "_")
  else
    paste0("atlas_", format(Sys.time(), "%Y%m%d%H%M%S"), "_", paste(sample(c(letters, 0L:9L), 6L), collapse = ""), "_")
}

#' Drop all optimizer working tables with a given prefix.
#' Lists tables in the schema matching the prefix pattern, then drops them all.
#' Falls back to static list if table listing fails.
#' @noRd
drop_prefixed_tables <- function(con, results_schema_str, prefix) {
  qs <- DBI::dbQuoteIdentifier(con, results_schema_str)
  # Try to list all tables matching the prefix
  all_tables <- tryCatch({
    tbls <- DBI::dbListTables(con)
    tbls[startsWith(tolower(tbls), tolower(prefix))]
  }, error = function(e) NULL)

  if (!is.null(all_tables) && length(all_tables) > 0) {
    invisible(lapply(all_tables, function(name) {
      q <- paste0(qs, ".", DBI::dbQuoteIdentifier(con, name))
      # Try DROP TABLE first, then DROP VIEW (VIEWs used for single-consumer nodes)
      tryCatch(DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", q)), error = function(e) NULL)
      tryCatch(DBI::dbExecute(con, paste0("DROP VIEW IF EXISTS ", q)), error = function(e) NULL)
    }))
  } else {
    # Fallback: drop known static tables
    invisible(lapply(OPTIMIZER_STATIC_TABLES, function(name) {
      q <- paste0(qs, ".", DBI::dbQuoteIdentifier(con, paste0(prefix, name)))
      tryCatch(DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", q)), error = function(e) NULL)
      tryCatch(DBI::dbExecute(con, paste0("DROP VIEW IF EXISTS ", q)), error = function(e) NULL)
    }))
  }
}

read_json_input <- function(json) {
  if (length(json) != 1 || !is.character(json))
    stop("json must be a single character string (path or JSON content).")
  if (file.exists(json))
    paste(readLines(json, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  else
    json
}

#' Single cohort: Atlas JSON to optimized SQL
#'
#' Translates a single Atlas cohort definition (JSON file or string) into
#' parameterized or rendered SQL, optionally for a target dialect.
#'
#' @param json Path to a cohort JSON file or a character string containing the JSON.
#' @param cohort_id Cohort definition ID used in the generated SQL (default 1L).
#' @param cdm_schema CDM schema name (default \code{"main"} if \code{render = TRUE}).
#' @param vocabulary_schema Vocabulary schema (default same as \code{cdm_schema}).
#' @param target_schema Target/results schema (default same as \code{cdm_schema}).
#' @param target_table Target cohort table name (default \code{"cohort"}).
#' @param target_dialect SQL dialect for translation (default \code{"sql server"}).
#' @param render If \code{TRUE}, substitute \code{@} parameters (default \code{TRUE}).
#' @param generate_stats Include inclusion rule stats in SQL (default \code{FALSE}).
#' @return A single character string containing the cohort SQL.
#' @noRd
atlas_json_to_sql <- function(json,
                              cohort_id = 1L,
                              cdm_schema = "main",
                              vocabulary_schema = NULL,
                              target_schema = NULL,
                              target_table = "cohort",
                              target_dialect = "sql server",
                              render = TRUE,
                              generate_stats = FALSE) {
  json_str <- read_json_input(json)
  cohort <- cohortExpressionFromJson(json_str)
  opts <- list(
    cdm_schema = cdm_schema,
    vocabulary_schema = vocabulary_schema %||% cdm_schema,
    target_table = paste0(target_schema %||% cdm_schema, ".", target_table),
    cohort_id = as.integer(cohort_id),
    cohort_id_field_name = "cohort_definition_id",
    generate_stats = generate_stats
  )
  sql <- buildCohortQuery(cohort, opts)
  if (isTRUE(render)) {
    sql <- render(
      sql,
      cdm_database_schema = cdm_schema,
      vocabulary_database_schema = opts$vocabulary_schema,
      target_database_schema = target_schema %||% cdm_schema,
      target_cohort_table = target_table,
      target_cohort_id = as.integer(cohort_id),
      cohort_id_field_name = "cohort_definition_id",
      results_database_schema = target_schema %||% cdm_schema,
      warnOnMissingParameters = FALSE
    )
  }
  if (!is.null(target_dialect) && nzchar(target_dialect)) {
    sql <- translate(sql, targetDialect = target_dialect)
  }
  sql
}

#' Batch: multiple Atlas JSONs to one optimized SQL script
#'
#' Translates multiple Atlas cohort definitions into a single optimized batch SQL script.
#' When \code{optimize} is \code{TRUE}, generates optimized SQL directly from cohort objects
#' (no temp files, no regex over SQL).
#'
#' @param json_inputs List of paths/JSON strings, or data frame with \code{cohort_definition_id} and \code{cohort}.
#' @param cdm_schema CDM schema placeholder (default \code{"@cdm_database_schema"}).
#' @param results_schema Results schema placeholder (default \code{"@results_schema"}).
#' @param target_dialect SQL dialect (default \code{"sql server"}). Use \code{NULL} to skip.
#' @param optimize If \code{TRUE}, produce optimized batch script (default \code{TRUE}).
#' @param enable_qualified_events_all Ignored (kept for backward compatibility).
#' @param enable_domain_event_cache Ignored (kept for backward compatibility).
#' @param force_full_path Ignored (kept for backward compatibility).
#' @param cache Logical; if TRUE, enable incremental DAG caching. Requires
#'   \code{con} and uses a persistent registry table. Default FALSE.
#' @param con DBI connection; required when \code{cache = TRUE}.
#' @param resolved_schema Character resolved schema name; required when \code{cache = TRUE}.
#' @return When \code{cache = FALSE}: a single character string containing the batch cohort SQL.
#'   When \code{cache = TRUE}: a list with \code{sql}, \code{dag}, \code{cache_hits}, \code{cache_misses}.
#' @noRd
atlas_json_to_sql_batch <- function(json_inputs,
                                    cdm_schema = "@cdm_database_schema",
                                    results_schema = "@results_schema",
                                    target_dialect = "sql server",
                                    optimize = TRUE,
                                    table_prefix = NULL,
                                    enable_qualified_events_all = TRUE,
                                    enable_domain_event_cache = FALSE,
                                    force_full_path = FALSE,
                                    cache = FALSE,
                                    con = NULL,
                                    resolved_schema = NULL,
                                    cdm_table_sql = NULL) {
  if (is.data.frame(json_inputs)) {
    stopifnot("cohort_definition_id" %in% names(json_inputs), "cohort" %in% names(json_inputs))
    cohort_ids <- as.integer(json_inputs$cohort_definition_id)
    json_list <- lapply(json_inputs$cohort, function(c) {
      if (is.character(c) && length(c) == 1L) {
        if (file.exists(c)) read_json_input(c) else c
      } else if (is.list(c))
        as.character(jsonlite::toJSON(c, auto_unbox = TRUE, null = "null"))
      else
        stop("Each cohort must be a JSON string or list.")
    })
  } else {
    if (!is.list(json_inputs)) json_inputs <- list(json_inputs)
    json_list <- lapply(json_inputs, read_json_input)
    cohort_ids <- seq_len(length(json_list))
  }

  if (!isTRUE(optimize) || length(json_list) == 0L) {
    build_opts <- function(cid) list(
      cdm_schema = cdm_schema,
      vocabulary_schema = "@vocabulary_database_schema",
      target_table = "@target_database_schema.@target_cohort_table",
      cohort_id = cid,
      cohort_id_field_name = "cohort_definition_id",
      generate_stats = FALSE
    )
    sql_list <- vector("list", length(json_list))
    for (i in seq_along(json_list)) {
      cohort <- cohortExpressionFromJson(json_list[[i]])
      sql_list[[i]] <- buildCohortQuery(cohort, build_opts(cohort_ids[[i]]))
    }
    batch_sql <- paste(sql_list, collapse = "\n\n")
    if (!is.null(target_dialect) && nzchar(target_dialect))
      batch_sql <- translate(batch_sql, targetDialect = target_dialect)
    return(batch_sql)
  }

  cohort_list <- lapply(json_list, cohortExpressionFromJson)
  result <- buildBatchCohortQuery(cohort_list, cohort_ids, list(
    cdm_schema = cdm_schema,
    results_schema = results_schema,
    table_prefix = table_prefix,
    cdm_table_sql = cdm_table_sql
  ), cache = cache, con = con, schema = resolved_schema)

  if (isTRUE(cache)) {
    # Cached path returns a list; translate the SQL component
    if (!is.null(target_dialect) && nzchar(target_dialect))
      result$sql <- translate(result$sql, targetDialect = target_dialect)
    return(result)
  }

  # Non-cached path: result is a plain SQL string
  batch_sql <- result
  if (!is.null(target_dialect) && nzchar(target_dialect))
    batch_sql <- translate(batch_sql, targetDialect = target_dialect)
  batch_sql
}


#' Normalize a CDMConnector schema value (list, named vector, or string) to a single string.
#' Strips the "prefix" element if present (the prefix is a table-name prefix, not part of the schema).
#' @noRd
normalize_schema_str <- function(x, default = "main") {
  if (is.null(x)) return(default)
  if (is.character(x) && length(x) == 1L) return(x)
  if (is.list(x)) {
    x <- x[!names(x) %in% "prefix"]
    return(paste(unlist(x), collapse = "."))
  }
  # Named character vector — strip "prefix" element
  if (!is.null(names(x))) x <- x[!names(x) %in% "prefix"]
  paste(as.character(x), collapse = ".")
}

#' Extract the table-name prefix from a CDMConnector write schema.
#' Returns "" if no prefix is present.
#' @noRd
extract_write_prefix <- function(x) {
  if (is.null(x)) return("")
  if (is.list(x)) return(x[["prefix"]] %||% "")
  if (!is.null(names(x)) && "prefix" %in% names(x)) return(as.character(x[["prefix"]]))
  ""
}

#' Generate a cohort set on a CDM object (optimized, no Java dependency)
#'
#' @description
#' Generates cohort sets using an optimized DAG-based SQL pipeline that does not
#' require Java or CirceR. This is a faster alternative to
#' \code{\link{generateCohortSet}} that produces equivalent results.
#'
#' A "cohort_table" object consists of four components:
#' \itemize{
#'   \item{A remote table reference to an OHDSI cohort table with columns:
#'         cohort_definition_id, subject_id, cohort_start_date,
#'         cohort_end_date.}
#'   \item{A **settings attribute** containing cohort settings including names.}
#'   \item{An **attrition attribute** with attrition information recorded during
#'         generation. This attribute is optional. Since calculating attrition
#'         takes additional compute it can be skipped resulting in a NULL
#'         attrition attribute.}
#'   \item{A **cohortCounts attribute** containing cohort counts.}
#' }
#'
#' @param cdm A cdm reference created by CDMConnector. write_schema must be
#'   specified.
#' @param cohortSet A cohortSet dataframe created with \code{\link{readCohortSet}}.
#' @param name Name of the cohort table to be created. This will also be used
#'   as a prefix for the cohort attribute tables. Must be a lowercase character
#'   string that starts with a letter and only contains letters, numbers, and
#'   underscores.
#' @param computeAttrition Should attrition be computed? TRUE (default) or FALSE
#' @param overwrite Should the cohort table be overwritten if it already
#'   exists? TRUE (default) or FALSE
#' @param cache Logical; if TRUE, enable incremental DAG caching. Previously
#'   computed intermediate tables are reused, and only changed portions of the
#'   DAG are recomputed. Default FALSE.
#'
#' @return A cdm reference with the generated cohort table added.
#' @importFrom utils flush.console
#' @export
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = "main",
#'                   writeSchema = "main")
#'
#' cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
#' cdm <- generateCohortSet2(cdm, cohortSet, name = "cohort")
#'
#' print(cdm$cohort)
#'
#' attrition(cdm$cohort)
#' settings(cdm$cohort)
#' cohortCount(cdm$cohort)
#' }
generateCohortSet2 <- function(cdm,
                                cohortSet,
                                name,
                                computeAttrition = TRUE,
                                overwrite = TRUE,
                                cache = FALSE) {

  # ---- Argument validation ----
  checkmate::assertClass(cdm, "cdm_reference")

  if (!is.data.frame(cohortSet)) {
    rlang::abort("`cohortSet` must be a dataframe from the output of `readCohortSet()`.")
  }
  checkmate::assertDataFrame(cohortSet, min.rows = 1, col.names = "named")

  # Handle OHDSI column name variants
  if ("cohortId" %in% names(cohortSet) && !("cohort_definition_id" %in% names(cohortSet))) {
    cohortSet$cohort_definition_id <- cohortSet$cohortId
  }
  if ("cohortName" %in% names(cohortSet) && !("cohort_name" %in% names(cohortSet))) {
    cohortSet$cohort_name <- cohortSet$cohortName
  }

  # Parse JSON to cohort list-column if needed
  if (!("cohort" %in% names(cohortSet)) && ("json" %in% names(cohortSet))) {
    cohortColumn <- list()
    for (i in seq_len(nrow(cohortSet))) {
      x <- cohortSet$json[i]
      if (!validUTF8(x)) { rlang::abort("Failed to convert json UTF-8 encoding") }
      cohortColumn[[i]] <- jsonlite::fromJSON(x, simplifyVector = FALSE)
    }
    cohortSet$cohort <- cohortColumn
  }

  stopifnot(all(c("cohort_definition_id", "cohort_name") %in% names(cohortSet)))
  stopifnot("cohort" %in% names(cohortSet) || "json" %in% names(cohortSet))

  checkmate::assertCharacter(name, len = 1, min.chars = 1, any.missing = FALSE)
  if (name != tolower(name)) {
    cli::cli_abort("Cohort table name {name} must be lowercase!")
  }
  if (!grepl("^[a-z]", substr(name, 1, 1))) {
    cli::cli_abort("Cohort table name {name} must start with a letter!")
  }
  if (!grepl("^[a-z][a-z0-9_]*$", name)) {
    cli::cli_abort("Cohort table name {name} must only contain letters, numbers, and underscores!")
  }
  checkmate::assertLogical(computeAttrition, len = 1)
  checkmate::assertLogical(overwrite, len = 1)

  cli::cli_alert_info("Generating {nrow(cohortSet)} cohort{?s}")

  # ---- Local CDM dispatch ----
  con <- cdmCon(cdm)
  if (is.null(con)) {
    return(generateCohortSetLocal2(
      cdm = cdm,
      cohortSet = cohortSet,
      name = name,
      computeAttrition = computeAttrition,
      overwrite = overwrite
    ))
  }

  checkmate::assertTRUE(DBI::dbIsValid(con))
  db <- dbms(con)
  results_schema <- cdmWriteSchema(cdm)
  cdm_schema_str <- normalize_schema_str(attr(cdm, "cdm_schema"))
  results_schema_str <- normalize_schema_str(results_schema)
  write_prefix <- extract_write_prefix(results_schema)
  checkmate::assert_character(results_schema,
                              min.chars = 1,
                              min.len = 1,
                              max.len = 3,
                              null.ok = FALSE)

  # ---- Overwrite logic ----
  existingTables <- listTables(con, results_schema)
  tables_to_check <- c(name, paste0(name, "_set"), paste0(name, "_attrition"))
  for (x in tables_to_check) {
    if (x %in% existingTables) {
      if (overwrite) {
        DBI::dbRemoveTable(con, .inSchema(results_schema, x, dbms = db))
      } else {
        cli::cli_abort("The cohort table {paste0(write_prefix, name)} already exists.\nSpecify overwrite = TRUE to overwrite it.")
      }
    }
  }

  target_dialect <- switch(db,
    "duckdb"     = "duckdb",
    "sqlserver"  = "sql server",
    "sql server" = "sql server",
    "postgresql" = "postgresql",
    "postgres"   = "postgresql",
    "redshift"   = "redshift",
    "snowflake"  = "snowflake",
    "spark"      = "spark",
    "bigquery"   = "bigquery",
    "oracle"     = "oracle",
    db  # default: pass db string through to SqlRender
  )

  # CDMConnector write schema may include a table-name prefix (e.g. "mdb99_")
  # that must be prepended to all tables we create. This ensures our tables
  # match what inSchema() expects when reading them back.
  # The sql_name is the prefixed table name used in SQL; `name` stays unprefixed
  # for inSchema() which adds the prefix itself.
  sql_name <- paste0(write_prefix, name)
  ie_name <- paste0(write_prefix, "inclusion_events")
  is_name <- paste0(write_prefix, "inclusion_stats")

  # Build quoted schema.table references for output tables using DBI::dbQuoteIdentifier.
  # Critical for Snowflake where unquoted identifiers are uppercased but CDMConnector
  # reads with quoted lowercase identifiers (case-sensitive mismatch).
  .quote_out_tbl <- function(tbl_name) {
    schema_parts <- strsplit(results_schema_str, ".", fixed = TRUE)[[1]]
    quoted <- vapply(schema_parts, function(p) as.character(DBI::dbQuoteIdentifier(con, p)), character(1))
    paste0(paste(quoted, collapse = "."), ".", DBI::dbQuoteIdentifier(con, tbl_name))
  }
  q_sql <- .quote_out_tbl(sql_name)
  q_ie  <- .quote_out_tbl(ie_name)
  q_is  <- .quote_out_tbl(is_name)

  if (isTRUE(cache)) {
    prefix <- paste0(write_prefix, CACHE_TABLE_PREFIX)
  } else {
    prefix <- paste0(write_prefix, atlas_unique_prefix())
  }
  # ---- Detect CDM table modifications (subset, filter, etc.) ----
  # Parse cohorts to find which CDM tables are needed
  cohort_list_parsed <- lapply(cohortSet$cohort, function(c) {
    if (is.character(c) && length(c) == 1L) cohortExpressionFromJson(c)
    else if (is.list(c)) cohortExpressionFromJson(jsonlite::toJSON(c, auto_unbox = TRUE, null = "null"))
    else c
  })
  needed_tables <- collect_batch_used_domains_from_cohorts(cohort_list_parsed)

  # Validate: error if required CDM tables are missing from the CDM object
  cdm_names_lower <- tolower(names(cdm))
  for (tbl_upper in needed_tables) {
    tbl_lower <- tolower(tbl_upper)
    if (!tbl_lower %in% cdm_names_lower) {
      cli::cli_abort("CDM is missing table {.val {tbl_lower}} required by the cohort definitions.")
    }
  }

  # Extract CDM table SQL for modified tables (cdmSubset / dplyr filter support).
  # Only for domain tables + OBSERVATION_PERIOD; only when the rendered SQL
  # contains JOIN/WHERE/UNION/EXCEPT/INTERSECT (i.e., the table has been modified).
  # Unmodified tables keep using the raw @cdm_database_schema.TABLE reference.
  cdm_table_sql <- NULL
  domain_plus_op <- c(vapply(DOMAIN_CONFIG, `[[`, character(1), "table"), "OBSERVATION_PERIOD")
  for (tbl_upper in intersect(needed_tables, domain_plus_op)) {
    tbl_lower <- tolower(tbl_upper)
    if (tbl_lower %in% cdm_names_lower && inherits(cdm[[tbl_lower]], "tbl_lazy")) {
      rendered <- as.character(dbplyr::sql_render(cdm[[tbl_lower]]))
      if (grepl("(JOIN|WHERE|UNION|EXCEPT|INTERSECT)", rendered, ignore.case = TRUE)) {
        if (is.null(cdm_table_sql)) cdm_table_sql <- list()
        cdm_table_sql[[tbl_upper]] <- rendered
      }
    }
  }

  # Generate SQL WITHOUT dialect translation -- translation is done per-statement
  # later to avoid O(n^2) SqlRender::translate on the full batch string.
  batch_result <- atlas_json_to_sql_batch(json_inputs = cohortSet,
                                 cdm_schema = cdm_schema_str,
                                 results_schema = results_schema_str,
                                 target_dialect = NULL,
                                 optimize = TRUE,
                                 table_prefix = prefix,
                                 cache = cache,
                                 con = if (isTRUE(cache)) con else NULL,
                                 resolved_schema = if (isTRUE(cache)) results_schema_str else NULL,
                                 cdm_table_sql = cdm_table_sql)

  # Extract SQL and cache metadata
  if (isTRUE(cache)) {
    sql <- batch_result$sql
    cache_hits <- batch_result$cache_hits
    cache_misses <- batch_result$cache_misses
    dag <- batch_result$dag
    dag_options <- batch_result$options
    n_hits <- length(cache_hits)
    n_misses <- length(cache_misses)
    message(sprintf("DAG cache: %d hits, %d misses (%d nodes to compute)",
                    n_hits, n_misses, n_misses))
  } else {
    sql <- batch_result
  }

  # Replace all @-parameters with concrete schema values (order matters: longer patterns first)
  # Uses stringi C-backed fixed replacement for speed on large strings.
  sql <- stringi::stri_replace_all_fixed(sql, "@vocabulary_database_schema", cdm_schema_str)
  sql <- stringi::stri_replace_all_fixed(sql, "@target_database_schema", results_schema_str)
  sql <- stringi::stri_replace_all_fixed(sql, "@target_cohort_table", sql_name)
  sql <- stringi::stri_replace_all_fixed(sql, "@cdm_database_schema", cdm_schema_str)
  sql <- stringi::stri_replace_all_fixed(sql, "@results_schema", results_schema_str)
  # Apply CDMConnector write prefix to inclusion table names in the generated SQL.
  # These are hardcoded as @results_schema.inclusion_events in the SQL template.
  if (nzchar(write_prefix)) {
    sql <- stringi::stri_replace_all_fixed(sql,
      paste0(results_schema_str, ".inclusion_events"),
      paste0(results_schema_str, ".", ie_name))
    sql <- stringi::stri_replace_all_fixed(sql,
      paste0(results_schema_str, ".inclusion_stats"),
      paste0(results_schema_str, ".", is_name))
  }

  # Quote output table references to match CDMConnector's case-sensitive quoting.
  # Unquoted table names on Snowflake are stored as UPPERCASE but CDMConnector reads
  # with quoted lowercase. Replace unquoted output refs with properly quoted versions.
  out_ref_sql <- paste0(results_schema_str, ".", sql_name)
  out_ref_ie  <- paste0(results_schema_str, ".", ie_name)
  out_ref_is  <- paste0(results_schema_str, ".", is_name)
  if (q_sql != out_ref_sql) {
    sql <- stringi::stri_replace_all_fixed(sql, out_ref_sql, q_sql)
    sql <- stringi::stri_replace_all_fixed(sql, out_ref_ie,  q_ie)
    sql <- stringi::stri_replace_all_fixed(sql, out_ref_is,  q_is)
  }
  if (grepl("@[a-z_]+", sql, perl = TRUE)) {
    remaining <- unique(regmatches(sql, gregexpr("@[a-z_]+", sql, perl = TRUE))[[1]])
    message("Unresolved @-params: ", paste(remaining, collapse = ", "))
    first <- remaining[1]
    pos <- regexpr(first, sql, fixed = TRUE)
    if (pos > 0) message("Context: ...", substr(sql, max(1, pos - 60), min(nchar(sql), pos + 60 + nchar(first))), "...")
    stop("Unresolved @-params found in SQL")
  }

  # Quote CDM table references on the full string (20 regex gsubs, fast C-level).
  # Done before split/translate since it doesn't interfere with SqlRender patterns.
  sql <- quote_cdm_table_refs(sql, con, cdm_schema_str)

  # Debug: dump pre-translate SQL (set option to a file path)
  debug_sql_file <- getOption("atlasCohortGenerator.debug_sql_file")
  if (is.character(debug_sql_file) && length(debug_sql_file) == 1L && nzchar(debug_sql_file)) {
    writeLines(sql, debug_sql_file)
    message("Batch SQL written to: ", debug_sql_file)
  }

  # Fast regex-based split into individual SQL statements.
  # Much faster than split_sql_core() which uses expensive per-character tokenization.
  # Safe for DAG-generated SQL which has no semicolons inside string literals.
  # Uses stringi C-backed split for speed.
  sql_split <- stringi::stri_split_regex(stringi::stri_join(sql, "\n"), ";[ \t]*\r?\n")[[1]]
  sql_split <- trimws(sql_split)
  sql_split <- sql_split[nzchar(sql_split)]
  # Filter out pure comment/empty blocks (keep anything with a SQL keyword)
  has_sql <- grepl("(CREATE|DROP|INSERT|SELECT|DELETE|UPDATE|WITH|TRUNCATE|ANALYZE)", sql_split, ignore.case = TRUE)
  sql_split <- sql_split[has_sql]

  # Resolve {N != 0}?{then}:{else} conditionals per-statement (fast bailout for most).
  for (i in seq_along(sql_split)) {
    sql_split[i] <- resolve_literal_conditionals(sql_split[i])
  }

  # Translate SQL statements to target dialect.
  # For DuckDB/PostgreSQL: lightweight cohort-SQL-only translator (~50x faster
  # than SqlRender) using focused stringi transforms.
  # For other dialects: per-statement pure-R translation (avoids O(n^2) batch scaling).
  translated <- translate_cohort_stmts(sql_split, target_dialect)

  n <- length(translated)
  pb <- utils::txtProgressBar(min = 0, max = n, style = 3)
  on.exit(close(pb), add = TRUE)

  # For non-cached runs, drop any existing prefixed working tables before generation.
  # For cached runs, we skip this -- cached tables are meant to persist.
  if (!isTRUE(cache)) {
    drop_prefixed_tables(con, results_schema_str, prefix)
  }

  # Create output tables expected by batch finalize.
  # Spark/Databricks doesn't support NULL/NOT NULL constraints in CREATE TABLE.
  # Spark/Databricks: use CREATE OR REPLACE TABLE to avoid TABLE_ALREADY_EXISTS
  # errors from stale tables left by previous failed runs.
  is_spark <- db %in% c("spark", "databricks")
  nn <- if (is_spark) "" else " NOT NULL"

  if (is_spark) {
    output_ddls <- c(
      sprintf("CREATE OR REPLACE TABLE %s (cohort_definition_id INTEGER, subject_id INTEGER, cohort_start_date DATE, cohort_end_date DATE)", q_sql),
      sprintf("CREATE OR REPLACE TABLE %s (cohort_definition_id INTEGER, inclusion_rule_id INTEGER, person_id INTEGER, event_id INTEGER)", q_ie),
      sprintf("CREATE OR REPLACE TABLE %s (cohort_definition_id INTEGER, inclusion_rule_id INTEGER, person_count INTEGER, gain_count INTEGER, person_total INTEGER)", q_is)
    )
  } else {
    output_ddls <- c(
      sprintf("DROP TABLE IF EXISTS %s", q_sql),
      sprintf("CREATE TABLE %s (cohort_definition_id INTEGER%s, subject_id INTEGER%s, cohort_start_date DATE%s, cohort_end_date DATE%s)", q_sql, nn, nn, nn, nn),
      sprintf("DROP TABLE IF EXISTS %s", q_ie),
      sprintf("CREATE TABLE %s (cohort_definition_id INTEGER%s, inclusion_rule_id INTEGER%s, person_id INTEGER%s, event_id INTEGER%s)", q_ie, nn, nn, nn, nn),
      sprintf("DROP TABLE IF EXISTS %s", q_is),
      sprintf("CREATE TABLE %s (cohort_definition_id INTEGER, inclusion_rule_id INTEGER, person_count INTEGER, gain_count INTEGER, person_total INTEGER)", q_is)
    )
  }
  for (ddl in output_ddls) DBI::dbExecute(con, ddl)

  tryCatch({
    for (i in seq_len(n)) {
      stmt <- translated[i]
      if (!nzchar(stmt)) {
        utils::setTxtProgressBar(pb, i)
        next
      }
      tryCatch({
        DBI::dbExecute(con, stmt)
      }, error = function(e) {
        cat("Failing statement ", i, ":\n", stmt, "\n")
        stop(e)
      })
      utils::setTxtProgressBar(pb, i)
      flush.console()
    }

    if (isTRUE(cache)) {
      # Register newly computed nodes in the cache registry
      cache_register_new_nodes(dag, dag_options, con, results_schema_str, cache_misses)
    } else {
      # Non-cached: drop all working tables
      drop_prefixed_tables(con, results_schema_str, prefix)
    }
  }, error = function(e) {
    if (!isTRUE(cache)) {
      # Non-cached: clean up on error
      tryCatch(drop_prefixed_tables(con, results_schema_str, prefix), error = function(clean_e) NULL)
    }
    # Cached: leave tables in place on error (partial progress is still valid)
    stop(e)
  })

  # Snowflake: unquoted column names are stored as UPPERCASE, but CDMConnector
  # requires lowercase. Recreate output tables with lowercase column aliases.
  if (db == "snowflake") {
    .snowflake_lowercase_cols <- function(qtbl, cols) {
      alias_list <- paste(vapply(cols, function(c) {
        sprintf("%s AS %s", toupper(c), DBI::dbQuoteIdentifier(con, c))
      }, character(1)), collapse = ", ")
      DBI::dbExecute(con, sprintf("CREATE OR REPLACE TABLE %s AS SELECT %s FROM %s", qtbl, alias_list, qtbl))
    }
    .snowflake_lowercase_cols(q_sql, c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"))
    .snowflake_lowercase_cols(q_ie,  c("cohort_definition_id", "inclusion_rule_id", "person_id", "event_id"))
    .snowflake_lowercase_cols(q_is,  c("cohort_definition_id", "inclusion_rule_id", "person_count", "gain_count", "person_total"))
  }

  cdm[[name]] <- dplyr::tbl(con, inSchema(results_schema, name, dbms = db))

  # ---- Compute attrition ----
  if (computeAttrition) {
    cohort_attrition_ref <- computeAttritionTable2(
      cdm = cdm,
      name = name,
      cohortSet = cohortSet
    )
  } else {
    cohort_attrition_ref <- NULL
  }

  # ---- Build cohort object ----
  cohort_set_ref <- dplyr::tibble(
    cohort_definition_id = as.integer(cohortSet$cohort_definition_id),
    cohort_name = if ("cohort_name" %in% names(cohortSet)) as.character(cohortSet$cohort_name) else paste0("cohort_", cohortSet$cohort_definition_id)
  )

  cdm[[name]] <- omopgenerics::newCdmTable(
    table = cdm[[name]],
    src = attr(cdm, "cdm_source"),
    name = name
  )

  cohortCodelistRef <- createAtlasCohortCodelistReference(cdm, cohortSet)
  cdm[[name]] <- omopgenerics::newCohortTable(
    table = cdm[[name]],
    cohortSetRef = cohort_set_ref,
    cohortAttritionRef = cohort_attrition_ref,
    cohortCodelistRef = cohortCodelistRef
  )

  cdm
}


# Compute sequential attrition from the DAG's inclusion_events/inclusion_stats tables
#
# @description Internal function that computes sequential attrition from the
# output tables populated by the optimized DAG pipeline. Uses the
# inclusion_events table (raw per-rule matches) and inclusion_stats table
# (qualified events total) to compute cumulative attrition.
#
# @param cdm A cdm_reference
# @param name Character; cohort table stem name
# @param cohortSet Data frame with cohort_definition_id and cohort columns
#
# @return A data.frame with attrition data (cohort_definition_id, number_records,
#   number_subjects, reason_id, reason, excluded_records, excluded_subjects)
# @keywords internal
computeAttritionTable2 <- function(cdm, name, cohortSet) {
  con <- cdmCon(cdm)
  schema <- cdmWriteSchema(cdm)
  db <- dbms(con)

  # Read the inclusion_events and inclusion_stats tables from DB
  ie_tbl_name <- "inclusion_events"
  is_tbl_name <- "inclusion_stats"
  ie_tbl <- dplyr::tbl(con, inSchema(schema, ie_tbl_name, dbms = db)) %>%
    dplyr::rename_all(tolower)
  is_tbl <- dplyr::tbl(con, inSchema(schema, is_tbl_name, dbms = db)) %>%
    dplyr::rename_all(tolower)

  cohort_tbl <- dplyr::tbl(con, inSchema(schema, name, dbms = db)) %>%
    dplyr::rename_all(tolower)

  attrition_list <- list()

  for (i in seq_len(nrow(cohortSet))) {
    id <- as.integer(cohortSet$cohort_definition_id[i])

    # Get inclusion rule names from parsed cohort JSON
    inclusion_rules <- cohortSet$cohort[[i]]$InclusionRules %||%
      cohortSet$cohort[[i]]$inclusionRules %||%
      list()
    n_rules <- length(inclusion_rules)

    # Get qualified events total from inclusion_stats (sentinel row with rule_id = -1)
    total_stats <- is_tbl %>%
      dplyr::filter(.data$cohort_definition_id == .env$id,
                     .data$inclusion_rule_id == -1L) %>%
      dplyr::collect()

    if (n_rules == 0 || nrow(total_stats) == 0) {
      # No inclusion rules: single row from final cohort counts
      counts <- cohort_tbl %>%
        dplyr::filter(.data$cohort_definition_id == .env$id) %>%
        dplyr::summarise(
          n_records = dplyr::n(),
          n_subjects = dplyr::n_distinct(.data$subject_id)
        ) %>%
        dplyr::collect()

      attrition_list[[i]] <- dplyr::tibble(
        cohort_definition_id = id,
        number_records = as.integer(counts$n_records),
        number_subjects = as.integer(counts$n_subjects),
        reason_id = 1L,
        reason = "Qualifying initial records",
        excluded_records = 0L,
        excluded_subjects = 0L
      )
    } else {
      # Sequential attrition: count persons/events matching ALL rules 0..k
      total_records <- as.integer(total_stats$person_total[1])
      total_subjects <- as.integer(total_stats$person_count[1])

      rows <- list()
      rows[[1]] <- dplyr::tibble(
        cohort_definition_id = id,
        number_records = total_records,
        number_subjects = total_subjects,
        reason_id = 1L,
        reason = "Qualifying initial records"
      )

      for (k in seq_len(n_rules)) {
        rule_name <- inclusion_rules[[k]]$name %||% "Unnamed criteria"

        # Count persons/events matching ALL of rules 0..(k-1)
        # A person/event passes all k rules if they appear k times in
        # the inclusion_events table for rules 0..k-1
        counts <- ie_tbl %>%
          dplyr::filter(.data$cohort_definition_id == .env$id,
                         .data$inclusion_rule_id <= .env$k - 1L) %>%
          dplyr::group_by(.data$person_id, .data$event_id) %>%
          dplyr::summarise(n_matched = dplyr::n(), .groups = "drop") %>%
          dplyr::filter(.data$n_matched == .env$k) %>%
          dplyr::ungroup() %>%
          dplyr::summarise(
            n_records = dplyr::n(),
            n_subjects = dplyr::n_distinct(.data$person_id)
          ) %>%
          dplyr::collect()

        rows[[k + 1]] <- dplyr::tibble(
          cohort_definition_id = id,
          number_records = as.integer(counts$n_records),
          number_subjects = as.integer(counts$n_subjects),
          reason_id = as.integer(k + 1L),
          reason = rule_name
        )
      }

      attrition <- dplyr::bind_rows(rows) %>%
        dplyr::mutate(
          excluded_records =
            dplyr::lag(.data$number_records, 1, order_by = .data$reason_id) -
            .data$number_records,
          excluded_subjects =
            dplyr::lag(.data$number_subjects, 1, order_by = .data$reason_id) -
            .data$number_subjects
        ) %>%
        dplyr::mutate(
          excluded_records = dplyr::coalesce(.data$excluded_records, 0L),
          excluded_subjects = dplyr::coalesce(.data$excluded_subjects, 0L)
        )

      attrition_list[[i]] <- attrition
    }
  }

  attrition <- dplyr::bind_rows(attrition_list) %>%
    dplyr::transmute(
      cohort_definition_id = as.integer(.data$cohort_definition_id),
      number_records = as.integer(.data$number_records),
      number_subjects = as.integer(.data$number_subjects),
      reason_id = as.integer(.data$reason_id),
      reason = as.character(.data$reason),
      excluded_records = as.integer(.data$excluded_records),
      excluded_subjects = as.integer(.data$excluded_subjects)
    )

  # Check if final cohort counts differ from last attrition row (era-ification collapse)
  final_counts <- cohort_tbl %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(n_records = dplyr::n(), n_subjects = dplyr::n_distinct(.data$subject_id)) %>%
    dplyr::collect() %>%
    dplyr::mutate_all(as.integer)

  last_attrition_row <- dplyr::slice_max(attrition, n = 1, order_by = .data$reason_id, by = "cohort_definition_id")

  new_attrition_row <- dplyr::inner_join(final_counts, last_attrition_row, by = "cohort_definition_id") %>%
    dplyr::filter(.data$number_records != .data$n_records |
                  .data$number_subjects != .data$n_subjects) %>%
    dplyr::transmute(
      cohort_definition_id = .data$cohort_definition_id,
      number_records = .data$n_records,
      number_subjects = .data$n_subjects,
      reason_id = 1L + .data$reason_id,
      reason = "Cohort records collapsed",
      excluded_records = .data$number_records - .data$n_records,
      excluded_subjects = .data$number_subjects - .data$n_subjects
    )

  attrition <- dplyr::bind_rows(attrition, new_attrition_row) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$reason_id)

  attrition
}


#' Generate a cohort set on a local CDM using the optimized pipeline
#'
#' Copies the local CDM to an in-memory DuckDB database, runs
#' \code{\link{generateCohortSet2}}, then collects the generated cohort table
#' and its attributes back into R and adds them to the input CDM.
#'
#' @param cdm A local cdm object (list of dataframes).
#' @param cohortSet A cohort set from \code{\link{readCohortSet}}.
#' @param name Name of the cohort table to create.
#' @param computeAttrition Whether to compute attrition.
#' @param overwrite Whether to overwrite an existing cohort table.
#' @return The input \code{cdm} with the new cohort table added (as local
#'   dataframes).
#' @keywords internal
#' @noRd
generateCohortSetLocal2 <- function(cdm,
                                    cohortSet,
                                    name,
                                    computeAttrition = TRUE,
                                    overwrite = TRUE) {
  rlang::check_installed("duckdb")
  checkmate::assert_class(cdm, "cdm_reference")
  checkmate::assert_list(cdm, names = "named")
  checkmate::assert_character(name, len = 1L, min.chars = 1L)

  # Create in-memory DuckDB and copy CDM tables (skip existing cohort tables)
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  write_schema <- c(schema = "main")
  for (nm in names(cdm)) {
    if (inherits(cdm[[nm]], "cohort_table")) {
      next
    }
    tbl <- cdm[[nm]]
    full_name <- .inSchema(schema = write_schema, table = nm, dbms = "duckdb")
    DBI::dbWriteTable(con, name = full_name, value = dplyr::as_tibble(tbl), overwrite = TRUE)
  }

  cdm_name <- omopgenerics::cdmName(cdm)
  cdm_db <- cdmFromCon(
    con = con,
    cdmSchema = "main",
    writeSchema = "main",
    cdmName = cdm_name
  )

  cdm_db <- generateCohortSet2(
    cdm = cdm_db,
    cohortSet = cohortSet,
    name = name,
    computeAttrition = computeAttrition,
    overwrite = overwrite
  )

  # Collect cohort table and attributes back to R
  cohort_df <- dplyr::collect(cdm_db[[name]]) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::mutate(
      cohort_definition_id = as.integer(.data$cohort_definition_id),
      subject_id = as.integer(.data$subject_id),
      cohort_start_date = as.Date(.data$cohort_start_date),
      cohort_end_date = as.Date(.data$cohort_end_date)
    )

  cohort_set_df <- omopgenerics::settings(cdm_db[[name]])
  cohort_attrition_df <- omopgenerics::attrition(cdm_db[[name]])
  cohort_codelist_ref <- attr(cdm_db[[name]], "cohort_codelist")
  cohort_codelist_df <- NULL
  if (!is.null(cohort_codelist_ref)) {
    if (inherits(cohort_codelist_ref, "tbl_lazy")) {
      cohort_codelist_df <- dplyr::collect(cohort_codelist_ref) %>% dplyr::rename_all(tolower)
    } else if (is.data.frame(cohort_codelist_ref)) {
      cohort_codelist_df <- cohort_codelist_ref
    }
  }

  # Wrap cohort dataframe as cdm_table and add to cdm before newCohortTable
  # (validation requires cohort to be part of cdm_reference)
  cohort_tbl <- omopgenerics::newCdmTable(
    table = cohort_df,
    src = attr(cdm, "cdm_source"),
    name = name
  )
  cdm[[name]] <- cohort_tbl
  cdm[[name]] <- omopgenerics::newCohortTable(
    table = cdm[[name]],
    cohortSetRef = cohort_set_df,
    cohortAttritionRef = cohort_attrition_df,
    cohortCodelistRef = cohort_codelist_df
  )

  cdm
}


#' Validate batch vs single-cohort equivalence
#'
#' Runs the cohort set through the optimized DAG batch path and also through
#' the independent single-cohort DAG path (one cohort at a time), comparing
#' final cohort tables row-by-row.
#' Reports which cohort_definition_id(s) diverge and by how many rows.
#'
#' @param cdm CDM reference.
#' @param cohortSet Cohort set (data.frame with cohort_definition_id, cohort columns).
#' @param name Base table name (default "validate_eq").
#' @return Invisible TRUE if all match, FALSE otherwise. Prints diagnostics.
#' @noRd
validate_batch_equivalence <- function(cdm, cohortSet, name = "validate_eq") {
  con <- cdmCon(cdm)
  results_schema <- cdmWriteSchema(cdm)
  results_schema_str <- normalize_schema_str(results_schema)
  db <- dbms(con)

  # Run optimized batch (the path we want to validate)
  batch_name <- paste0(name, "_batch")
  cdm <- generateCohortSet2(cdm, cohortSet, name = batch_name)
  batch_df <- dplyr::collect(cdm[[batch_name]])
  batch_df <- batch_df[order(batch_df$cohort_definition_id, batch_df$subject_id, batch_df$cohort_start_date), ]
  rownames(batch_df) <- NULL

  # Run each cohort independently through the DAG path (one cohort at a time)
  # to verify batch deduplication doesn't change results
  single_dfs <- list()
  for (i in seq_len(nrow(cohortSet))) {
    single_name <- paste0(name, "_s", i)
    single_set <- cohortSet[i, , drop = FALSE]
    cdm_schema_str <- normalize_schema_str(attr(cdm, "cdm_schema"))
    target_dialect <- switch(db,
      "duckdb"     = "duckdb",
      "sqlserver"  = "sql server",
      "sql server" = "sql server",
      "postgresql" = "postgresql",
      "postgres"   = "postgresql",
      "redshift"   = "redshift",
      "snowflake"  = "snowflake",
      "spark"      = "spark",
      "bigquery"   = "bigquery",
      "oracle"     = "oracle",
      db
    )
    single_prefix <- atlas_unique_prefix()
    single_sql <- atlas_json_to_sql_batch(json_inputs = single_set,
                                          cdm_schema = cdm_schema_str,
                                          results_schema = results_schema_str,
                                          target_dialect = target_dialect,
                                          optimize = TRUE,
                                          table_prefix = single_prefix)
    # Resolve @-params same as generateCohortSet2
    single_sql <- stringr::str_replace_all(single_sql, "@vocabulary_database_schema", cdm_schema_str)
    single_sql <- stringr::str_replace_all(single_sql, "@target_database_schema", results_schema_str)
    single_sql <- stringr::str_replace_all(single_sql, "@target_cohort_table", single_name)
    single_sql <- stringr::str_replace_all(single_sql, "@cdm_database_schema", cdm_schema_str)
    single_sql <- stringr::str_replace_all(single_sql, "@results_schema", results_schema_str)
    single_sql <- render(single_sql, warnOnMissingParameters = FALSE)
    single_sql <- resolve_literal_conditionals(single_sql)
    single_sql <- quote_cdm_table_refs(single_sql, con, cdm_schema_str)
    # Create target table and execute
    tryCatch(DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s.%s", results_schema_str, single_name)), error = function(e) NULL)
    DBI::dbExecute(con, sprintf("CREATE TABLE %s.%s (cohort_definition_id INTEGER NOT NULL, subject_id INTEGER NOT NULL, cohort_start_date DATE NOT NULL, cohort_end_date DATE NOT NULL)",
                                results_schema_str, single_name))
    stmts <- split_sql_core(single_sql)
    for (stmt in stmts) {
      tryCatch(DBI::dbExecute(con, stmt), error = function(e) {
        message("Single-cohort stmt failed for cohort ", single_set$cohort_definition_id[1], ": ", conditionMessage(e))
      })
    }
    single_dfs[[i]] <- tryCatch(DBI::dbGetQuery(con, sprintf("SELECT * FROM %s.%s", results_schema_str, single_name)), error = function(e) data.frame())
    tryCatch(DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s.%s", results_schema_str, single_name)), error = function(e) NULL)
    tryCatch(drop_prefixed_tables(con, results_schema_str, single_prefix), error = function(e) NULL)
  }
  single_df <- do.call(rbind, single_dfs)
  single_df <- single_df[order(single_df$cohort_definition_id, single_df$subject_id, single_df$cohort_start_date), ]
  rownames(single_df) <- NULL

  # Cleanup batch table
  tryCatch(DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s.%s", results_schema_str, batch_name)), error = function(e) NULL)

  # Compare
  all_ok <- TRUE
  all_cids <- sort(unique(c(batch_df$cohort_definition_id, single_df$cohort_definition_id)))
  for (cid in all_cids) {
    b <- batch_df[batch_df$cohort_definition_id == cid, ]
    s <- single_df[single_df$cohort_definition_id == cid, ]
    rownames(b) <- NULL; rownames(s) <- NULL
    # Compare by key columns (character coercion for type-safe comparison)
    key_cols <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
    b_key <- do.call(paste, lapply(b[key_cols], as.character))
    s_key <- do.call(paste, lapply(s[key_cols], as.character))
    in_batch_only <- sum(!b_key %in% s_key)
    in_single_only <- sum(!s_key %in% b_key)
    if (in_batch_only > 0 || in_single_only > 0 || nrow(b) != nrow(s)) {
      all_ok <- FALSE
      message(sprintf("MISMATCH cohort %d: batch=%d rows, single=%d rows", cid, nrow(b), nrow(s)))
      if (in_batch_only > 0) message(sprintf("  In batch only: %d rows", in_batch_only))
      if (in_single_only > 0) message(sprintf("  In single only: %d rows", in_single_only))
    } else {
      message(sprintf("OK cohort %d: %d rows match", cid, nrow(b)))
    }
  }
  if (all_ok) message("\nAll cohorts match.")
  else message("\nEquivalence check FAILED for one or more cohorts.")
  invisible(all_ok)
}

# Batch cohort SQL builder: generate optimized batch SQL directly from cohort objects
# (no temp files, no regex over generated SQL). See docs/OPTIMIZE_AT_GENERATION_VS_POST.md

# Map criteria type (from extract_criteria) to CDM table name used by optimizer
CRITERIA_TYPE_TO_CDM_TABLE <- c(
  ConditionOccurrence = "CONDITION_OCCURRENCE",
  DrugExposure = "DRUG_EXPOSURE",
  ProcedureOccurrence = "PROCEDURE_OCCURRENCE",
  Observation = "OBSERVATION",
  Measurement = "MEASUREMENT",
  DeviceExposure = "DEVICE_EXPOSURE",
  VisitOccurrence = "VISIT_OCCURRENCE",
  ObservationPeriod = "OBSERVATION_PERIOD",
  Death = "DEATH",
  ConditionEra = "CONDITION_ERA",
  DrugEra = "DRUG_ERA",
  DoseEra = "DOSE_ERA",
  Specimen = "SPECIMEN",
  VisitDetail = "VISIT_DETAIL",
  PayerPlanPeriod = "PAYER_PLAN_PERIOD",
  LocationRegion = "LOCATION_REGION"
)

#' Recursively collect criteria types (domains) from a criteria group.
#' Uses list accumulator internally to avoid O(n^2) vector growth when
#' called repeatedly across many cohorts.
#' @param group List - criteria group with CriteriaList / Groups
#' @param acc List accumulator (internal, for recursive calls)
#' @return Character vector of criteria type names
#' @keywords internal
#' @noRd
collect_criteria_types_from_group <- function(group, acc = NULL) {
  is_top <- is.null(acc)
  if (is_top) acc <- new.env(parent = emptyenv())
  if (is_top) { acc$items <- vector("list", 32L); acc$n <- 0L }

  if (!is.list(group)) {
    if (is_top) return(unlist(acc$items[seq_len(acc$n)]))
    return(invisible(NULL))
  }
  criteria_list <- get_key(group, c("CriteriaList", "criteriaList")) %||% list()
  for (cr in criteria_list) {
    ext <- extract_criteria(cr)
    if (!is.null(ext)) {
      acc$n <- acc$n + 1L
      if (acc$n > length(acc$items)) acc$items <- c(acc$items, vector("list", length(acc$items)))
      acc$items[[acc$n]] <- ext$type
      # VisitType filter generates JOIN to VISIT_OCCURRENCE
      vt <- ext$data$VisitType %||% ext$data$visitType
      vtc <- ext$data$VisitTypeCS %||% ext$data$visitTypeCS
      if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId %||% vtc$codesetId))) {
        acc$n <- acc$n + 1L
        if (acc$n > length(acc$items)) acc$items <- c(acc$items, vector("list", length(acc$items)))
        acc$items[[acc$n]] <- "VisitOccurrence"
      }
      # Recurse into CorrelatedCriteria (nested criteria within a criterion)
      corr <- ext$data$CorrelatedCriteria %||% ext$data$correlatedCriteria
      if (!is.null(corr))
        collect_criteria_types_from_group(corr, acc)
    }
  }
  groups <- get_key(group, c("Groups", "groups")) %||% list()
  for (g in groups) {
    collect_criteria_types_from_group(g, acc)
  }
  if (is_top) return(unlist(acc$items[seq_len(acc$n)]))
  invisible(NULL)
}

#' Collect CDM table names used by cohort objects (for ##*_FILTERED)
#'
#' @param cohort_list List of cohort expression lists.
#' @return Character vector of CDM table names (e.g. "DRUG_EXPOSURE").
#' @keywords internal
#' @noRd
collect_batch_used_domains_from_cohorts <- function(cohort_list) {
  # Use shared environment accumulator across all cohorts to avoid O(n^2) growth
  acc <- new.env(parent = emptyenv())
  acc$items <- vector("list", 64L)
  acc$n <- 0L

  for (cohort in cohort_list) {
    pc <- get_key(cohort, c("PrimaryCriteria", "primaryCriteria"))
    if (!is.null(pc)) {
      collect_criteria_types_from_group(pc, acc)
      add_criteria <- get_key(cohort, c("AdditionalCriteria", "additionalCriteria"))
      if (!is.null(add_criteria) && !is.null(add_criteria$CriteriaList %||% add_criteria$criteriaList %||% add_criteria$Groups %||% add_criteria$groups))
        collect_criteria_types_from_group(add_criteria, acc)
    }
    for (rule in (get_key(cohort, c("InclusionRules", "inclusionRules")) %||% list())) {
      expr <- get_key(rule, c("expression", "Expression"))
      if (!is.null(expr)) collect_criteria_types_from_group(expr, acc)
    }
    for (cc in (get_key(cohort, c("CensoringCriteria", "censoringCriteria")) %||% list())) {
      ext <- extract_criteria(cc)
      if (!is.null(ext)) {
        acc$n <- acc$n + 1L
        if (acc$n > length(acc$items)) acc$items <- c(acc$items, vector("list", length(acc$items)))
        acc$items[[acc$n]] <- ext$type
      }
    }
  }
  types <- unlist(acc$items[seq_len(acc$n)])
  tables <- unique(c(CRITERIA_TYPE_TO_CDM_TABLE[unique(types)], "OBSERVATION_PERIOD"))
  tables <- tables[!is.na(tables)]
  sort(unique(tables))
}


#' Build optimized batch cohort SQL using the execution DAG
#'
#' Builds an execution graph (DAG) from cohort definitions, deduplicates shared
#' computation nodes, and emits a single SQL script with schema-qualified
#' prefixed working tables (no temp tables).
#'
#' When \code{cache = TRUE} and a DBI connection is provided, the function
#' checks a persistent cache registry for previously computed nodes.  Nodes
#' whose content hash already exists in the cache are skipped, and only new
#' or changed nodes are materialized.
#'
#' @param cohort_list List of cohort expression lists (from cohortExpressionFromJson).
#' @param cohort_ids Integer vector, same length as cohort_list.
#' @param options List with cdm_schema, vocabulary_schema, results_schema, table_prefix.
#' @param cache Logical; if TRUE, enable incremental caching (default FALSE).
#' @param con DBI connection; required when \code{cache = TRUE}.
#' @param schema Character resolved schema name; required when \code{cache = TRUE}.
#' @return When \code{cache = FALSE}: single SQL string.
#'   When \code{cache = TRUE}: list with \code{sql}, \code{dag}, \code{cache_hits}, \code{cache_misses}.
#' @keywords internal
#' @noRd
buildBatchCohortQuery <- function(cohort_list, cohort_ids, options = list(),
                                  cache = FALSE, con = NULL, schema = NULL) {
  if (length(cohort_list) == 0) stop("cohort_list must not be empty")
  if (length(cohort_ids) != length(cohort_list)) stop("cohort_ids must match length of cohort_list")

  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  results_schema <- options$results_schema %||% "@results_schema"
  vocab_schema <- options$vocabulary_schema %||% "@vocabulary_database_schema"

  if (isTRUE(cache)) {
    if (is.null(con)) stop("DBI connection 'con' is required when cache = TRUE")
    if (is.null(schema)) stop("Resolved schema name 'schema' is required when cache = TRUE")
    table_prefix <- CACHE_TABLE_PREFIX
  } else {
    table_prefix <- options$table_prefix %||% atlas_unique_prefix()
  }

  dag_options <- list(
    cdm_schema = cdm_schema,
    results_schema = results_schema,
    vocabulary_schema = vocab_schema,
    table_prefix = table_prefix,
    cdm_table_sql = options$cdm_table_sql
  )

  dag <- build_execution_dag(cohort_list, cohort_ids, dag_options)

  if (isTRUE(cache)) {
    ensure_cache_registry(con, schema)
    result <- emit_dag_sql_cached(dag, dag_options, con, schema)
    result$dag <- dag
    result$options <- dag_options
    return(result)
  }

  emit_dag_sql(dag, dag_options)
}
