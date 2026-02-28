# translate_lightweight.R
# Lightweight SQL dialect translator for DAG-generated cohort SQL.
#
# Handles only the SQL Server patterns emitted by the cohort generator.
# For supported dialects (DuckDB, PostgreSQL), performs direct stringi-based
# transforms that are 10-50x faster than SqlRender's general-purpose translator.
# For unsupported dialects, falls back to per-statement pure-R translation
# which avoids the O(n^2) batch-scaling issue of SqlRender::translate().

# ===========================================================================
# Main entry point
# ===========================================================================

#' Translate pre-split cohort SQL statements to target dialect.
#'
#' Lightweight translator that handles only the SQL Server patterns emitted
#' by the DAG-based cohort generator.  For DuckDB and PostgreSQL, uses fast
#' stringi-based transforms.  For other dialects, falls back to per-statement
#' pure-R translation (each statement is tiny, so tokenization is cheap and
#' we avoid the O(n^2) scaling SqlRender has on large batched inputs).
#'
#' @param stmts Character vector of SQL statements in SQL Server dialect
#'   (already split, no trailing semicolons).
#' @param target_dialect Target SQL dialect name (e.g. \code{"duckdb"},
#'   \code{"postgresql"}, \code{"sql server"}).
#' @return Character vector of translated statements.
#' @noRd
translate_cohort_stmts <- function(stmts, target_dialect) {
  if (is.null(target_dialect)) return(stmts)

  td <- tolower(target_dialect)

  # SQL Server: SQL is already in this dialect, but ANALYZE hints need conversion.
  # The regex must handle optional leading comment lines (e.g. "-- ANALYZE hints...\nANALYZE tbl")
  # since the SQL splitter can merge comment lines with the following statement.
  if (td == "sql server") {
    analyze_idx <- grepl("\\bANALYZE\\b", stmts, ignore.case = TRUE, perl = TRUE)
    if (any(analyze_idx)) {
      stmts[analyze_idx] <- sub("(?im)^ANALYZE\\b", "UPDATE STATISTICS",
                                 stmts[analyze_idx], perl = TRUE)
    }
    return(stmts)
  }

  # Fast path for DuckDB only (lightweight stringi-based translator).
  # All other dialects use the full SqlRender pattern engine for correctness.
  if (td == "duckdb") return(.translate_duckdb(stmts))

  # Per-statement SqlRender-based translation for all non-DuckDB dialects.
  .translate_stmts_r(stmts, target_dialect)
}

# ===========================================================================
# Balanced-parenthesis utilities
# ===========================================================================

#' Extract arguments from a SQL function call, respecting nested parentheses
#' and quoted strings.
#'
#' @param sql Full SQL string.
#' @param open_pos Position of the opening \code{(} character.
#' @return List with \code{args} (character vector of trimmed arguments)
#'   and \code{close_pos} (position of the closing \code{)}).
#' @noRd
.extract_func_args <- function(sql, open_pos) {
  n <- nchar(sql)
  depth <- 1L
  pos <- open_pos + 1L
  args <- character(0)
  arg_start <- pos
  in_quote <- FALSE
  quote_char <- ""

  while (pos <= n && depth > 0L) {
    ch <- substr(sql, pos, pos)
    if (in_quote) {
      if (ch == quote_char) in_quote <- FALSE
    } else if (ch == "'" || ch == '"') {
      in_quote <- TRUE
      quote_char <- ch
    } else if (ch == "(") {
      depth <- depth + 1L
    } else if (ch == ")") {
      depth <- depth - 1L
      if (depth == 0L) {
        args <- c(args, trimws(substr(sql, arg_start, pos - 1L)))
        return(list(args = args, close_pos = pos))
      }
    } else if (ch == "," && depth == 1L) {
      args <- c(args, trimws(substr(sql, arg_start, pos - 1L)))
      arg_start <- pos + 1L
    }
    pos <- pos + 1L
  }
  # Unbalanced parens -- return what we have
  list(args = args, close_pos = pos - 1L)
}

#' Rewrite all occurrences of a named function call in a SQL string.
#'
#' Finds \code{func_name(} (case-insensitive), extracts balanced-paren
#' arguments, and replaces the entire call with \code{transformer(args)}.
#' Handles nested calls correctly via repeat-from-start semantics.
#'
#' @param sql   SQL string.
#' @param func_name Function name to match (case-insensitive, word-boundary).
#' @param transformer \code{function(args)} returning the replacement string.
#' @return Modified SQL string.
#' @noRd
.rewrite_func <- function(sql, func_name, transformer) {
  pattern <- paste0("(?i)\\b", func_name, "\\s*\\(")

  # Safety limit to prevent infinite loops on pathological input
  iter <- 0L
  repeat {
    iter <- iter + 1L
    if (iter > 500L) break
    m <- regexpr(pattern, sql, perl = TRUE)
    if (m == -1L) break

    func_start <- m[1L]
    open_paren <- func_start + attr(m, "match.length") - 1L
    result <- .extract_func_args(sql, open_paren)

    replacement <- transformer(result$args)
    sql <- paste0(
      substr(sql, 1L, func_start - 1L),
      replacement,
      substr(sql, result$close_pos + 1L, nchar(sql))
    )
  }
  sql
}

# ===========================================================================
# Dialect-specific function transformers
# ===========================================================================

# ---- DuckDB ----

#' DuckDB DATEADD: \code{DATEADD(unit, n, date)} ->
#'   \code{(date + TO_DAYS/MONTHS/YEARS(CAST(n AS INTEGER)))}
#' @noRd
.dateadd_duckdb <- function(args) {
  unit <- tolower(trimws(args[1]))
  n_expr <- args[2]
  date_expr <- args[3]
  to_fn <- switch(unit,
    "d" = , "dd" = , "day"    = "TO_DAYS",
    "m" = , "mm" = , "month"  = "TO_MONTHS",
    "yy" = , "yyyy" = , "year" = "TO_YEARS",
    "second" = "TO_SECONDS",
    "minute" = "TO_MINUTES",
    "hour"   = "TO_HOURS",
    "TO_DAYS"   # fallback
  )
  paste0("(", date_expr, " + ", to_fn, "(CAST(", n_expr, " AS INTEGER)))")
}

#' DuckDB DATEDIFF: \code{DATEDIFF(unit, start, end)} -> date arithmetic
#' @noRd
.datediff_duckdb <- function(args) {
  unit <- tolower(trimws(args[1]))
  s <- args[2]; e <- args[3]
  switch(unit,
    "d" = , "dd" = , "day" =
      paste0("(CAST(", e, " AS DATE) - CAST(", s, " AS DATE))"),
    "yy" = , "yyyy" = , "year" =
      paste0("(EXTRACT(YEAR FROM CAST(", e, " AS DATE)) - EXTRACT(YEAR FROM CAST(", s, " AS DATE)))"),
    "m" = , "mm" = , "month" =
      paste0("(extract(year from age(CAST(", e, " AS DATE), CAST(", s, " AS DATE)))*12 + extract(month from age(CAST(", e, " AS DATE), CAST(", s, " AS DATE))))"),
    # fallback: day
    paste0("(CAST(", e, " AS DATE) - CAST(", s, " AS DATE))")
  )
}

#' DuckDB DATEFROMPARTS: \code{DATEFROMPARTS(y,m,d)} -> string concat :: DATE
#' @noRd
.datefromparts_duckdb <- function(args) {
  y <- args[1]; m <- args[2]; d <- args[3]
  paste0("(CAST(", y, " AS VARCHAR) || '-' || CAST(", m, " AS VARCHAR) || '-' || CAST(", d, " AS VARCHAR)) :: DATE")
}

#' DuckDB EOMONTH: \code{EOMONTH(date)} -> end of month via DATE_TRUNC
#' @noRd
.eomonth_duckdb <- function(args) {
  date_expr <- args[1]
  paste0("(DATE_TRUNC('MONTH', ", date_expr, ") + INTERVAL '1 MONTH' - INTERVAL '1 day')::DATE")
}

# ---- PostgreSQL ----

#' PostgreSQL DATEADD: \code{DATEADD(unit, n, date)} -> interval arithmetic
#' @noRd
.dateadd_postgresql <- function(args) {
  unit <- tolower(trimws(args[1]))
  n_expr <- args[2]
  date_expr <- args[3]
  interval_unit <- switch(unit,
    "d" = , "dd" = , "day"    = "day",
    "m" = , "mm" = , "month"  = "month",
    "yy" = , "yyyy" = , "year" = "year",
    "day"   # fallback
  )
  paste0("(CAST(", date_expr, " AS DATE) + (", n_expr, ") * INTERVAL '1 ", interval_unit, "')")
}

#' PostgreSQL DATEDIFF (same as DuckDB for the units we support)
#' @noRd
.datediff_postgresql <- .datediff_duckdb

#' PostgreSQL DATEFROMPARTS: \code{DATEFROMPARTS(y,m,d)} -> make_date()
#' @noRd
.datefromparts_postgresql <- function(args) {
  paste0("make_date(CAST(", args[1], " AS INTEGER), CAST(", args[2], " AS INTEGER), CAST(", args[3], " AS INTEGER))")
}

#' PostgreSQL EOMONTH: same as DuckDB
#' @noRd
.eomonth_postgresql <- .eomonth_duckdb

# ---- Shared / cross-dialect ----

#' ISNULL(a, b) -> COALESCE(a, b)
#' @noRd
.isnull_to_coalesce <- function(args) {
  paste0("COALESCE(", paste(args, collapse = ", "), ")")
}

#' IIF(cond, true, false) -> CASE WHEN cond THEN true ELSE false END
#' @noRd
.iif_to_case <- function(args) {
  paste0("CASE WHEN ", args[1], " THEN ", args[2], " ELSE ", args[3], " END")
}

# ===========================================================================
# Vectorized function rewriting
# ===========================================================================

#' Rewrite DATEADD, DATEDIFF, DATEFROMPARTS, ISNULL, EOMONTH, IIF in a
#' character vector of SQL statements.  Only processes statements that
#' actually contain the function name (fast vectorized pre-filter).
#' @noRd
.rewrite_functions_vec <- function(stmts, dialect) {
  dateadd_fn      <- if (dialect == "duckdb") .dateadd_duckdb      else .dateadd_postgresql
  datediff_fn     <- if (dialect == "duckdb") .datediff_duckdb     else .datediff_postgresql
  datefromparts_fn <- if (dialect == "duckdb") .datefromparts_duckdb else .datefromparts_postgresql
  eomonth_fn      <- if (dialect == "duckdb") .eomonth_duckdb      else .eomonth_postgresql

  # Pre-filter: vectorised stri_detect is much cheaper than per-stmt regex
  has_datediff      <- stringi::stri_detect_fixed(stmts, "DATEDIFF",      case_insensitive = TRUE)
  has_dateadd       <- stringi::stri_detect_fixed(stmts, "DATEADD",       case_insensitive = TRUE)
  has_datefromparts <- stringi::stri_detect_fixed(stmts, "DATEFROMPARTS", case_insensitive = TRUE)
  has_isnull        <- stringi::stri_detect_fixed(stmts, "ISNULL",        case_insensitive = TRUE)
  has_eomonth       <- stringi::stri_detect_fixed(stmts, "EOMONTH",       case_insensitive = TRUE)
  has_iif           <- stringi::stri_detect_fixed(stmts, "IIF",           case_insensitive = TRUE)

  # Process DATEDIFF before DATEADD (DATEADD args can contain DATEDIFF)
  for (i in which(has_datediff))      stmts[i] <- .rewrite_func(stmts[i], "DATEDIFF",      datediff_fn)
  for (i in which(has_dateadd))       stmts[i] <- .rewrite_func(stmts[i], "DATEADD",       dateadd_fn)
  for (i in which(has_datefromparts)) stmts[i] <- .rewrite_func(stmts[i], "DATEFROMPARTS", datefromparts_fn)
  for (i in which(has_isnull))        stmts[i] <- .rewrite_func(stmts[i], "ISNULL",        .isnull_to_coalesce)
  for (i in which(has_eomonth))       stmts[i] <- .rewrite_func(stmts[i], "EOMONTH",       eomonth_fn)
  for (i in which(has_iif))           stmts[i] <- .rewrite_func(stmts[i], "IIF",           .iif_to_case)

  stmts
}

# ===========================================================================
# SELECT INTO -> CREATE TABLE AS
# ===========================================================================

#' Transform \code{SELECT ... INTO tablename FROM ...} to
#' \code{CREATE TABLE tablename AS SELECT ... FROM ...}.
#'
#' Handles both plain SELECT INTO and WITH ... SELECT INTO.
#' Correctly skips INSERT INTO (which is a different pattern).
#' @noRd
.transform_select_into <- function(stmt) {
  # Quick bailout (most statements have no INTO)
  if (!stringi::stri_detect_fixed(stmt, "INTO", case_insensitive = TRUE)) return(stmt)

  # Find "INTO <tablename>" -- table can be schema.table with dots, quotes, brackets
  m <- regexpr("(?i)\\bINTO\\s+([A-Za-z0-9_.\"\\[\\]]+)", stmt, perl = TRUE)
  if (m == -1L) return(stmt)

  into_start <- m[1L]
  into_match_len <- attr(m, "match.length")
  matched_text <- substr(stmt, into_start, into_start + into_match_len - 1L)

  # Extract table name
  table_name <- trimws(sub("(?i)^INTO\\s+", "", matched_text, perl = TRUE))

  # Verify: this is SELECT INTO, not INSERT INTO
  before <- substr(stmt, 1L, into_start - 1L)
  if (grepl("\\bINSERT\\s*$", before, ignore.case = TRUE, perl = TRUE)) return(stmt)
  if (!grepl("\\bSELECT\\b", before, ignore.case = TRUE, perl = TRUE)) return(stmt)

  # Remove "INTO tablename" from the statement, prepend CREATE TABLE AS
  after_into <- substr(stmt, into_start + into_match_len, nchar(stmt))
  new_select <- paste0(trimws(before), " ", trimws(after_into))
  paste0("CREATE TABLE ", table_name, " AS\n", trimws(new_select))
}

# ===========================================================================
# DuckDB full translator
# ===========================================================================

.translate_duckdb <- function(stmts) {
  # Phase 0: Strip ANALYZE hints entirely (DuckDB auto-analyzes, no action needed)
  # Blank entire statements that contain ANALYZE (including any leading comment lines)
  analyze_idx <- stringi::stri_detect_regex(stmts, "(?i)\\bANALYZE\\s+[A-Za-z]")
  stmts[analyze_idx] <- ""

  # Phase 1: Function rewrites (balanced-paren aware, per-statement)
  stmts <- .rewrite_functions_vec(stmts, "duckdb")

  # Phase 2: Simple fixed substitutions (fully vectorised across all stmts)
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bCOUNT_BIG\\s*\\(", "COUNT(")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bGETDATE\\s*\\(\\s*\\)", "CURRENT_DATE")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bTRY_CAST\\s*\\(", "CAST(")

  # Phase 3: TRUNCATE TABLE -> DELETE FROM
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bTRUNCATE\\s+TABLE\\b", "DELETE FROM")

  # Phase 4: Type-name substitutions (word-boundary safe)
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bVARCHAR\\s*\\(\\s*MAX\\s*\\)", "TEXT")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bDATETIME2?\\b", "TIMESTAMP")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bFLOAT\\b", "NUMERIC")

  # Phase 5: Date-part functions  YEAR/MONTH/DAY -> with CAST(AS DATE)
  stmts <- .rewrite_datepart_funcs_duckdb(stmts)

  # Phase 6: SELECT INTO -> CREATE TABLE AS (structural, per-statement)
  # Must come after type subs so DATETIME in "CAST(x AS DATETIME)" is already TIMESTAMP
  stmts <- vapply(stmts, .transform_select_into, character(1), USE.NAMES = FALSE)

  stmts
}

#' Rewrite standalone YEAR(), MONTH(), DAY() to include CAST(AS DATE).
#' DuckDB requires the argument to be DATE-typed for these functions.
#' @noRd
.rewrite_datepart_funcs_duckdb <- function(stmts) {
  # YEAR(x) -> YEAR(CAST(x AS DATE)), etc.
  # Only process statements that contain these functions
  has_year  <- stringi::stri_detect_regex(stmts, "(?i)\\bYEAR\\s*\\(")
  has_month <- stringi::stri_detect_regex(stmts, "(?i)\\bMONTH\\s*\\(")
  has_day   <- stringi::stri_detect_regex(stmts, "(?i)\\bDAY\\s*\\(")

  # Factory to avoid closure-capture-of-loop-variable issue
  make_datepart_transformer <- function(fn_name) {
    force(fn_name)
    function(args) {
      arg <- args[1]
      # Don't double-wrap if already CAST(... AS DATE)
      if (grepl("(?i)CAST\\s*\\(.+AS\\s+DATE\\s*\\)", arg, perl = TRUE))
        return(paste0(fn_name, "(", arg, ")"))
      paste0(fn_name, "(CAST(", arg, " AS DATE))")
    }
  }

  for (fn in c("YEAR", "MONTH", "DAY")) {
    idx <- switch(fn, YEAR = which(has_year), MONTH = which(has_month), DAY = which(has_day))
    if (length(idx) == 0L) next
    transformer <- make_datepart_transformer(fn)
    for (i in idx) stmts[i] <- .rewrite_func(stmts[i], fn, transformer)
  }
  stmts
}

# ===========================================================================
# PostgreSQL full translator
# ===========================================================================

.translate_postgresql <- function(stmts) {
  # Phase 1: Function rewrites
  stmts <- .rewrite_functions_vec(stmts, "postgresql")

  # Phase 2: Simple fixed substitutions
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bCOUNT_BIG\\s*\\(", "COUNT(")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bGETDATE\\s*\\(\\s*\\)", "CURRENT_DATE")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bTRY_CAST\\s*\\(", "CAST(")

  # Phase 3: TRUNCATE TABLE -> DELETE FROM
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bTRUNCATE\\s+TABLE\\b", "DELETE FROM")

  # Phase 4: Type-name substitutions
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bVARCHAR\\s*\\(\\s*MAX\\s*\\)", "TEXT")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bDATETIME2?\\b", "TIMESTAMP")
  stmts <- stringi::stri_replace_all_regex(stmts, "(?i)\\bFLOAT\\b", "NUMERIC")

  # Phase 5: SELECT INTO -> CREATE TABLE AS
  stmts <- vapply(stmts, .transform_select_into, character(1), USE.NAMES = FALSE)

  stmts
}

# ===========================================================================
# Fallback: per-statement pure-R translation
# ===========================================================================

#' Translate statements one at a time via the pure-R pattern engine
#' (translate_sql_with_path).  Each statement is small (~200 chars)
#' so tokenization is fast.  Avoids the O(n^2) batch-scaling issue.
#' @noRd
.translate_stmts_r <- function(stmts, target_dialect) {
  td <- tolower(target_dialect)

  # Pre-processing: convert ANALYZE hints to dialect-appropriate form
  # before SqlRender (which doesn't handle ANALYZE)
  # Use \b anchor (not ^ anchor) to match ANALYZE even when preceded by comment lines
  analyze_idx <- grepl("\\bANALYZE\\b", stmts, ignore.case = TRUE, perl = TRUE)
  if (any(analyze_idx)) {
    if (td %in% c("sql server", "sqlserver")) {
      # SQL Server: ANALYZE tbl -> UPDATE STATISTICS tbl
      stmts[analyze_idx] <- sub("(?im)^ANALYZE\\b", "UPDATE STATISTICS",
                                 stmts[analyze_idx], perl = TRUE)
    } else if (td %in% c("oracle")) {
      # Oracle: strip ANALYZE (DBMS_STATS is PL/SQL, too complex for inline)
      stmts[analyze_idx] <- ""
    }
    # PostgreSQL, Redshift: ANALYZE is native, keep as-is
    # Spark/Databricks: ANALYZE tbl -> ANALYZE TABLE tbl COMPUTE STATISTICS
    if (td %in% c("spark", "databricks")) {
      stmts[analyze_idx] <- paste0(
        sub("(?i)\\bANALYZE\\s+", "ANALYZE TABLE ", stmts[analyze_idx], perl = TRUE),
        " COMPUTE STATISTICS"
      )
    }
    # Snowflake, BigQuery: auto-analyze, strip
    if (td %in% c("snowflake", "bigquery")) {
      stmts[analyze_idx] <- ""
    }
  }

  # Spark/Databricks: strip NULL/NOT NULL constraints from CREATE TABLE DDL
  # (Spark SQL parser rejects explicit NULL/NOT NULL in column definitions)
  if (td %in% c("spark", "databricks")) {
    create_idx <- grepl("^\\s*CREATE\\s+TABLE\\b", stmts, ignore.case = TRUE, perl = TRUE)
    if (any(create_idx)) {
      stmts[create_idx] <- gsub("\\s+NOT\\s+NULL\\b", "", stmts[create_idx],
                                 ignore.case = TRUE, perl = TRUE)
      stmts[create_idx] <- gsub("\\s+NULL\\b", "", stmts[create_idx],
                                 ignore.case = TRUE, perl = TRUE)
    }
  }

  # Pre-processing: SELECT INTO -> CREATE TABLE AS for dialects where SqlRender
  # patterns can fail (e.g., Snowflake WITH...SELECT INTO). Skip for Spark/Databricks
  # because SqlRender adds USING DELTA and handles the full conversion correctly.
  if (!td %in% c("sql server", "sqlserver", "spark", "databricks")) {
    stmts <- vapply(stmts, .transform_select_into, character(1), USE.NAMES = FALSE)
  }

  session_id <- get_global_session_id()
  temp_prefix <- getOption("sqlRenderTempEmulationSchema", "")
  if (is.null(temp_prefix) || is.na(temp_prefix)) temp_prefix <- ""
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")

  # Identify statements that should skip SqlRender (already pre-processed):
  # - Plain CREATE TABLE DDLs with column defs (NULL stripping already done).
  #   These have '(' after the table name, NOT "AS SELECT" (which need translation).
  # - DROP TABLE IF EXISTS (no translation needed)
  is_create <- grepl("^\\s*CREATE\\s+TABLE\\b", stmts, ignore.case = TRUE, perl = TRUE)
  is_create_as <- grepl("\\bAS\\s*$|\\bAS\\s+SELECT\\b|\\bAS\\s*\\(", stmts,
                         ignore.case = TRUE, perl = TRUE)
  skip_sqlrender <- (is_create & !is_create_as) |
                    grepl("^\\s*DROP\\s+TABLE\\b", stmts, ignore.case = TRUE, perl = TRUE)

  for (i in seq_along(stmts)) {
    if (!nzchar(stmts[i]) || skip_sqlrender[i]) next
    # Add trailing ; because SqlRender patterns expect it (e.g. SELECT INTO ;)
    translated <- translate_sql_with_path(
      paste0(stmts[i], ";"),
      target_dialect, session_id, temp_prefix, path
    )
    # Strip trailing semicolons and blank lines left by translate
    stmts[i] <- sub(";\\s*$", "", translated, perl = TRUE)
  }

  # Spark/Databricks post-processing
  if (td %in% c("spark", "databricks")) {
    # 1. Re-split: SqlRender's Spark CTE decomposition produces multi-statement
    #    output (DROP VIEW; CREATE TEMPORARY VIEW; CREATE TABLE...) concatenated
    #    into a single string. ODBC can only execute one statement at a time.
    needs_split <- grepl(";", stmts, fixed = TRUE)
    if (any(needs_split)) {
      expanded <- vector("list", length(stmts))
      for (i in seq_along(stmts)) {
        if (needs_split[i]) {
          parts <- strsplit(stmts[i], ";")[[1]]
          parts <- trimws(parts)
          parts <- parts[nzchar(parts)]
          expanded[[i]] <- parts
        } else {
          expanded[[i]] <- stmts[i]
        }
      }
      stmts <- unlist(expanded, use.names = FALSE)
    }

    # 2. Strip SQL line comments (-- ...) which break when ODBC collapses
    #    multi-line SQL into a single line, causing -- to comment out the
    #    rest of the entire statement.
    stmts <- gsub("--[^\n]*", "", stmts, perl = TRUE)
  }

  # Snowflake/Databricks: domain-filtered tables (condition_occurrence_filtered, etc.)
  # are created with lowercase column names. Quote alias.column refs so they resolve.
  # Domain aliases from DOMAIN_CONFIG: co, de, po, o, m, d, v.
  # IMPORTANT: Skip the statements that CREATE the filtered tables — those read from
  # the CDM (uppercase columns), so quoting as lowercase would break Snowflake.
  if (td %in% c("snowflake", "databricks")) {
    domain_aliases <- c("co", "de", "po", "o", "m", "d", "v")
    is_filtered_create <- grepl("_filtered\\s+(AS\\b|FROM\\b)", stmts, ignore.case = TRUE, perl = TRUE)
    for (i in seq_along(stmts)) {
      if (is_filtered_create[i]) next
      for (al in domain_aliases) {
        # Match alias.column (lowercase column name, not already quoted)
        pattern <- paste0("\\b", al, "\\.([a-z_][a-z0-9_]*)\\b")
        replacement <- paste0(al, ".\\\"\\1\\\"")
        stmts[i] <- gsub(pattern, replacement, stmts[i], perl = TRUE)
      }
    }
  }

  stmts
}
