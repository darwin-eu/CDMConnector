# Package-internal context for DAG emit layer to set the codesets table name.
# Default is "#Codesets" for the single-cohort (non-batch) path.
# The DAG batch path sets this to a qualified table name before calling circe builders.
.sql_context <- new.env(parent = emptyenv())
.sql_context$codesets_table <- "#Codesets"

#' Parse cohort expression from JSON
#'
#' Load a cohort expression from a JSON string. Handles both camelCase and
#' PascalCase field names for Java/Atlas compatibility.
#'
#' @param json_str Character string containing cohort definition JSON
#' @return List structure representing the cohort expression (validated)
#' @keywords internal
cohort_expression_from_json <- function(json_str) {
  data <- jsonlite::fromJSON(json_str, simplifyDataFrame = FALSE)

  # Handle cdmVersionRange as string
  if (!is.null(data$cdmVersionRange) && is.character(data$cdmVersionRange)) {
    data$cdmVersionRange <- NULL
  }

  # Handle empty censorWindow
  if (!is.null(data$censorWindow) && identical(data$censorWindow, list())) {
    data$censorWindow <- NULL
  }

  # Ensure ConceptSetExpression objects have required fields
  if (!is.null(data$ConceptSets) && length(data$ConceptSets) > 0) {
    for (i in seq_along(data$ConceptSets)) {
      cs <- data$ConceptSets[[i]]
      if (is.list(cs) && !is.null(cs$expression) && is.list(cs$expression)) {
        expr <- cs$expression
        if (is.null(expr$isExcluded)) expr$isExcluded <- FALSE
        if (is.null(expr$includeMapped)) expr$includeMapped <- FALSE
        if (is.null(expr$includeDescendants)) expr$includeDescendants <- FALSE
        data$ConceptSets[[i]]$expression <- expr
      }
    }
  }

  # Normalize keys: accept both PascalCase and camelCase
  data <- normalize_cohort_keys(data)
  data
}

#' Normalize cohort JSON keys for consistent access
#' @param x List from JSON
#' @param depth Current recursion depth
#' @return List with normalized keys
#' @keywords internal
normalize_cohort_keys <- function(x, depth = 0) {
  if (depth > 50) return(x)  # Prevent infinite recursion
  if (!is.list(x) || is.data.frame(x)) return(x)
  if (length(x) == 0) return(x)
  for (i in seq_along(x)) {
    xi <- x[[i]]
    if (!is.null(xi)) {
      x[[i]] <- normalize_cohort_keys(xi, depth + 1)
    }
  }
  x
}

#' Get primary criteria from cohort expression
#' @param cohort List (cohort expression)
#' @return List or NULL
#' @keywords internal
get_primary_criteria <- function(cohort) {
  get_key(cohort, c("PrimaryCriteria", "primaryCriteria"))
}

#' Get concept sets from cohort expression
#' @param cohort List (cohort expression)
#' @return List of concept sets
#' @keywords internal
get_concept_sets <- function(cohort) {
  cs <- get_key(cohort, c("ConceptSets", "conceptSets"))
  if (is.null(cs)) list() else cs
}

#' Get observation window from primary criteria
#' @param pc List (primary criteria)
#' @return List with prior_days, post_days
#' @keywords internal
get_observation_window <- function(pc) {
  ow <- get_key(pc, c("ObservationWindow", "observationWindow"))
  if (is.null(ow)) return(list(prior_days = 0, post_days = 0))
  list(
    prior_days = get_key(ow, c("PriorDays", "priorDays"), 0) %||% 0,
    post_days = get_key(ow, c("PostDays", "postDays"), 0) %||% 0
  )
}

#' Get primary limit type from primary criteria
#' @param pc List (primary criteria)
#' @return Character "All", "First", or "Last"
#' @keywords internal
get_primary_limit_type <- function(pc) {
  pl <- get_key(pc, c("PrimaryCriteriaLimit", "PrimaryLimit", "primaryLimit", "primaryCriteriaLimit"))
  if (is.null(pl)) return("All")
  type <- get_key(pl, c("Type", "type"))
  if (is.null(type)) return("All")
  type
}

#' Get criteria list from primary criteria
#' @param pc List (primary criteria)
#' @return List of criteria items
#' @keywords internal
get_criteria_list <- function(pc) {
  get_key(pc, c("CriteriaList", "criteriaList")) %||% list()
}

#' Builder utilities for SQL query generation
#'
#' Utility functions for building SQL from cohort criteria.
#' Mirrors Java CIRCE-BE BuilderUtils.
#'
#' @name builder_utils_section
#' @keywords internal
NULL

BUILDER_UTILS <- new.env()

#' Get concept IDs from concept list
#' @param concepts List of concept objects
#' @return Integer vector of concept IDs
get_concept_ids <- function(concepts) {
  if (is.null(concepts) || length(concepts) == 0) return(integer(0))
  ids <- vapply(concepts, function(conc) {
    if (is.atomic(conc)) return(NA_integer_)
    if (!is.list(conc)) return(NA_integer_)
    id <- conc$CONCEPT_ID %||% conc$conceptId %||% conc$ConceptId
    if (is.null(id)) NA_integer_ else as.integer(id)
  }, integer(1))
  ids[!is.na(ids)]
}

#' Split IN clause for large value lists (Oracle 1000 limit)
#' @param column_name SQL column name
#' @param values Integer vector
#' @param max_length Max values per chunk
#' @return SQL expression string
split_in_clause <- function(column_name, values, max_length = 1000) {
  if (is.null(values) || length(values) == 0) return("NULL")
  chunks <- split(values, ceiling(seq_along(values) / max_length))
  clauses <- vapply(chunks, function(chunk) {
    paste0(column_name, " in (", paste(chunk, collapse = ","), ")")
  }, character(1))
  paste0("(", paste(clauses, collapse = " or "), ")")
}

#' Get SQL operator from op string
#' @param op Operator name (lt, lte, eq, gt, gte, etc.)
#' @return SQL operator string
get_operator <- function(op) {
  ops <- c(lt = "<", lte = "<=", eq = "=", "!eq" = "<>", gt = ">", gte = ">=")
  op_lower <- tolower(op)
  if (op_lower %in% names(ops)) return(ops[[op_lower]])
  stop("Unknown operator type: ", op)
}

#' Convert date string to SQL DATEFROMPARTS
#' @param date_string Date in YYYY-MM-DD format
#' @return SQL expression
date_string_to_sql <- function(date_string) {
  parts <- strsplit(date_string, "-")[[1]]
  if (length(parts) != 3) stop("Invalid date format: ", date_string, ". Expected YYYY-MM-DD.")
  paste0("DATEFROMPARTS(", as.integer(parts[1]), ", ", as.integer(parts[2]), ", ", as.integer(parts[3]), ")")
}

#' Build date range clause
#' @param sql_expression SQL column expression
#' @param date_range List with op, value, extent
#' @return SQL WHERE clause or NULL
build_date_range_clause <- function(sql_expression, date_range) {
  if (is.null(date_range) || is.null(date_range$Op) && is.null(date_range$op)) return(NULL)
  op <- tolower(date_range$Op %||% date_range$op)
  val <- date_range$Value %||% date_range$value
  if (op %in% c("bt", "!bt")) {
    extent <- date_range$Extent %||% date_range$extent
    if (is.null(val) || is.null(extent)) return(NULL)
    neg <- if (startsWith(op, "!")) "not " else ""
    paste0(neg, "(", sql_expression, " >= ", date_string_to_sql(as.character(val)),
           " and ", sql_expression, " <= ", date_string_to_sql(as.character(extent)), ")")
  } else {
    if (is.null(val)) return(NULL)
    paste0(sql_expression, " ", get_operator(op), " ", date_string_to_sql(as.character(val)))
  }
}

#' Build numeric range clause
#' @param sql_expression SQL column expression
#' @param numeric_range List with op, value, extent
#' @param format Optional format for floats
#' @return SQL WHERE clause or NULL
build_numeric_range_clause <- function(sql_expression, numeric_range, format = NULL) {
  if (is.null(numeric_range) || (is.null(numeric_range$Op) && is.null(numeric_range$op))) return(NULL)
  op <- tolower(numeric_range$Op %||% numeric_range$op)
  val <- numeric_range$Value %||% numeric_range$value
  extent <- numeric_range$Extent %||% numeric_range$extent
  if (grepl("bt$", op)) {
    if (is.null(val) || is.null(extent)) return(NULL)
    neg <- if (startsWith(op, "!")) "not " else ""
    if (!is.null(format)) {
      paste0(neg, "(", sql_expression, " >= ", sprintf(format, as.numeric(val)),
             " and ", sql_expression, " <= ", sprintf(format, as.numeric(extent)), ")")
    } else {
      paste0(neg, "(", sql_expression, " >= ", as.integer(val),
             " and ", sql_expression, " <= ", as.integer(extent), ")")
    }
  } else {
    if (is.null(val)) return(NULL)
    if (!is.null(format)) {
      paste0(sql_expression, " ", get_operator(op), " ", sprintf(format, as.numeric(val)))
    } else {
      paste0(sql_expression, " ", get_operator(op), " ", as.integer(val))
    }
  }
}

#' Build text filter clause
#' @param text_filter List with text, op or character
#' @param column_name SQL column name
#' @return SQL WHERE clause or NULL
build_text_filter_clause <- function(text_filter, column_name) {
  if (is.null(text_filter)) return(NULL)
  if (is.character(text_filter)) {
    txt <- gsub("'", "''", text_filter, fixed = TRUE)
    return(paste0(column_name, " LIKE '%", txt, "%'"))
  }
  text <- text_filter$Text %||% text_filter$text
  op <- text_filter$Op %||% text_filter$op %||% "contains"
  if (is.null(text)) return(NULL)
  txt <- gsub("'", "''", text, fixed = TRUE)
  switch(op,
    eq = paste0(column_name, " = '", txt, "'"),
    "!eq" = paste0(column_name, " <> '", txt, "'"),
    startsWith = paste0(column_name, " LIKE '", txt, "%'"),
    endsWith = paste0(column_name, " LIKE '%", txt, "'"),
    contains = paste0(column_name, " LIKE '%", txt, "%'"),
    "!contains" = paste0(column_name, " NOT LIKE '%", txt, "%'"),
    paste0(column_name, " = '", txt, "'")
  )
}

#' Get codeset join expression
#' @param standard_codeset_id Integer or NULL
#' @param standard_concept_column Column name
#' @param source_codeset_id Integer or NULL
#' @param source_concept_column Column name
#' @return SQL JOIN clause
get_codeset_join_expression <- function(standard_codeset_id, standard_concept_column,
                                         source_codeset_id, source_concept_column) {
  cs_tbl <- .sql_context$codesets_table
  clauses <- character(0)
  if (!is.null(standard_codeset_id)) {
    clauses <- c(clauses, paste0("JOIN ", cs_tbl, " cs on (", standard_concept_column,
      " = cs.concept_id and cs.codeset_id = ", standard_codeset_id, ")"))
  }
  if (!is.null(source_codeset_id)) {
    clauses <- c(clauses, paste0("JOIN ", cs_tbl, " cns on (", source_concept_column,
      " = cns.concept_id and cns.codeset_id = ", source_codeset_id, ")"))
  }
  paste(clauses, collapse = " ")
}

#' Get codeset IN expression
#' @param codeset_id Integer
#' @param column_name Column name
#' @param is_exclusion Logical
#' @return SQL expression
get_codeset_in_expression <- function(codeset_id, column_name, is_exclusion = FALSE) {
  cs_tbl <- .sql_context$codesets_table
  op <- if (is_exclusion) "not" else ""
  paste0(op, " ", column_name, " in (select concept_id from ", cs_tbl, " where codeset_id = ", codeset_id, ")")
}

#' Get codeset WHERE clause for standard-only codeset (no source concept)
#' Used by DrugEra, ConditionEra, DoseEra to match CirceR/Java output format.
#' @param codeset_id Integer or NULL
#' @param column_name Column name
#' @return "WHERE col in (...)" or ""
get_codeset_where_clause <- function(codeset_id, column_name) {
  if (is.null(codeset_id)) return("")
  paste0("WHERE ", trimws(get_codeset_in_expression(codeset_id, column_name)))
}

#' Get date adjustment expression
#' @param date_adjustment List with startOffset, endOffset, startWith, endWith
#' @param start_column Column for start
#' @param end_column Column for end
#' @return SQL expression
get_date_adjustment_expression <- function(date_adjustment, start_column, end_column) {
  start_off <- date_adjustment$startOffset %||% date_adjustment$StartOffset %||% 0
  end_off <- date_adjustment$endOffset %||% date_adjustment$EndOffset %||% 0
  paste0("DATEADD(day,", start_off, ", ", start_column, ") as start_date, DATEADD(day,",
         end_off, ", ", end_column, ") as end_date")
}

#' Build concept set expression query
#'
#' Generates SQL for a concept set expression (include/exclude, descendants, mapped).
#'
#' @param expression List - ConceptSetExpression with items
#' @param vocabulary_schema Schema for vocabulary tables (default: @vocabulary_database_schema)
#' @return Character SQL query
build_concept_set_expression_query <- function(expression, vocabulary_schema = "@vocabulary_database_schema") {
  items <- expression$items %||% list()
  if (length(items) == 0) {
    return(paste0("select concept_id from ", vocabulary_schema, ".CONCEPT where 0=1"))
  }

  # Pre-allocate lists with max possible size (each item can go into at most one bucket per category)
  n_items <- length(items)
  include_concepts <- vector("list", n_items)
  include_descendant_concepts <- vector("list", n_items)
  include_mapped_concepts <- vector("list", n_items)
  include_mapped_descendant_concepts <- vector("list", n_items)
  exclude_concepts <- vector("list", n_items)
  exclude_descendant_concepts <- vector("list", n_items)
  exclude_mapped_concepts <- vector("list", n_items)
  exclude_mapped_descendant_concepts <- vector("list", n_items)
  ni_inc <- 0L; ni_idesc <- 0L; ni_imap <- 0L; ni_imapdesc <- 0L
  ni_exc <- 0L; ni_edesc <- 0L; ni_emap <- 0L; ni_emapdesc <- 0L

  for (item in items) {
    concept <- item$concept
    is_excluded <- item$isExcluded %||% item$is_excluded %||% FALSE
    include_descendants <- item$includeDescendants %||% item$include_descendants %||% FALSE
    include_mapped <- item$includeMapped %||% item$include_mapped %||% FALSE

    if (!is_excluded) {
      ni_inc <- ni_inc + 1L; include_concepts[[ni_inc]] <- concept
      if (include_descendants) { ni_idesc <- ni_idesc + 1L; include_descendant_concepts[[ni_idesc]] <- concept }
      if (include_mapped) {
        ni_imap <- ni_imap + 1L; include_mapped_concepts[[ni_imap]] <- concept
        if (include_descendants) { ni_imapdesc <- ni_imapdesc + 1L; include_mapped_descendant_concepts[[ni_imapdesc]] <- concept }
      }
    } else {
      ni_exc <- ni_exc + 1L; exclude_concepts[[ni_exc]] <- concept
      if (include_descendants) { ni_edesc <- ni_edesc + 1L; exclude_descendant_concepts[[ni_edesc]] <- concept }
      if (include_mapped) {
        ni_emap <- ni_emap + 1L; exclude_mapped_concepts[[ni_emap]] <- concept
        if (include_descendants) { ni_emapdesc <- ni_emapdesc + 1L; exclude_mapped_descendant_concepts[[ni_emapdesc]] <- concept }
      }
    }
  }
  # Trim to actual size
  include_concepts <- include_concepts[seq_len(ni_inc)]
  include_descendant_concepts <- include_descendant_concepts[seq_len(ni_idesc)]
  include_mapped_concepts <- include_mapped_concepts[seq_len(ni_imap)]
  include_mapped_descendant_concepts <- include_mapped_descendant_concepts[seq_len(ni_imapdesc)]
  exclude_concepts <- exclude_concepts[seq_len(ni_exc)]
  exclude_descendant_concepts <- exclude_descendant_concepts[seq_len(ni_edesc)]
  exclude_mapped_concepts <- exclude_mapped_concepts[seq_len(ni_emap)]
  exclude_mapped_descendant_concepts <- exclude_mapped_descendant_concepts[seq_len(ni_emapdesc)]

  build_sub <- function(concepts, descendant_concepts, mapped_concepts, mapped_descendant_concepts) {
    queries <- character(0)
    if (length(concepts) > 0) {
      ids <- get_concept_ids(concepts)
      concept_id_in <- split_in_clause("concept_id", ids)
      queries <- c(queries, paste0("select concept_id from ", vocabulary_schema, ".CONCEPT where ", concept_id_in))
    }
    if (length(descendant_concepts) > 0) {
      ids <- get_concept_ids(descendant_concepts)
      concept_id_in <- split_in_clause("ca.ancestor_concept_id", ids)
      queries <- c(queries, paste0(
        "select c.concept_id from ", vocabulary_schema, ".CONCEPT c\n",
        "join ", vocabulary_schema, ".CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id\n",
        "WHERE c.invalid_reason is null and ", concept_id_in))
    }
    base_query <- paste(queries, collapse = "\nUNION  \n")
    if (length(mapped_concepts) > 0 || length(mapped_descendant_concepts) > 0) {
      mapped_queries <- character(0)
      if (length(mapped_concepts) > 0) {
        ids <- get_concept_ids(mapped_concepts)
        concept_id_in <- split_in_clause("concept_id", ids)
        mapped_queries <- c(mapped_queries, paste0("select concept_id from ", vocabulary_schema, ".CONCEPT where ", concept_id_in))
      }
      if (length(mapped_descendant_concepts) > 0) {
        ids <- get_concept_ids(mapped_descendant_concepts)
        concept_id_in <- split_in_clause("ca.ancestor_concept_id", ids)
        mapped_queries <- c(mapped_queries, paste0(
          "select c.concept_id from ", vocabulary_schema, ".CONCEPT c\n",
          "join ", vocabulary_schema, ".CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id\n",
          "WHERE c.invalid_reason is null and ", concept_id_in))
      }
      mapped_base <- paste(mapped_queries, collapse = "\nUNION  \n")
      mapped_full <- paste0(
        "select distinct cr.concept_id_1 as concept_id FROM\n(\n  ", mapped_base, "\n) C\n",
        "join ", vocabulary_schema, ".concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL")
      base_query <- paste0(base_query, " UNION ", mapped_full)
    }
    base_query
  }

  if (length(include_concepts) == 0 && length(include_descendant_concepts) == 0) {
    return(paste0("select concept_id from ", vocabulary_schema, ".CONCEPT where 0=1"))
  }

  include_query <- build_sub(include_concepts, include_descendant_concepts,
                             include_mapped_concepts, include_mapped_descendant_concepts)
  result <- paste0("select distinct I.concept_id FROM (\n  ", include_query, "\n) I")

  if (length(exclude_concepts) > 0 || length(exclude_descendant_concepts) > 0) {
    exclude_query <- build_sub(exclude_concepts, exclude_descendant_concepts,
                               exclude_mapped_concepts, exclude_mapped_descendant_concepts)
    result <- paste0(result, "\nLEFT JOIN (\n  ", exclude_query, "\n) E ON I.concept_id = E.concept_id\nWHERE E.concept_id is null")
  }
  result
}
#' SQL builders for cohort criteria
#'
#' Generates SQL for each OMOP CDM domain criteria type.
#'
#' @name sql_builders_section
#' @keywords internal
NULL

#' Extract criteria type and data from polymorphic criteria structure
#' @param item List - either named by type (ConditionOccurrence=...) or in Criteria/criteria
#' @return List with type, data
extract_criteria <- function(item) {
  if (!is.list(item)) return(NULL)
  # Primary criteria format: list(ConditionOccurrence = list(...))
  type_names <- c("ConditionOccurrence", "DrugExposure", "ProcedureOccurrence",
    "VisitOccurrence", "Observation", "Measurement", "DeviceExposure", "Specimen",
    "Death", "VisitDetail", "ObservationPeriod", "PayerPlanPeriod", "LocationRegion",
    "ConditionEra", "DrugEra", "DoseEra")
  for (tn in type_names) {
    if (!is.null(item[[tn]])) {
      data <- item[[tn]]
      if (!is.list(data)) data <- list()
      # Ensure First default
      if (is.null(data$First) && is.null(data$first)) data$First <- FALSE
      return(list(type = tn, data = data))
    }
  }
  # Correlated criteria: list(Criteria = list(ConditionOccurrence = ...))
  inner <- item$Criteria %||% item$criteria
  if (is.list(inner)) return(extract_criteria(inner))
  NULL
}

#' Get codeset join for criteria with codeset_id
#' @param criteria List
#' @param standard_col Standard concept column
#' @param source_col Source concept column (optional)
#' @return SQL fragment
get_codeset_clause <- function(criteria, standard_col, source_col = NULL) {
  codeset_id <- criteria$CodesetId %||% criteria$codesetId
  source_id <- criteria$ConditionSourceConcept %||% criteria$conditionSourceConcept %||%
    criteria$DrugSourceConcept %||% criteria$drugSourceConcept %||%
    criteria$ProcedureSourceConcept %||% criteria$procedureSourceConcept %||%
    criteria$VisitSourceConcept %||% criteria$visitSourceConcept %||%
    criteria$MeasurementSourceConcept %||% criteria$measurementSourceConcept %||%
    criteria$ObservationSourceConcept %||% criteria$observationSourceConcept %||%
    criteria$DeviceSourceConcept %||% criteria$deviceSourceConcept %||%
    criteria$SpecimenSourceConcept %||% criteria$specimenSourceConcept %||%
    criteria$DeathSourceConcept %||% criteria$deathSourceConcept %||%
    criteria$CauseSourceConcept %||% criteria$causeSourceConcept
  if (is.null(source_col)) source_id <- NULL
  get_codeset_join_expression(codeset_id, standard_col, source_id, source_col)
}

#' Add ProviderSpecialty filter support to a domain builder
#' @return list with select_col (to add to inner SELECT), join_sql, where_parts
add_provider_specialty_filter <- function(criteria, cdm_schema, alias_prefix) {
  ps <- criteria$ProviderSpecialty %||% criteria$providerSpecialty
  psc <- criteria$ProviderSpecialtyCS %||% criteria$providerSpecialtyCS
  result <- list(select_col = NULL, join_sql = NULL, where_parts = character(0))
  if (length(ps) > 0 || (!is.null(psc) && !is.null(psc$CodesetId %||% psc$codesetId))) {
    result$select_col <- paste0(alias_prefix, ".provider_id")
    result$join_sql <- paste0("LEFT JOIN ", cdm_schema, ".PROVIDER PR on C.provider_id = PR.provider_id")
    if (length(ps) > 0) {
      ids <- get_concept_ids(ps)
      if (length(ids) > 0)
        result$where_parts <- c(result$where_parts, paste0("PR.specialty_concept_id in (", paste(ids, collapse = ","), ")"))
    }
    if (!is.null(psc) && !is.null(psc$CodesetId %||% psc$codesetId))
      result$where_parts <- c(result$where_parts, get_codeset_in_expression(psc$CodesetId %||% psc$codesetId, "PR.specialty_concept_id", psc$IsExclusion %||% psc$isExclusion %||% FALSE))
  }
  result
}

#' Build Condition Occurrence SQL
build_condition_occurrence_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "co.condition_concept_id", "co.condition_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY co.person_id ORDER BY co.condition_start_date, co.condition_occurrence_id) as ordinal" else ""
  select_clause <- "co.person_id, co.condition_occurrence_id, co.condition_concept_id, co.visit_occurrence_id"
  date_adj <- criteria$DateAdjustment %||% criteria$dateAdjustment
  if (!is.null(date_adj)) {
    start_col <- if ((date_adj$StartWith %||% date_adj$startWith) == "start_date") "co.condition_start_date" else "COALESCE(co.condition_end_date, DATEADD(day,1,co.condition_start_date))"
    end_col <- if ((date_adj$EndWith %||% date_adj$endWith) == "start_date") "co.condition_start_date" else "COALESCE(co.condition_end_date, DATEADD(day,1,co.condition_start_date))"
    select_clause <- paste0(select_clause, ", ", get_date_adjustment_expression(date_adj, start_col, end_col))
  } else {
    select_clause <- paste0(select_clause, ", co.condition_start_date as start_date, COALESCE(co.condition_end_date, DATEADD(day,1,co.condition_start_date)) as end_date")
  }
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # occurrenceStartDate, occurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # conditionType
  ct <- criteria$ConditionType %||% criteria$conditionType
  ctc <- criteria$ConditionTypeCS %||% criteria$conditionTypeCS
  cte <- criteria$ConditionTypeExclude %||% criteria$conditionTypeExclude %||% FALSE
  if (length(ct) > 0) {
    ids <- get_concept_ids(ct)
    if (length(ids) > 0) {
      op <- if (cte) "not in" else "in"
      where_parts <- c(where_parts, paste0("C.condition_type_concept_id ", op, " (", paste(ids, collapse = ","), ")"))
    }
  }
  if (!is.null(ctc) && !is.null(ctc$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(ctc$CodesetId, "C.condition_type_concept_id", ctc$IsExclusion %||% FALSE))
  }
  # age, gender - need PERSON join
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || (length(gender) > 0) || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # visitType
  vt <- criteria$VisitType %||% criteria$visitType
  vtc <- criteria$VisitTypeCS %||% criteria$visitTypeCS
  if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId))) {
    join_clause <- paste0(join_clause, " JOIN ", cdm_schema, ".VISIT_OCCURRENCE V on C.visit_occurrence_id = V.visit_occurrence_id and C.person_id = V.person_id")
    if (length(vt) > 0) { ids <- get_concept_ids(vt); if (length(ids) > 0) where_parts <- c(where_parts, paste0("V.visit_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(vtc) && !is.null(vtc$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(vtc$CodesetId, "V.visit_concept_id", vtc$IsExclusion %||% FALSE))
  }
  # stopReason
  sr <- criteria$StopReason %||% criteria$stopReason
  if (!is.null(sr)) { select_clause <- paste0(select_clause, ", co.stop_reason"); cl <- build_text_filter_clause(sr, "C.stop_reason"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "co")
  if (!is.null(ps_filter$select_col)) select_clause <- paste0(select_clause, ", ", ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Condition Occurrence Criteria\nSELECT C.person_id, C.condition_occurrence_id as event_id, C.start_date, C.end_date,\n  C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".CONDITION_OCCURRENCE co\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Condition Occurrence Criteria")
}

#' Build Death SQL
build_death_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_join_expression(
    criteria$CodesetId %||% criteria$codesetId,
    "d.cause_concept_id",
    criteria$DeathSourceConcept %||% criteria$deathSourceConcept %||% criteria$CauseSourceConcept %||% criteria$causeSourceConcept,
    "d.cause_source_concept_id"
  )
  select_clause <- "d.person_id, d.cause_concept_id, d.death_date as start_date, DATEADD(day,1,d.death_date) as end_date"
  dt <- criteria$DeathType %||% criteria$deathType
  dtc <- criteria$DeathTypeCS %||% criteria$deathTypeCS
  has_dt <- (is.list(dt) && length(dt) > 0) || (!is.null(dtc) && !is.null(dtc$CodesetId))
  if (has_dt) select_clause <- paste0(select_clause, ", d.death_type_concept_id")
  join_clause <- ""
  where_parts <- character(0)
  if (is.list(dt) && length(dt) > 0) {
    ids <- get_concept_ids(dt)
    op <- if (criteria$DeathTypeExclude %||% criteria$deathTypeExclude %||% FALSE) "not in" else "in"
    where_parts <- c(where_parts, paste0("C.death_type_concept_id ", op, " (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(dtc) && !is.null(dtc$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(dtc$CodesetId, "C.death_type_concept_id"))
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || length(gender) > 0 || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id"))
  }
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Death Criteria\nSELECT C.person_id, C.person_id as event_id, C.start_date, C.end_date,\n       CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, "\n  FROM ", cdm_schema, ".DEATH d\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Death Criteria")
}

#' Build Observation Period SQL
build_observation_period_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  select_clause <- "op.person_id, op.observation_period_id, op.period_type_concept_id, op.observation_period_start_date as start_date, op.observation_period_end_date as end_date"
  ordinal <- ", row_number() over (PARTITION BY op.person_id ORDER BY op.observation_period_start_date) as ordinal"
  udp <- criteria$UserDefinedPeriod %||% criteria$userDefinedPeriod
  start_expr <- "@startDateExpression"
  end_expr <- "@endDateExpression"
  if (!is.null(udp) && !is.null(udp$startDate)) start_expr <- date_string_to_sql(udp$startDate)
  if (!is.null(udp) && !is.null(udp$endDate)) end_expr <- date_string_to_sql(udp$endDate)
  first <- criteria$First %||% criteria$first %||% FALSE
  where_parts <- if (first) "C.ordinal = 1" else character(0)
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", where_parts) else ""
  q <- paste0("-- Begin Observation Period Criteria\nselect C.person_id, C.observation_period_id as event_id, ", start_expr, " as start_date, ", end_expr, " as end_date,\n       CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date\nfrom \n(\n  select ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".OBSERVATION_PERIOD op\n) C\n", where_clause, "\n-- End Observation Period Criteria")
  gsub("@startDateExpression", "C.start_date", gsub("@endDateExpression", "C.end_date", q))
}

#' Build Location Region SQL
build_location_region_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_id <- criteria$CodesetId %||% criteria$codesetId
  cs_tbl <- .sql_context$codesets_table
  codeset_clause <- if (!is.null(codeset_id)) paste0("AND l.region_concept_id in (SELECT concept_id from ", cs_tbl, " where codeset_id = ", codeset_id, ")") else ""
  paste0("SELECT C.person_id, C.location_id as event_id, C.start_date, C.end_date, CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date\nFROM (\n  SELECT lh.entity_id as person_id, lh.location_id, l.region_concept_id, lh.start_date, lh.end_date\n  FROM ", cdm_schema, ".LOCATION_HISTORY lh\n  JOIN ", cdm_schema, ".LOCATION l on lh.location_id = l.location_id\n  WHERE lh.domain_id = 'PERSON'\n  ", codeset_clause, "\n) C\n-- End Location Region Criteria")
}

#' Build Drug Exposure SQL
build_drug_exposure_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "de.drug_concept_id", "de.drug_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY de.person_id ORDER BY de.drug_exposure_start_date, de.drug_exposure_id) as ordinal" else ""
  select_cols <- c("de.person_id", "de.drug_exposure_id", "de.drug_concept_id", "de.visit_occurrence_id",
                   "days_supply", "quantity", "refills")
  # Include type/route columns when needed for filtering
  dt <- criteria$DrugType %||% criteria$drugType
  dtc <- criteria$DrugTypeCS %||% criteria$drugTypeCS
  if (length(dt) > 0 || (!is.null(dtc) && !is.null(dtc$CodesetId %||% dtc$codesetId)))
    select_cols <- c(select_cols, "de.drug_type_concept_id")
  rc <- criteria$RouteConcept %||% criteria$routeConcept
  rcc <- criteria$RouteConceptCS %||% criteria$routeConceptCS
  if (length(rc) > 0 || (!is.null(rcc) && !is.null(rcc$CodesetId %||% rcc$codesetId)))
    select_cols <- c(select_cols, "de.route_concept_id")
  date_adj <- criteria$DateAdjustment %||% criteria$dateAdjustment
  if (!is.null(date_adj)) {
    start_col <- if ((date_adj$StartWith %||% date_adj$startWith) == "start_date") "de.drug_exposure_start_date" else "COALESCE(de.drug_exposure_end_date, DATEADD(day,de.days_supply,de.drug_exposure_start_date), DATEADD(day,1,de.drug_exposure_start_date))"
    end_col <- if ((date_adj$EndWith %||% date_adj$endWith) == "start_date") "de.drug_exposure_start_date" else "COALESCE(de.drug_exposure_end_date, DATEADD(day,de.days_supply,de.drug_exposure_start_date), DATEADD(day,1,de.drug_exposure_start_date))"
    select_cols <- c(select_cols, get_date_adjustment_expression(date_adj, start_col, end_col))
  } else {
    select_cols <- c(select_cols, "de.drug_exposure_start_date as start_date",
                     "COALESCE(de.drug_exposure_end_date, DATEADD(day,de.days_supply,de.drug_exposure_start_date), DATEADD(day,1,de.drug_exposure_start_date)) as end_date")
  }
  select_clause <- paste(select_cols, collapse = ", ")
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # OccurrenceStartDate, OccurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # DrugType
  dte <- criteria$DrugTypeExclude %||% criteria$drugTypeExclude %||% FALSE
  if (length(dt) > 0) {
    ids <- get_concept_ids(dt)
    if (length(ids) > 0) {
      op <- if (dte) "not in" else "in"
      where_parts <- c(where_parts, paste0("C.drug_type_concept_id ", op, " (", paste(ids, collapse = ","), ")"))
    }
  }
  if (!is.null(dtc) && !is.null(dtc$CodesetId %||% dtc$codesetId))
    where_parts <- c(where_parts, get_codeset_in_expression(dtc$CodesetId %||% dtc$codesetId, "C.drug_type_concept_id", dtc$IsExclusion %||% dtc$isExclusion %||% FALSE))
  # RouteConcept
  if (length(rc) > 0) {
    ids <- get_concept_ids(rc); if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.route_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(rcc) && !is.null(rcc$CodesetId %||% rcc$codesetId))
    where_parts <- c(where_parts, get_codeset_in_expression(rcc$CodesetId %||% rcc$codesetId, "C.route_concept_id", rcc$IsExclusion %||% rcc$isExclusion %||% FALSE))
  # Refills, Quantity, DaysSupply
  refills <- criteria$Refills %||% criteria$refills
  if (!is.null(refills)) { cl <- build_numeric_range_clause("C.refills", refills); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  qty <- criteria$Quantity %||% criteria$quantity
  if (!is.null(qty)) { cl <- build_numeric_range_clause("C.quantity", qty); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  ds <- criteria$DaysSupply %||% criteria$daysSupply
  if (!is.null(ds)) { cl <- build_numeric_range_clause("C.days_supply", ds); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # Age, Gender
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || (length(gender) > 0 && is.list(gender)) || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0 && is.list(gender)) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # VisitType
  vt <- criteria$VisitType %||% criteria$visitType
  vtc <- criteria$VisitTypeCS %||% criteria$visitTypeCS
  if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId))) {
    join_clause <- paste0(join_clause, " JOIN ", cdm_schema, ".VISIT_OCCURRENCE V on C.visit_occurrence_id = V.visit_occurrence_id and C.person_id = V.person_id")
    if (length(vt) > 0) { ids <- get_concept_ids(vt); if (length(ids) > 0) where_parts <- c(where_parts, paste0("V.visit_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(vtc) && !is.null(vtc$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(vtc$CodesetId, "V.visit_concept_id", vtc$IsExclusion %||% FALSE))
  }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "de")
  if (!is.null(ps_filter$select_col)) select_cols <- c(select_cols, ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  select_clause <- paste(select_cols, collapse = ", ")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Drug Exposure Criteria\nSELECT C.person_id, C.drug_exposure_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".DRUG_EXPOSURE de\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Drug Exposure Criteria")
}

#' Build Visit Occurrence SQL
build_visit_occurrence_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "vo.visit_concept_id", "vo.visit_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY vo.person_id ORDER BY vo.visit_start_date, vo.visit_occurrence_id) as ordinal" else ""
  select_cols <- c("vo.person_id", "vo.visit_occurrence_id", "vo.visit_concept_id")
  vt <- criteria$VisitType %||% criteria$visitType
  vtc <- criteria$VisitTypeCS %||% criteria$visitTypeCS
  if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId %||% vtc$codesetId)))
    select_cols <- c(select_cols, "vo.visit_type_concept_id")
  date_adj <- criteria$DateAdjustment %||% criteria$dateAdjustment
  if (!is.null(date_adj)) {
    start_col <- if ((date_adj$StartWith %||% date_adj$startWith) == "start_date") "vo.visit_start_date" else "vo.visit_end_date"
    end_col <- if ((date_adj$EndWith %||% date_adj$endWith) == "start_date") "vo.visit_start_date" else "vo.visit_end_date"
    select_cols <- c(select_cols, get_date_adjustment_expression(date_adj, start_col, end_col))
  } else {
    select_cols <- c(select_cols, "vo.visit_start_date as start_date", "vo.visit_end_date as end_date")
  }
  select_clause <- paste(select_cols, collapse = ", ")
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # OccurrenceStartDate, OccurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # VisitType (visit_type_concept_id, not visit_concept_id)
  vte <- criteria$VisitTypeExclude %||% criteria$visitTypeExclude %||% FALSE
  if (length(vt) > 0) {
    ids <- get_concept_ids(vt)
    if (length(ids) > 0) { op <- if (vte) "not in" else "in"; where_parts <- c(where_parts, paste0("C.visit_type_concept_id ", op, " (", paste(ids, collapse = ","), ")")) }
  }
  if (!is.null(vtc) && !is.null(vtc$CodesetId %||% vtc$codesetId))
    where_parts <- c(where_parts, get_codeset_in_expression(vtc$CodesetId %||% vtc$codesetId, "C.visit_type_concept_id", vtc$IsExclusion %||% vtc$isExclusion %||% FALSE))
  # VisitLength
  vl <- criteria$VisitLength %||% criteria$visitLength
  if (!is.null(vl)) { cl <- build_numeric_range_clause("DATEDIFF(day,C.start_date, C.end_date)", vl); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # Age, Gender
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || (length(gender) > 0) || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "vo")
  if (!is.null(ps_filter$select_col)) select_cols <- c(select_cols, ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  # Rebuild select_clause in case ProviderSpecialty added provider_id
  select_clause <- paste(select_cols, collapse = ", ")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Visit Occurrence Criteria\nselect C.person_id, C.visit_occurrence_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nfrom \n(\n  select ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".VISIT_OCCURRENCE vo\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Visit Occurrence Criteria")
}

#' Build Procedure Occurrence SQL
build_procedure_occurrence_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "po.procedure_concept_id", "po.procedure_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY po.person_id ORDER BY po.procedure_date, po.procedure_occurrence_id) as ordinal" else ""
  select_cols <- c("po.person_id", "po.procedure_occurrence_id", "po.procedure_concept_id",
                   "po.visit_occurrence_id", "po.quantity")
  pt <- criteria$ProcedureType %||% criteria$procedureType
  ptc <- criteria$ProcedureTypeCS %||% criteria$procedureTypeCS
  if (length(pt) > 0 || (!is.null(ptc) && !is.null(ptc$CodesetId %||% ptc$codesetId)))
    select_cols <- c(select_cols, "po.procedure_type_concept_id")
  modifier <- criteria$Modifier %||% criteria$modifier
  modc <- criteria$ModifierCS %||% criteria$modifierCS
  if (length(modifier) > 0 || (!is.null(modc) && !is.null(modc$CodesetId %||% modc$codesetId)))
    select_cols <- c(select_cols, "po.modifier_concept_id")
  date_adj <- criteria$DateAdjustment %||% criteria$dateAdjustment
  if (!is.null(date_adj)) {
    start_col <- if ((date_adj$StartWith %||% date_adj$startWith) == "start_date") "po.procedure_date" else "DATEADD(day,1,po.procedure_date)"
    end_col <- if ((date_adj$EndWith %||% date_adj$endWith) == "start_date") "po.procedure_date" else "DATEADD(day,1,po.procedure_date)"
    select_cols <- c(select_cols, get_date_adjustment_expression(date_adj, start_col, end_col))
  } else {
    select_cols <- c(select_cols, "po.procedure_date as start_date", "DATEADD(day,1,po.procedure_date) as end_date")
  }
  select_clause <- paste(select_cols, collapse = ", ")
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # OccurrenceStartDate, OccurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # ProcedureType
  pte <- criteria$ProcedureTypeExclude %||% criteria$procedureTypeExclude %||% FALSE
  if (length(pt) > 0) {
    ids <- get_concept_ids(pt)
    if (length(ids) > 0) { op <- if (pte) "not in" else "in"; where_parts <- c(where_parts, paste0("C.procedure_type_concept_id ", op, " (", paste(ids, collapse = ","), ")")) }
  }
  if (!is.null(ptc) && !is.null(ptc$CodesetId %||% ptc$codesetId))
    where_parts <- c(where_parts, get_codeset_in_expression(ptc$CodesetId %||% ptc$codesetId, "C.procedure_type_concept_id", ptc$IsExclusion %||% ptc$isExclusion %||% FALSE))
  # Modifier
  if (length(modifier) > 0) {
    ids <- get_concept_ids(modifier); if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.modifier_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(modc) && !is.null(modc$CodesetId %||% modc$codesetId))
    where_parts <- c(where_parts, get_codeset_in_expression(modc$CodesetId %||% modc$codesetId, "C.modifier_concept_id", modc$IsExclusion %||% modc$isExclusion %||% FALSE))
  # Quantity
  qty <- criteria$Quantity %||% criteria$quantity
  if (!is.null(qty)) { cl <- build_numeric_range_clause("C.quantity", qty); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # Age, Gender
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || (length(gender) > 0) || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # VisitType
  vt <- criteria$VisitType %||% criteria$visitType
  vtc <- criteria$VisitTypeCS %||% criteria$visitTypeCS
  if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId))) {
    join_clause <- paste0(join_clause, " JOIN ", cdm_schema, ".VISIT_OCCURRENCE V on C.visit_occurrence_id = V.visit_occurrence_id and C.person_id = V.person_id")
    if (length(vt) > 0) { ids <- get_concept_ids(vt); if (length(ids) > 0) where_parts <- c(where_parts, paste0("V.visit_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(vtc) && !is.null(vtc$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(vtc$CodesetId, "V.visit_concept_id", vtc$IsExclusion %||% FALSE))
  }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "po")
  if (!is.null(ps_filter$select_col)) select_cols <- c(select_cols, ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  select_clause <- paste(select_cols, collapse = ", ")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Procedure Occurrence Criteria\nSELECT C.person_id, C.procedure_occurrence_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".PROCEDURE_OCCURRENCE po\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Procedure Occurrence Criteria")
}

#' Build Measurement SQL
build_measurement_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "m.measurement_concept_id", "m.measurement_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY m.person_id ORDER BY m.measurement_date, m.measurement_id) as ordinal" else ""
  # CirceR/Java always include value_as_number, range_high, range_low; add unit/value_as_concept when used
  select_cols <- c("m.person_id", "m.measurement_id", "m.measurement_concept_id", "m.visit_occurrence_id",
    "m.value_as_number", "m.range_high", "m.range_low")
  unit <- criteria$Unit %||% criteria$unit
  unit_cs <- criteria$UnitCS %||% criteria$unitCS
  if (!is.null(unit) && length(unit) > 0 || (!is.null(unit_cs) && !is.null(unit_cs$CodesetId))) {
    select_cols <- c(select_cols, "m.unit_concept_id")
  }
  vac <- criteria$ValueAsConcept %||% criteria$valueAsConcept
  vac_cs <- criteria$ValueAsConceptCS %||% criteria$valueAsConceptCS
  if (!is.null(vac) && length(vac) > 0 || (!is.null(vac_cs) && !is.null(vac_cs$CodesetId))) {
    select_cols <- c(select_cols, "m.value_as_concept_id")
  }
  # MeasurementType
  mt <- criteria$MeasurementType %||% criteria$measurementType
  mtc <- criteria$MeasurementTypeCS %||% criteria$measurementTypeCS
  if (length(mt) > 0 || (!is.null(mtc) && !is.null(mtc$CodesetId))) {
    select_cols <- c(select_cols, "m.measurement_type_concept_id")
  }
  # Operator
  op_concept <- criteria$Operator %||% criteria$operator
  opc <- criteria$OperatorCS %||% criteria$operatorCS
  if (length(op_concept) > 0 || (!is.null(opc) && !is.null(opc$CodesetId))) {
    select_cols <- c(select_cols, "m.operator_concept_id")
  }
  date_adj <- criteria$DateAdjustment %||% criteria$dateAdjustment
  if (!is.null(date_adj)) {
    start_col <- if ((date_adj$StartWith %||% date_adj$startWith) == "start_date") "m.measurement_date" else "DATEADD(day,1,m.measurement_date)"
    end_col <- if ((date_adj$EndWith %||% date_adj$endWith) == "start_date") "m.measurement_date" else "DATEADD(day,1,m.measurement_date)"
    select_cols <- c(select_cols, get_date_adjustment_expression(date_adj, start_col, end_col))
  } else {
    select_cols <- c(select_cols, "m.measurement_date as start_date, DATEADD(day,1,m.measurement_date) as end_date")
  }
  select_clause <- paste(select_cols, collapse = ", ")
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # occurrenceStartDate, occurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # MeasurementType
  mte <- criteria$MeasurementTypeExclude %||% criteria$measurementTypeExclude %||% FALSE
  if (length(mt) > 0) {
    ids <- get_concept_ids(mt)
    if (length(ids) > 0) {
      mt_op <- if (mte) "not in" else "in"
      where_parts <- c(where_parts, paste0("C.measurement_type_concept_id ", mt_op, " (", paste(ids, collapse = ","), ")"))
    }
  }
  if (!is.null(mtc) && !is.null(mtc$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(mtc$CodesetId, "C.measurement_type_concept_id", mtc$IsExclusion %||% FALSE))
  }
  # Operator
  if (length(op_concept) > 0) {
    ids <- get_concept_ids(op_concept)
    if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.operator_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(opc) && !is.null(opc$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(opc$CodesetId, "C.operator_concept_id", opc$IsExclusion %||% FALSE))
  }
  # valueAsNumber, rangeLow, rangeHigh, rangeLowRatio, rangeHighRatio (CirceR uses .4f format)
  van <- criteria$ValueAsNumber %||% criteria$valueAsNumber
  if (!is.null(van)) { cl <- build_numeric_range_clause("C.value_as_number", van, "%.4f"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  rl <- criteria$RangeLow %||% criteria$rangeLow
  if (!is.null(rl)) { cl <- build_numeric_range_clause("C.range_low", rl, "%.4f"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  rh <- criteria$RangeHigh %||% criteria$rangeHigh
  if (!is.null(rh)) { cl <- build_numeric_range_clause("C.range_high", rh, "%.4f"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  rlr <- criteria$RangeLowRatio %||% criteria$rangeLowRatio
  if (!is.null(rlr)) { cl <- build_numeric_range_clause("(C.value_as_number / NULLIF(C.range_low, 0))", rlr, "%.4f"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  rhr <- criteria$RangeHighRatio %||% criteria$rangeHighRatio
  if (!is.null(rhr)) { cl <- build_numeric_range_clause("(C.value_as_number / NULLIF(C.range_high, 0))", rhr, "%.4f"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # Unit
  if (!is.null(unit) && length(unit) > 0) {
    ids <- get_concept_ids(unit)
    if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.unit_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(unit_cs) && !is.null(unit_cs$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(unit_cs$CodesetId, "C.unit_concept_id", unit_cs$IsExclusion %||% FALSE))
  }
  # ValueAsConcept
  if (!is.null(vac) && length(vac) > 0) {
    ids <- get_concept_ids(vac)
    if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.value_as_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(vac_cs) && !is.null(vac_cs$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(vac_cs$CodesetId, "C.value_as_concept_id", vac_cs$IsExclusion %||% FALSE))
  }
  # Abnormal (value outside range or abnormal concept)
  abnormal <- criteria$Abnormal %||% criteria$abnormal
  if (isTRUE(abnormal)) {
    where_parts <- c(where_parts, "(C.value_as_number < C.range_low or C.value_as_number > C.range_high or C.value_as_concept_id in (4155142, 4155143))")
  }
  # Age, Gender
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || length(gender) > 0 || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0(join_clause, " JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # VisitType
  vt <- criteria$VisitType %||% criteria$visitType
  vtc <- criteria$VisitTypeCS %||% criteria$visitTypeCS
  if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId))) {
    join_clause <- paste0(join_clause, " JOIN ", cdm_schema, ".VISIT_OCCURRENCE V on C.visit_occurrence_id = V.visit_occurrence_id and C.person_id = V.person_id")
    if (length(vt) > 0) { ids <- get_concept_ids(vt); if (length(ids) > 0) where_parts <- c(where_parts, paste0("V.visit_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(vtc) && !is.null(vtc$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(vtc$CodesetId, "V.visit_concept_id", vtc$IsExclusion %||% FALSE))
  }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "m")
  if (!is.null(ps_filter$select_col)) select_cols <- c(select_cols, ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  select_clause <- paste(select_cols, collapse = ", ")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Measurement Criteria\nSELECT C.person_id, C.measurement_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".MEASUREMENT m\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Measurement Criteria")
}

#' Build Observation SQL
build_observation_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "o.observation_concept_id", "o.observation_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY o.person_id ORDER BY o.observation_date, o.observation_id) as ordinal" else ""
  # CirceR/Java: value_as_number always; add value_as_string, value_as_concept_id, unit_concept_id when used
  select_cols <- c("o.person_id", "o.observation_id", "o.observation_concept_id", "o.visit_occurrence_id",
    "o.value_as_number")
  vac <- criteria$ValueAsConcept %||% criteria$valueAsConcept
  vac_cs <- criteria$ValueAsConceptCS %||% criteria$valueAsConceptCS
  unit <- criteria$Unit %||% criteria$unit
  unit_cs <- criteria$UnitCS %||% criteria$unitCS
  if (!is.null(vac) && length(vac) > 0 || (!is.null(vac_cs) && !is.null(vac_cs$CodesetId))) {
    select_cols <- c(select_cols, "o.value_as_concept_id")
  }
  if (!is.null(unit) && length(unit) > 0 || (!is.null(unit_cs) && !is.null(unit_cs$CodesetId))) {
    select_cols <- c(select_cols, "o.unit_concept_id")
  }
  # value_as_string when valueAsString filter used
  vas <- criteria$ValueAsString %||% criteria$valueAsString
  if (!is.null(vas)) select_cols <- c(select_cols, "o.value_as_string")
  select_cols <- c(select_cols, "o.observation_date as start_date, DATEADD(day,1,o.observation_date) as end_date")
  select_clause <- paste(select_cols, collapse = ", ")
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # age, gender - need PERSON join
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || (length(gender) > 0) || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # occurrenceStartDate, occurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # valueAsNumber, valueAsConcept, unit - add when used (CirceR uses .4f for numeric)
  van <- criteria$ValueAsNumber %||% criteria$valueAsNumber
  if (!is.null(van)) { cl <- build_numeric_range_clause("C.value_as_number", van, "%.4f"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  vac <- criteria$ValueAsConcept %||% criteria$valueAsConcept
  vac_cs <- criteria$ValueAsConceptCS %||% criteria$valueAsConceptCS
  if (!is.null(vac) && length(vac) > 0) {
    ids <- get_concept_ids(vac)
    if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.value_as_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(vac_cs) && !is.null(vac_cs$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(vac_cs$CodesetId, "C.value_as_concept_id", vac_cs$IsExclusion %||% FALSE))
  }
  unit <- criteria$Unit %||% criteria$unit
  unit_cs <- criteria$UnitCS %||% criteria$unitCS
  if (!is.null(unit) && length(unit) > 0) {
    ids <- get_concept_ids(unit)
    if (length(ids) > 0) where_parts <- c(where_parts, paste0("C.unit_concept_id in (", paste(ids, collapse = ","), ")"))
  }
  if (!is.null(unit_cs) && !is.null(unit_cs$CodesetId)) {
    where_parts <- c(where_parts, get_codeset_in_expression(unit_cs$CodesetId, "C.unit_concept_id", unit_cs$IsExclusion %||% FALSE))
  }
  # ValueAsString filter
  vas <- criteria$ValueAsString %||% criteria$valueAsString
  if (!is.null(vas)) { cl <- build_text_filter_clause(vas, "C.value_as_string"); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "o")
  if (!is.null(ps_filter$select_col)) select_cols <- c(select_cols, ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  select_clause <- paste(select_cols, collapse = ", ")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Observation Criteria\nSELECT C.person_id, C.observation_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".OBSERVATION o\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Observation Criteria")
}

#' Build Device Exposure SQL
build_device_exposure_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "de.device_concept_id", "de.device_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY de.person_id ORDER BY de.device_exposure_start_date, de.device_exposure_id) as ordinal" else ""
  select_cols <- c("de.person_id", "de.device_exposure_id", "de.device_concept_id", "de.visit_occurrence_id", "de.quantity")
  dt <- criteria$DeviceType %||% criteria$deviceType
  dtc <- criteria$DeviceTypeCS %||% criteria$deviceTypeCS
  if (length(dt) > 0 || (!is.null(dtc) && !is.null(dtc$CodesetId %||% dtc$codesetId)))
    select_cols <- c(select_cols, "de.device_type_concept_id")
  date_adj <- criteria$DateAdjustment %||% criteria$dateAdjustment
  if (!is.null(date_adj)) {
    start_col <- if ((date_adj$StartWith %||% date_adj$startWith) == "start_date") "de.device_exposure_start_date" else "COALESCE(de.device_exposure_end_date, DATEADD(day,1,de.device_exposure_start_date))"
    end_col <- if ((date_adj$EndWith %||% date_adj$endWith) == "start_date") "de.device_exposure_start_date" else "COALESCE(de.device_exposure_end_date, DATEADD(day,1,de.device_exposure_start_date))"
    select_cols <- c(select_cols, get_date_adjustment_expression(date_adj, start_col, end_col))
  } else {
    select_cols <- c(select_cols, "de.device_exposure_start_date as start_date", "COALESCE(de.device_exposure_end_date, DATEADD(day,1,de.device_exposure_start_date)) as end_date")
  }
  select_clause <- paste(select_cols, collapse = ", ")
  join_clause <- ""
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  # OccurrenceStartDate, OccurrenceEndDate
  osd <- criteria$OccurrenceStartDate %||% criteria$occurrenceStartDate
  oed <- criteria$OccurrenceEndDate %||% criteria$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("C.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("C.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # DeviceType
  dte <- criteria$DeviceTypeExclude %||% criteria$deviceTypeExclude %||% FALSE
  if (length(dt) > 0) {
    ids <- get_concept_ids(dt)
    if (length(ids) > 0) { op <- if (dte) "not in" else "in"; where_parts <- c(where_parts, paste0("C.device_type_concept_id ", op, " (", paste(ids, collapse = ","), ")")) }
  }
  if (!is.null(dtc) && !is.null(dtc$CodesetId %||% dtc$codesetId))
    where_parts <- c(where_parts, get_codeset_in_expression(dtc$CodesetId %||% dtc$codesetId, "C.device_type_concept_id", dtc$IsExclusion %||% dtc$isExclusion %||% FALSE))
  # Quantity
  qty <- criteria$Quantity %||% criteria$quantity
  if (!is.null(qty)) { cl <- build_numeric_range_clause("C.quantity", qty); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  # Age, Gender
  age <- criteria$Age %||% criteria$age
  gender <- criteria$Gender %||% criteria$gender
  gcs <- criteria$GenderCS %||% criteria$genderCS
  if (!is.null(age) || (length(gender) > 0) || (!is.null(gcs) && !is.null(gcs$CodesetId))) {
    join_clause <- paste0("JOIN ", cdm_schema, ".PERSON P on C.person_id = P.person_id")
    if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(C.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
    if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  }
  # VisitType
  vt2 <- criteria$VisitType %||% criteria$visitType
  vtc2 <- criteria$VisitTypeCS %||% criteria$visitTypeCS
  if (length(vt2) > 0 || (!is.null(vtc2) && !is.null(vtc2$CodesetId))) {
    join_clause <- paste0(join_clause, " JOIN ", cdm_schema, ".VISIT_OCCURRENCE V on C.visit_occurrence_id = V.visit_occurrence_id and C.person_id = V.person_id")
    if (length(vt2) > 0) { ids <- get_concept_ids(vt2); if (length(ids) > 0) where_parts <- c(where_parts, paste0("V.visit_concept_id in (", paste(ids, collapse = ","), ")")) }
    if (!is.null(vtc2) && !is.null(vtc2$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(vtc2$CodesetId, "V.visit_concept_id", vtc2$IsExclusion %||% FALSE))
  }
  # ProviderSpecialty
  ps_filter <- add_provider_specialty_filter(criteria, cdm_schema, "de")
  if (!is.null(ps_filter$select_col)) select_cols <- c(select_cols, ps_filter$select_col)
  if (!is.null(ps_filter$join_sql)) join_clause <- paste(join_clause, ps_filter$join_sql)
  where_parts <- c(where_parts, ps_filter$where_parts)
  select_clause <- paste(select_cols, collapse = ", ")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Device Exposure Criteria\nSELECT C.person_id, C.device_exposure_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".DEVICE_EXPOSURE de\n  ", codeset_clause, "\n) C\n", join_clause, "\n", where_clause, "\n-- End Device Exposure Criteria")
}

#' Build Specimen SQL
build_specimen_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "s.specimen_concept_id", "s.specimen_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY s.person_id ORDER BY s.specimen_date, s.specimen_id) as ordinal" else ""
  select_clause <- "s.person_id, s.specimen_id, s.specimen_concept_id, s.specimen_date as start_date, s.specimen_date as end_date"
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Specimen Criteria\nSELECT C.person_id, C.specimen_id as event_id, C.start_date, C.end_date, CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".SPECIMEN s\n  ", codeset_clause, "\n) C\n", where_clause, "\n-- End Specimen Criteria")
}

#' Build Condition Era SQL
build_condition_era_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_where_clause(criteria$CodesetId %||% criteria$codesetId, "ce.condition_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY ce.person_id ORDER BY ce.condition_era_start_date) as ordinal" else ""
  select_clause <- "ce.person_id, ce.condition_era_id, ce.condition_concept_id, ce.condition_era_start_date as start_date, ce.condition_era_end_date as end_date"
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Condition Era Criteria\nSELECT C.person_id, C.condition_era_id as event_id, C.start_date, C.end_date, CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".CONDITION_ERA ce\n  ", codeset_clause, "\n) C\n", where_clause, "\n-- End Condition Era Criteria")
}

#' Build Drug Era SQL
build_drug_era_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_where_clause(criteria$CodesetId %||% criteria$codesetId, "de.drug_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY de.person_id ORDER BY de.drug_era_start_date) as ordinal" else ""
  select_clause <- "de.person_id, de.drug_era_id, de.drug_concept_id, de.drug_exposure_count, de.gap_days, de.drug_era_start_date as start_date, de.drug_era_end_date as end_date"
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Drug Era Criteria\nSELECT C.person_id, C.drug_era_id as event_id, C.start_date, C.end_date, CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date, C.drug_concept_id as domain_concept_id\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".DRUG_ERA de\n  ", codeset_clause, "\n) C\n", where_clause, "\n-- End Drug Era Criteria")
}

#' Build Dose Era SQL
build_dose_era_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_where_clause(criteria$CodesetId %||% criteria$codesetId, "de.drug_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY de.person_id ORDER BY de.dose_era_start_date) as ordinal" else ""
  select_clause <- "de.person_id, de.dose_era_id, de.drug_concept_id, de.dose_era_start_date as start_date, de.dose_era_end_date as end_date"
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Dose Era Criteria\nSELECT C.person_id, C.dose_era_id as event_id, C.start_date, C.end_date, CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date, C.drug_concept_id as domain_concept_id\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".DOSE_ERA de\n  ", codeset_clause, "\n) C\n", where_clause, "\n-- End Dose Era Criteria")
}

#' Build Visit Detail SQL
build_visit_detail_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  codeset_clause <- get_codeset_clause(criteria, "vd.visit_detail_concept_id", "vd.visit_detail_source_concept_id")
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY vd.person_id ORDER BY vd.visit_detail_start_date, vd.visit_detail_id) as ordinal" else ""
  select_clause <- "vd.person_id, vd.visit_detail_id, vd.visit_detail_concept_id, vd.visit_occurrence_id, vd.visit_detail_start_date as start_date, vd.visit_detail_end_date as end_date"
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Visit Detail Criteria\nSELECT C.person_id, C.visit_detail_id as event_id, C.start_date, C.end_date, C.visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".VISIT_DETAIL vd\n  ", codeset_clause, "\n) C\n", where_clause, "\n-- End Visit Detail Criteria")
}

#' Build Payer Plan Period SQL (simplified)
build_payer_plan_period_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  first <- criteria$First %||% criteria$first %||% FALSE
  ordinal <- if (first) ", row_number() over (PARTITION BY ppp.person_id ORDER BY ppp.payer_plan_period_start_date) as ordinal" else ""
  select_clause <- "ppp.person_id, ppp.payer_plan_period_id, ppp.payer_plan_period_start_date as start_date, ppp.payer_plan_period_end_date as end_date"
  where_parts <- character(0)
  if (first) where_parts <- c(where_parts, "C.ordinal = 1")
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Payer Plan Period Criteria\nSELECT C.person_id, C.payer_plan_period_id as event_id, C.start_date, C.end_date, CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date\nFROM \n(\n  SELECT ", select_clause, ordinal, "\n  FROM ", cdm_schema, ".PAYER_PLAN_PERIOD ppp\n) C\n", where_clause, "\n-- End Payer Plan Period Criteria")
}

#' Dispatch to appropriate criteria SQL builder
#' @param criteria List - criteria object (may be wrapped in type key)
#' @param cdm_schema CDM schema name
#' @return SQL string
get_criteria_sql <- function(criteria, cdm_schema = "@cdm_database_schema") {
  ext <- extract_criteria(criteria)
  if (is.null(ext)) stop("Unknown or invalid criteria type")
  type <- ext$type
  data <- ext$data
  base_query <- switch(type,
    ConditionOccurrence = build_condition_occurrence_sql(data, cdm_schema),
    Death = build_death_sql(data, cdm_schema),
    DrugExposure = build_drug_exposure_sql(data, cdm_schema),
    ProcedureOccurrence = build_procedure_occurrence_sql(data, cdm_schema),
    VisitOccurrence = build_visit_occurrence_sql(data, cdm_schema),
    Observation = build_observation_sql(data, cdm_schema),
    Measurement = build_measurement_sql(data, cdm_schema),
    DeviceExposure = build_device_exposure_sql(data, cdm_schema),
    Specimen = build_specimen_sql(data, cdm_schema),
    VisitDetail = build_visit_detail_sql(data, cdm_schema),
    ObservationPeriod = build_observation_period_sql(data, cdm_schema),
    PayerPlanPeriod = build_payer_plan_period_sql(data, cdm_schema),
    LocationRegion = build_location_region_sql(data, cdm_schema),
    ConditionEra = build_condition_era_sql(data, cdm_schema),
    DrugEra = build_drug_era_sql(data, cdm_schema),
    DoseEra = build_dose_era_sql(data, cdm_schema),
    stop("Unsupported criteria type: ", type)
  )
  corr <- data$CorrelatedCriteria %||% data$correlatedCriteria
  if (!is.null(corr) && (length(corr$CriteriaList %||% corr$criteriaList %||% list()) > 0 || length(corr$Groups %||% corr$groups %||% list()) > 0)) {
    base_query <- wrap_criteria_query(base_query, corr, cdm_schema)
  }
  base_query
}
#' Cohort Expression Query Builder
#'
#' Main SQL query builder for cohort expressions.
#' Generates complete cohort SQL from cohort definition.
#'
#' @name cohort_expression_query_builder_section
#' @keywords internal
NULL

COHORT_QUERY_TEMPLATE <- "
@codesetQuery

@primaryEventsQuery
--- Inclusion Rule Inserts

@inclusionCohortInserts

@includedEventsQuery
@strategy_ends_temp_tables

@cohortRowsAndFinalBlock

delete from @target_database_schema.@target_cohort_table where @cohort_id_field_name = @target_cohort_id;
insert into @target_database_schema.@target_cohort_table (@cohort_id_field_name, subject_id, cohort_start_date, cohort_end_date)
@finalCohortQuery
;

@inclusionAnalysisQuery

@strategy_ends_cleanup

@cleanupBlock
"

# Cleanup block (trailing TRUNCATE/DROP) — used in template and in structured tail_sql
CLEANUP_BLOCK <- "
TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
"

# Cohort rows + final_cohort block (everything after strategy_ends up to and including group_idx)
COHORT_ROWS_AND_FINAL_TEMPLATE <- "
-- generate cohort periods into #final_cohort
select person_id, start_date, end_date
INTO #cohort_rows
from ( -- first_ends
\tselect F.person_id, F.start_date, F.end_date
\tFROM (
\t  select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
\t  from #included_events I
\t  join ( -- cohort_ends
-- cohort exit dates
@cohort_end_unions
\t    ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
\t) F
\tWHERE F.ordinal = 1
) FE;


select person_id, min(start_date) as start_date, DATEADD(day,-1 * @eraconstructorpad, max(end_date)) as end_date
into #final_cohort
from (
  select person_id, start_date, end_date, sum(is_start) over (partition by person_id order by start_date, is_start desc rows unbounded preceding) group_idx
  from (
    select person_id, start_date, end_date,
      case when max(end_date) over (partition by person_id order by start_date rows between unbounded preceding and 1 preceding) >= start_date then 0 else 1 end is_start
    from (
      select person_id, start_date, DATEADD(day,@eraconstructorpad,end_date) as end_date
      from #cohort_rows
    ) CR
  ) ST
) GR
group by person_id, group_idx;
"

CODESET_QUERY_TEMPLATE <- "
DROP TABLE IF EXISTS #Codesets;
create table #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

@codesetInserts

UPDATE STATISTICS #Codesets;
"

PRIMARY_EVENTS_TEMPLATE <- "
select event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
from
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date @QualifiedEventSort, pe.event_id) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  from (-- Begin Primary Events
@primaryEventsSubQuery
-- End Primary Events
) pe
  @additionalCriteriaQuery
) QE
@QualifiedLimitFilter
;
"

PRIMARY_EVENTS_SUBQUERY_TEMPLATE <- "
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
from
(
  select E.person_id, E.start_date, E.end_date,
         row_number() over (partition by E.person_id order by E.sort_date @EventSort, E.event_id) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
  @criteriaQueries
  ) E
\tjoin @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  where @primaryEventsFilter
) P
@primaryEventLimit
"

INCLUDED_EVENTS_TEMPLATE <- "
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
from (
  select event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date @IncludedEventSort) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    left join #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    group by Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
@InclusionRuleMaskFilter
) Results
@ResultLimitFilter
;
"

DATE_OFFSET_STRATEGY_TEMPLATE <- "
-- date offset strategy

select event_id, person_id,
  case when DATEADD(day,@offset,@dateField) > op_end_date then op_end_date else DATEADD(day,@offset,@dateField) end as end_date
INTO #strategy_ends
from @eventTable;
"

# CustomEra: cohort exit = end of drug_era (or condition_era) that contains the event start
CUSTOM_ERA_DRUG_TEMPLATE <- "
-- CustomEra (Drug): end date = drug_era_end_date for the era containing the event start
select event_id, person_id, min(end_date) as end_date
INTO #strategy_ends
from (
  select ie.event_id, ie.person_id,
    case when de.drug_era_end_date > ie.op_end_date then ie.op_end_date else de.drug_era_end_date end as end_date
  from @eventTable ie
  inner join @cdm_database_schema.drug_era de on de.person_id = ie.person_id
    and ie.start_date >= de.drug_era_start_date
    and ie.start_date <= de.drug_era_end_date
  inner join #Codesets c on c.codeset_id = @DrugCodesetId and c.concept_id = de.drug_concept_id
) X
group by event_id, person_id;
"

CUSTOM_ERA_CONDITION_TEMPLATE <- "
-- CustomEra (Condition): end date = condition_era_end_date for the era containing the event start
select event_id, person_id, min(end_date) as end_date
INTO #strategy_ends
from (
  select ie.event_id, ie.person_id,
    case when ce.condition_era_end_date > ie.op_end_date then ie.op_end_date else ce.condition_era_end_date end as end_date
  from @eventTable ie
  inner join @cdm_database_schema.condition_era ce on ce.person_id = ie.person_id
    and ie.start_date >= ce.condition_era_start_date
    and ie.start_date <= ce.condition_era_end_date
  inner join #Codesets c on c.codeset_id = @ConditionCodesetId and c.concept_id = ce.condition_concept_id
) X
group by event_id, person_id;
"

INCLUSION_RULE_TEMP_TABLE_TEMPLATE <- "-- Create a temp table of inclusion rule rows for joining in the inclusion rule impact analysis

select cast(rule_sequence as int) as rule_sequence
into #inclusion_rules
from (
  @inclusionRuleUnions
) IR;
"

COHORT_INCLUSION_ANALYSIS_TEMPLATE <- "-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where @cohort_id_field_name = @target_cohort_id and mode_id = @inclusionImpactMode;
insert into @results_database_schema.cohort_inclusion_result (@cohort_id_field_name, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as @cohort_id_field_name, inclusion_rule_mask, count_big(*) as person_count, @inclusionImpactMode as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from @eventTable Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts
delete from @results_database_schema.cohort_inclusion_stats where @cohort_id_field_name = @target_cohort_id and mode_id = @inclusionImpactMode;
insert into @results_database_schema.cohort_inclusion_stats (@cohort_id_field_name, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as @cohort_id_field_name, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, @inclusionImpactMode as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from @eventTable Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from @eventTable) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = @inclusionImpactMode AND SR.@cohort_id_field_name = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where @cohort_id_field_name = @target_cohort_id and mode_id = @inclusionImpactMode;
insert into @results_database_schema.cohort_summary_stats (@cohort_id_field_name, base_count, final_count, mode_id)
select @target_cohort_id as @cohort_id_field_name, PC.total as person_count, coalesce(FC.total, 0) as final_count, @inclusionImpactMode as mode_id
FROM
(select count_big(event_id) as total from @eventTable) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = @inclusionImpactMode and sr.@cohort_id_field_name = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;
"

BEST_EVENTS_QUERY <- "
-- Find the event that is the 'best match' per person.
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

select q.person_id, q.event_id
into #best_events
from #qualified_events Q
join (
\tSELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
\tFROM (
\t\tSELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
\t\tFROM #qualified_events Q
\t\tLEFT JOIN #inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
\t\tGROUP BY Q.person_id, Q.event_id, Q.start_date
\t) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;
"

GROUP_QUERY_TEMPLATE <- "-- Begin Criteria Group
select @indexId as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM @eventTable E
  @joinType JOIN
  (
    @criteriaQueries
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  @occurrenceCountClause
) G
-- End Criteria Group
"

ADDITIONAL_CRITERIA_INNER_TEMPLATE <- "-- Begin Correlated Criteria
select @indexId as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id@additionalColumns
FROM @eventTable P
JOIN (
  @criteriaQuery
) A on A.person_id = P.person_id @windowCriteria ) cc
GROUP BY cc.person_id, cc.event_id
@occurrenceCriteria
-- End Correlated Criteria
"

ADDITIONAL_CRITERIA_LEFT_TEMPLATE <- "-- Begin Correlated Criteria
select @indexId as index_id, p.person_id, p.event_id
from @eventTable p
LEFT JOIN (
SELECT p.person_id, p.event_id@additionalColumns
FROM @eventTable P
JOIN (
@criteriaQuery
) A on A.person_id = P.person_id @windowCriteria ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
@occurrenceCriteria
-- End Correlated Criteria
"

#' Get scalar from possibly-vector value (JSON may return arrays)
scalar <- function(x) if (is.null(x)) NULL else if (length(x) > 0) x[[1]] else NULL

#' Build window criteria clause from StartWindow/EndWindow
build_window_criteria <- function(start_window, end_window, check_observation_period) {
  clauses <- character(0)
  if (check_observation_period) {
    clauses <- c(clauses, "A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE")
  }
  start_idx <- "P.START_DATE"
  start_evt <- "A.START_DATE"
  if (!is.null(start_window)) {
    use_end <- scalar(start_window$UseIndexEnd %||% start_window$useIndexEnd)
    use_evt_end <- scalar(start_window$UseEventEnd %||% start_window$useEventEnd)
    if (isTRUE(use_end)) start_idx <- "P.END_DATE"
    if (isTRUE(use_evt_end)) start_evt <- "A.END_DATE"
    s <- start_window$Start %||% start_window$start
    e <- start_window$End %||% start_window$end
    s_days <- scalar(s$Days %||% s$days)
    s_coeff <- scalar(s$Coeff %||% s$coeff) %||% 1
    e_days <- scalar(e$Days %||% e$days)
    e_coeff <- scalar(e$Coeff %||% e$coeff) %||% 1
    if (!is.null(s) && !is.null(s_days)) {
      clauses <- c(clauses, paste0(start_evt, " >= DATEADD(day,", s_coeff * s_days, ",", start_idx, ")"))
    } else if (!is.null(s) && check_observation_period && (s_coeff == -1 || identical(s_coeff, c(-1)))) {
      clauses <- c(clauses, paste0(start_evt, " >= P.OP_START_DATE"))
    } else if (!is.null(s) && check_observation_period && (s_coeff == 1 || identical(s_coeff, c(1)))) {
      clauses <- c(clauses, paste0(start_evt, " >= P.OP_END_DATE"))
    }
    if (!is.null(e) && !is.null(e_days)) {
      clauses <- c(clauses, paste0(start_evt, " <= DATEADD(day,", e_coeff * e_days, ",", start_idx, ")"))
    } else if (!is.null(e) && check_observation_period && (e_coeff == -1 || identical(e_coeff, c(-1)))) {
      clauses <- c(clauses, paste0(start_evt, " <= P.OP_START_DATE"))
    } else if (!is.null(e) && check_observation_period && (e_coeff == 1 || identical(e_coeff, c(1)))) {
      clauses <- c(clauses, paste0(start_evt, " <= P.OP_END_DATE"))
    }
  }
  if (!is.null(end_window)) {
    use_end <- scalar(end_window$UseIndexEnd %||% end_window$useIndexEnd)
    use_evt_end <- scalar(end_window$UseEventEnd %||% end_window$useEventEnd)
    end_idx <- if (isTRUE(use_end)) "P.END_DATE" else "P.START_DATE"
    end_evt <- if (isFALSE(use_evt_end)) "A.START_DATE" else "A.END_DATE"
    s <- end_window$Start %||% end_window$start
    e <- end_window$End %||% end_window$end
    s_days <- scalar(s$Days %||% s$days)
    s_coeff <- scalar(s$Coeff %||% s$coeff) %||% 1
    e_days <- scalar(e$Days %||% e$days)
    e_coeff <- scalar(e$Coeff %||% e$coeff) %||% 1
    if (!is.null(s) && !is.null(s_days)) {
      clauses <- c(clauses, paste0(end_evt, " >= DATEADD(day,", s_coeff * s_days, ",", end_idx, ")"))
    } else if (!is.null(s) && check_observation_period && (s_coeff == -1 || identical(s_coeff, c(-1)))) {
      clauses <- c(clauses, paste0(end_evt, " >= P.OP_START_DATE"))
    } else if (!is.null(s) && check_observation_period && (s_coeff == 1 || identical(s_coeff, c(1)))) {
      clauses <- c(clauses, paste0(end_evt, " >= P.OP_END_DATE"))
    }
    if (!is.null(e) && !is.null(e_days)) {
      clauses <- c(clauses, paste0(end_evt, " <= DATEADD(day,", e_coeff * e_days, ",", end_idx, ")"))
    } else if (!is.null(e) && check_observation_period && (e_coeff == -1 || identical(e_coeff, c(-1)))) {
      clauses <- c(clauses, paste0(end_evt, " <= P.OP_START_DATE"))
    } else if (!is.null(e) && check_observation_period && (e_coeff == 1 || identical(e_coeff, c(1)))) {
      clauses <- c(clauses, paste0(end_evt, " <= P.OP_END_DATE"))
    }
  }
  if (length(clauses) > 0) paste0(" AND ", paste(clauses, collapse = " AND ")) else ""
}

#' Get occurrence operator from type (0=Exactly, 1=AtMost, 2=AtLeast)
get_occurrence_operator <- function(occ_type) {
  switch(as.character(occ_type), "0" = "=", "1" = "<=", "2" = ">=", "=")
}

#' Get domain concept column for criteria type (for COUNT DISTINCT)
get_domain_concept_col <- function(criteria) {
  ext <- extract_criteria(criteria)
  if (is.null(ext)) return(NULL)
  type_to_col <- c(ConditionOccurrence = "condition_concept_id", DrugExposure = "drug_concept_id",
    ProcedureOccurrence = "procedure_concept_id", VisitOccurrence = "visit_concept_id",
    Observation = "observation_concept_id", Measurement = "measurement_concept_id",
    DeviceExposure = "device_concept_id", Specimen = "specimen_concept_id", Death = "cause_concept_id",
    ConditionEra = "condition_concept_id", DrugEra = "drug_concept_id", DoseEra = "drug_concept_id",
    VisitDetail = "visit_detail_concept_id", ObservationPeriod = "period_type_concept_id")
  type_to_col[[ext$type]]
}

#' Get correlated criteria query for a criteria list item (Criteria + StartWindow + Occurrence)
get_corelated_criteria_query <- function(correlated_item, event_table, index_id, cdm_schema) {
  occ <- correlated_item$Occurrence %||% correlated_item$occurrence
  occ_type <- occ$Type %||% occ$type %||% 2
  occ_count <- occ$Count %||% occ$count %||% 1
  is_distinct <- isTRUE(occ$IsDistinct %||% occ$isDistinct)
  count_col <- occ$CountColumn %||% occ$countColumn
  template <- if (occ_type == 1 || occ_count == 0) ADDITIONAL_CRITERIA_LEFT_TEMPLATE else ADDITIONAL_CRITERIA_INNER_TEMPLATE
  inner <- correlated_item$Criteria %||% correlated_item$criteria
  if (is.null(inner)) return("")
  criteria_sql <- get_criteria_sql(inner, cdm_schema)
  is_temp <- nchar(event_table) > 0 && substr(trimws(event_table), 1, 1) == "#"
  additional_cols <- ""
  count_col_expr <- "cc.event_id"
  if (is_distinct) {
    # Map CountColumn to actual SQL column name
    if (!is.null(count_col) && is.character(count_col) && toupper(count_col) == "START_DATE") {
      dom_col <- "start_date"
    } else if (!is.null(count_col) && is.character(count_col) && toupper(count_col) == "DOMAIN_CONCEPT") {
      # DOMAIN_CONCEPT is a Java-side alias; map to the actual column
      dom_col <- get_domain_concept_col(inner)
    } else {
      dom_col <- if (!is.null(count_col) && is.character(count_col)) count_col else get_domain_concept_col(inner)
    }
    if (!is.null(dom_col)) {
      additional_cols <- ", A.domain_concept_id"
      count_col_expr <- "cc.domain_concept_id"
      # Wrap criteria SQL to project dom_col as domain_concept_id
      # (mirrors Java CirceR: SELECT DC.*, DC.<col> as domain_concept_id FROM (...) DC)
      if (!grepl("as domain_concept_id", criteria_sql, ignore.case = TRUE)) {
        criteria_sql <- paste0(
          "SELECT DC.*, DC.", dom_col, " as domain_concept_id FROM (\n",
          criteria_sql, "\n) DC"
        )
      }
    }
  }
  if (!is_temp && (grepl("SELECT", event_table, ignore.case = TRUE) || grepl("FROM", event_table, ignore.case = TRUE))) {
    if (!grepl("op_start_date", event_table, ignore.case = TRUE)) {
      event_table <- paste0("(SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date\nFROM (\n", event_table, "\n) Q\nJOIN ", cdm_schema, ".OBSERVATION_PERIOD OP on Q.person_id = OP.person_id and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date)")
    }
  }
  window_clause <- build_window_criteria(
    correlated_item$StartWindow %||% correlated_item$startWindow,
    correlated_item$EndWindow %||% correlated_item$endWindow,
    !isTRUE(correlated_item$IgnoreObservationPeriod %||% correlated_item$ignoreObservationPeriod %||% FALSE)
  )
  restrict_visit <- correlated_item$RestrictVisit %||% correlated_item$restrictVisit
  if (isTRUE(restrict_visit)) window_clause <- paste0(window_clause, " AND A.visit_occurrence_id = P.visit_occurrence_id")
  query <- template
  query <- gsub("@criteriaQuery", criteria_sql, query, fixed = TRUE)
  query <- gsub("@eventTable", event_table, query, fixed = TRUE)
  query <- gsub("@additionalColumns", additional_cols, query, fixed = TRUE)
  query <- gsub("@windowCriteria", window_clause, query, fixed = TRUE)
  query <- gsub("@indexId", as.character(index_id), query, fixed = TRUE)
  occ_op <- get_occurrence_operator(occ_type)
  distinct_str <- if (is_distinct) "DISTINCT " else ""
  query <- gsub("@occurrenceCriteria", paste0("HAVING COUNT(", distinct_str, count_col_expr, ") ", occ_op, " ", occ_count), query, fixed = TRUE)
  query
}

#' Get demographic criteria query
#' @param dc List - DemographicCriteria (OccurrenceStartDate, OccurrenceEndDate, Age, Gender, etc.)
#' @param event_table Event table expression
#' @param index_id Index for this criteria
#' @param cdm_schema CDM schema
get_demographic_criteria_query <- function(dc, event_table, index_id, cdm_schema = "@cdm_database_schema") {
  where_parts <- character(0)
  osd <- dc$OccurrenceStartDate %||% dc$occurrenceStartDate
  oed <- dc$OccurrenceEndDate %||% dc$occurrenceEndDate
  if (!is.null(osd)) { cl <- build_date_range_clause("E.start_date", osd); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  if (!is.null(oed)) { cl <- build_date_range_clause("E.end_date", oed); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  age <- dc$Age %||% dc$age
  if (!is.null(age)) { cl <- build_numeric_range_clause("YEAR(E.start_date) - P.year_of_birth", age); if (!is.null(cl)) where_parts <- c(where_parts, cl) }
  gender <- dc$Gender %||% dc$gender
  if (length(gender) > 0) { ids <- get_concept_ids(gender); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.gender_concept_id in (", paste(ids, collapse = ","), ")")) }
  gcs <- dc$GenderCS %||% dc$genderCS
  if (!is.null(gcs) && !is.null(gcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(gcs$CodesetId, "P.gender_concept_id", gcs$IsExclusion %||% FALSE))
  race <- dc$Race %||% dc$race
  if (length(race) > 0) { ids <- get_concept_ids(race); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.race_concept_id in (", paste(ids, collapse = ","), ")")) }
  rcs <- dc$RaceCS %||% dc$raceCS
  if (!is.null(rcs) && !is.null(rcs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(rcs$CodesetId, "P.race_concept_id", rcs$IsExclusion %||% FALSE))
  ethnicity <- dc$Ethnicity %||% dc$ethnicity
  if (length(ethnicity) > 0) { ids <- get_concept_ids(ethnicity); if (length(ids) > 0) where_parts <- c(where_parts, paste0("P.ethnicity_concept_id in (", paste(ids, collapse = ","), ")")) }
  ecs <- dc$EthnicityCS %||% dc$ethnicityCS
  if (!is.null(ecs) && !is.null(ecs$CodesetId)) where_parts <- c(where_parts, get_codeset_in_expression(ecs$CodesetId, "P.ethnicity_concept_id", ecs$IsExclusion %||% FALSE))
  where_clause <- if (length(where_parts) > 0) paste0("WHERE ", paste(where_parts, collapse = " AND ")) else ""
  paste0("-- Begin Demographic Criteria\nSELECT ", index_id, " as index_id, e.person_id, e.event_id\nFROM ", event_table, " E\nJOIN ", cdm_schema, ".PERSON P ON P.person_id = E.person_id\n", where_clause, "\nGROUP BY e.person_id, e.event_id\n-- End Demographic Criteria")
}

#' Get criteria group query
get_criteria_group_query <- function(group, event_table, cdm_schema = "@cdm_database_schema") {
  criteria_list <- group$CriteriaList %||% group$criteriaList %||% list()
  groups <- group$Groups %||% group$groups %||% list()
  demo_list <- group$DemographicCriteriaList %||% group$demographicCriteriaList %||% list()
  ac_queries <- character(0)
  index_id <- 0
  for (cc in criteria_list) {
    inner <- cc$Criteria %||% cc$criteria
    ext <- if (!is.null(inner)) extract_criteria(list(Criteria = inner)) else NULL
    inner_type <- if (!is.null(ext)) ext$type else ""
    # CirceR: ObservationPeriod uses nested Criteria Group (CorrelatedCriteria) as event table
    corr <- inner$ObservationPeriod$CorrelatedCriteria %||% inner$ObservationPeriod$correlatedCriteria
    if (inner_type == "ObservationPeriod" && !is.null(corr) && !is_group_empty(corr)) {
      op_sql <- get_criteria_sql(inner, cdm_schema)
      op_event_table <- paste0("(SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date\nFROM (\n", op_sql, "\n) Q\nJOIN ", cdm_schema, ".OBSERVATION_PERIOD OP on Q.person_id = OP.person_id and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date)")
      sub_group <- corr
      sub_cg <- get_criteria_group_query(sub_group, op_event_table, cdm_schema)
      sub_cg <- gsub("@indexId", "0", sub_cg, fixed = TRUE)
      wrap_pe <- paste0("select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.visit_occurrence_id, PE.sort_date FROM (\n", op_sql, "\n) PE\nJOIN (\n", sub_cg, "\n) AC on AC.person_id = PE.person_id and AC.event_id = PE.event_id")
      occ <- cc$Occurrence %||% cc$occurrence
      occ_type <- occ$Type %||% occ$type %||% 2
      occ_count <- occ$Count %||% occ$count %||% 1
      window_clause <- build_window_criteria(
        cc$StartWindow %||% cc$startWindow,
        cc$EndWindow %||% cc$endWindow,
        !isTRUE(cc$IgnoreObservationPeriod %||% cc$ignoreObservationPeriod %||% FALSE)
      )
      occ_op <- get_occurrence_operator(occ_type)
      q <- paste0("-- Begin Correlated Criteria\nselect ", index_id, " as index_id, cc.person_id, cc.event_id\nfrom (SELECT p.person_id, p.event_id \nFROM ", event_table, " P\nJOIN (\n", wrap_pe, "\n) A on A.person_id = P.person_id ", window_clause, " ) cc \nGROUP BY cc.person_id, cc.event_id\nHAVING COUNT(cc.event_id) ", occ_op, " ", occ_count, "\n-- End Correlated Criteria")
      ac_queries <- c(ac_queries, q)
    } else {
      ac_queries <- c(ac_queries, get_corelated_criteria_query(cc, event_table, index_id, cdm_schema))
    }
    index_id <- index_id + 1
  }
  for (dc in demo_list) {
    ac_queries <- c(ac_queries, get_demographic_criteria_query(dc, event_table, index_id, cdm_schema))
    index_id <- index_id + 1
  }
  for (g in groups) {
    gq <- get_criteria_group_query(g, event_table, cdm_schema)
    ac_queries <- c(ac_queries, gsub("@indexId", as.character(index_id), gq, fixed = TRUE))
    index_id <- index_id + 1
  }
  total <- index_id
  if (total == 0) return(paste0("-- Begin Criteria Group\nSELECT @indexId as index_id, person_id, event_id FROM ", event_table, "\n-- End Criteria Group\n"))
  grp_type <- toupper(group$Type %||% group$type %||% "ALL")
  join_type <- "INNER"
  occ_clause <- switch(grp_type,
    "ALL" = paste0("HAVING COUNT(index_id) = ", total),
    "ANY" = "HAVING COUNT(index_id) > 0",
    "AT_LEAST" = paste0("HAVING COUNT(index_id) >= ", group$Count %||% group$count %||% 1),
    "AT_MOST" = {
      join_type <- "LEFT"
      paste0("HAVING COUNT(index_id) <= ", group$Count %||% group$count %||% 0)
    },
    paste0("HAVING COUNT(index_id) = ", total)
  )
  query <- GROUP_QUERY_TEMPLATE
  query <- gsub("@eventTable", event_table, query, fixed = TRUE)
  query <- gsub("@criteriaQueries", paste(ac_queries, collapse = "\nUNION ALL\n"), query, fixed = TRUE)
  query <- gsub("@joinType", join_type, query, fixed = TRUE)
  query <- gsub("@occurrenceCountClause", occ_clause, query, fixed = TRUE)
  query <- gsub("@indexId", "0", query, fixed = TRUE)
  query
}

#' Check if criteria group is empty
is_group_empty <- function(group) {
  cl <- length(group$CriteriaList %||% group$criteriaList %||% list())
  gr <- length(group$Groups %||% group$groups %||% list())
  dl <- length(group$DemographicCriteriaList %||% group$demographicCriteriaList %||% list())
  cl == 0 && gr == 0 && dl == 0
}

#' Wrap base criteria query with correlated criteria group
wrap_criteria_query <- function(base_query, group, cdm_schema = "@cdm_database_schema") {
  q_op <- paste0("SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date\nFROM (\n", base_query, "\n) Q\nJOIN ", cdm_schema, ".OBSERVATION_PERIOD OP on Q.person_id = OP.person_id and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date")
  event_table <- paste0("(\n", q_op, "\n)")
  group_query <- get_criteria_group_query(group, event_table, cdm_schema)
  group_query <- gsub("@indexId", "0", group_query, fixed = TRUE)
  paste0("select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.visit_occurrence_id, PE.sort_date FROM (\n", base_query, "\n) PE\nJOIN (\n", group_query, "\n) AC on AC.person_id = PE.person_id and AC.event_id = PE.event_id")
}

#' Build cohort query from cohort expression
#'
#' @param cohort List - cohort expression (from cohort_expression_from_json)
#' @param options List - build options with cdm_schema, vocabulary_schema, target_table, cohort_id, etc.
#' @return List with \code{full_sql} (complete script), \code{tail_sql}, \code{primary_events_sql}, \code{codeset_sql}, and other fragments for batch assembly.
build_cohort_query_internal <- function(cohort, options = list()) {
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  vocab_schema <- options$vocabulary_schema %||% "@vocabulary_database_schema"
  target_table <- options$target_table %||% "@target_database_schema.@target_cohort_table"
  cohort_id <- options$cohort_id %||% 1
  cohort_id_field <- options$cohort_id_field_name %||% "cohort_definition_id"
  era_pad <- (cohort$CollapseSettings %||% cohort$collapseSettings)$EraPad %||% 0

  # Codeset query
  concept_sets <- get_key(cohort, c("ConceptSets", "conceptSets")) %||% list()
  union_parts <- character(0)  # saved for structured output (batch codeset build)
  if (length(concept_sets) == 0) {
    codeset_query <- gsub("@codesetInserts", "", CODESET_QUERY_TEMPLATE)
  } else {
    for (cs in concept_sets) {
      cs_id <- get_key(cs, c("id", "ID", "Id"))
      if (is.null(cs_id)) next
      expr <- get_key(cs, c("expression", "Expression"))
      if (is.null(expr)) next
      expr_sql <- build_concept_set_expression_query(expr, vocab_schema)
      union_parts <- c(union_parts, paste0("SELECT ", cs_id, " as codeset_id, c.concept_id FROM (", expr_sql, "\n) C"))
    }
    codeset_inserts <- paste0("INSERT INTO #Codesets (codeset_id, concept_id)\n", paste(union_parts, collapse = " UNION ALL \n"), ";")
    codeset_query <- sub("@codesetInserts", codeset_inserts, CODESET_QUERY_TEMPLATE)
  }

  # Primary events subquery
  pc <- get_key(cohort, c("PrimaryCriteria", "primaryCriteria"))
  if (is.null(pc)) stop("Cohort must have PrimaryCriteria")

  criteria_list <- get_key(pc, c("CriteriaList", "criteriaList")) %||% list()
  criteria_queries <- character(0)
  for (cr in criteria_list) {
    criteria_queries <- c(criteria_queries, get_criteria_sql(cr, cdm_schema))
  }
  primary_events_sub <- sub("@criteriaQueries", paste(criteria_queries, collapse = "\nUNION ALL\n"), PRIMARY_EVENTS_SUBQUERY_TEMPLATE)

  ow <- get_key(pc, c("ObservationWindow", "observationWindow"))
  prior_days <- (ow$PriorDays %||% ow$priorDays) %||% 0
  post_days <- (ow$PostDays %||% ow$postDays) %||% 0
  primary_events_filter <- paste0("DATEADD(day,", prior_days, ",OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,", post_days, ",E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE")
  primary_events_sub <- sub("@primaryEventsFilter", primary_events_filter, primary_events_sub)

  pl <- get_key(pc, c("PrimaryCriteriaLimit", "PrimaryLimit", "primaryLimit"))
  event_sort <- if (identical((pl$Type %||% pl$type), "Last")) "DESC" else "ASC"
  primary_events_sub <- sub("@EventSort", event_sort, primary_events_sub)
  primary_event_limit <- if (identical((pl$Type %||% pl$type), "All")) "" else "WHERE P.ordinal = 1"
  primary_events_sub <- sub("@primaryEventLimit", primary_event_limit, primary_events_sub)

  primary_events_query <- sub("@primaryEventsSubQuery", primary_events_sub, PRIMARY_EVENTS_TEMPLATE)
  add_criteria <- get_key(cohort, c("AdditionalCriteria", "additionalCriteria"))
  if (!is.null(add_criteria) && !is_group_empty(add_criteria)) {
    event_table <- paste0("(\n-- Begin Primary Events\n", primary_events_sub, "\n-- End Primary Events\n)")
    add_criteria_sql <- get_criteria_group_query(add_criteria, event_table, cdm_schema)
    add_criteria_sql <- gsub("@indexId", "0", add_criteria_sql, fixed = TRUE)
    primary_events_query <- sub("@additionalCriteriaQuery",
      paste0("\nJOIN (\n", add_criteria_sql, ") AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id"),
      primary_events_query)
  } else {
    primary_events_query <- sub("@additionalCriteriaQuery", "", primary_events_query)
  }

  ql <- get_key(cohort, c("QualifiedLimit", "qualifiedLimit"))
  qualified_sort <- if (identical((ql$Type %||% ql$type), "Last")) "DESC" else "ASC"
  primary_events_query <- sub("@QualifiedEventSort", qualified_sort, primary_events_query)
  # Apply QualifiedLimit "First"/"Last" whenever set (take first/last qualified event per person); do not gate on additional criteria
  qualified_limit_filter <- if (!is.null(ql) && !identical(toupper(ql$Type %||% ql$type %||% ""), "ALL")) {
    "WHERE QE.ordinal = 1"
  } else {
    ""
  }
  primary_events_query <- sub("@QualifiedLimitFilter", qualified_limit_filter, primary_events_query)

  # Inclusion rules
  inclusion_rules <- get_key(cohort, c("InclusionRules", "inclusionRules")) %||% list()
  if (length(inclusion_rules) > 0) {
    ir_inserts <- character(0)
    ir_tables <- character(0)
    for (i in seq_along(inclusion_rules)) {
      rule_idx <- i - 1
      ir_expr <- get_key(inclusion_rules[[i]], c("expression", "Expression"))
      cg_sql <- get_criteria_group_query(ir_expr, "#qualified_events", cdm_schema)
      cg_sql <- gsub("@indexId", "0", cg_sql, fixed = TRUE)
      rule_query <- paste0(
        "select ", rule_idx, " as inclusion_rule_id, person_id, event_id\nINTO #Inclusion_", rule_idx, "\nFROM (\n",
        "  select pe.person_id, pe.event_id\n  FROM #qualified_events pe\n",
        "  JOIN (\n", cg_sql, ") AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id\n",
        ") Results\n;")
      ir_inserts <- c(ir_inserts, rule_query)
      ir_tables <- c(ir_tables, paste0("#Inclusion_", rule_idx))
    }
    ir_union <- paste0("select inclusion_rule_id, person_id, event_id from ", paste(ir_tables, collapse = "\nUNION ALL\nselect inclusion_rule_id, person_id, event_id from "))
    ir_inserts <- c(ir_inserts, paste0("SELECT inclusion_rule_id, person_id, event_id\nINTO #inclusion_events\nFROM (", ir_union, ") I;"))
    for (tbl in ir_tables) {
      ir_inserts <- c(ir_inserts, paste0("TRUNCATE TABLE ", tbl, ";\nDROP TABLE ", tbl, ";\n"))
    }
    inclusion_cohort_inserts <- paste(ir_inserts, collapse = "\n")
  } else {
    inclusion_cohort_inserts <- "create table #inclusion_events (inclusion_rule_id bigint,\n\tperson_id bigint,\n\tevent_id bigint\n);"
  }

  # Included events
  el <- get_key(cohort, c("ExpressionLimit", "expressionLimit"))
  included_sort <- if (identical((el$Type %||% el$type), "Last")) "DESC" else "ASC"
  included_events_query <- sub("@IncludedEventSort", included_sort, INCLUDED_EVENTS_TEMPLATE)
  result_limit_filter <- if (!is.null(el) && !identical((el$Type %||% el$type), "All")) "where results.ordinal = 1" else ""
  included_events_query <- sub("@ResultLimitFilter", result_limit_filter, included_events_query)
  rule_count <- length(inclusion_rules)
  # Resolve at R generation time: rule_count is known, no need for SqlRender {N != 0}?{...} conditional
  # (avoids DuckDB parser errors when translate() is called before render())
  if (rule_count != 0) {
    inclusion_mask_filter <- paste0("\n  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask\n  where (MG.inclusion_rule_mask = POWER(cast(2 as bigint),", rule_count, ")-1)\n")
  } else {
    inclusion_mask_filter <- ""
  }
  included_events_query <- sub("@InclusionRuleMaskFilter", inclusion_mask_filter, included_events_query)

  # End strategy
  end_strategy <- get_key(cohort, c("EndStrategy", "endStrategy"))
  cohort_end_unions <- character(0)
  strategy_ends_temp <- ""
  strategy_ends_cleanup <- ""

  # Only add default "by default" when we do NOT have DateOffset/CustomEra strategy
  has_custom_end <- FALSE
  if (!is.null(end_strategy)) {
    doff <- end_strategy$DateOffset %||% end_strategy$dateOffset
    if (!is.null(doff)) {
      has_custom_end <- TRUE
      offset <- as.character(doff$Offset %||% doff$offset %||% 0)
      date_field <- (doff$DateField %||% doff$dateField %||% "StartDate")
      date_col <- if (identical(date_field, "StartDate")) "start_date" else "end_date"
      strategy_sql <- gsub("@eventTable", "#included_events", DATE_OFFSET_STRATEGY_TEMPLATE, fixed = TRUE)
      strategy_sql <- gsub("@offset", offset, strategy_sql, fixed = TRUE)
      strategy_sql <- gsub("@dateField", date_col, strategy_sql, fixed = TRUE)
      strategy_ends_temp <- strategy_sql
      strategy_ends_cleanup <- "TRUNCATE TABLE #strategy_ends;\nDROP TABLE #strategy_ends;\n"
      cohort_end_unions <- c(cohort_end_unions, "-- End Date Strategy\nselect event_id, person_id, end_date from #strategy_ends")
    }
    custom_era <- end_strategy$CustomEra %||% end_strategy$customEra
    if (!is.null(custom_era)) {
      has_custom_end <- TRUE
      drug_codeset <- custom_era$DrugCodesetId %||% custom_era$drugCodesetId
      cond_codeset <- custom_era$ConditionCodesetId %||% custom_era$conditionCodesetId
      if (!is.null(drug_codeset)) {
        strategy_sql <- gsub("@eventTable", "#included_events", CUSTOM_ERA_DRUG_TEMPLATE, fixed = TRUE)
        strategy_sql <- gsub("@DrugCodesetId", as.character(drug_codeset), strategy_sql, fixed = TRUE)
        strategy_sql <- gsub("@cdm_database_schema", cdm_schema, strategy_sql, fixed = TRUE)
        strategy_ends_temp <- strategy_sql
        strategy_ends_cleanup <- "TRUNCATE TABLE #strategy_ends;\nDROP TABLE #strategy_ends;\n"
        cohort_end_unions <- c(cohort_end_unions, "-- CustomEra Drug\nselect event_id, person_id, end_date from #strategy_ends")
      } else if (!is.null(cond_codeset)) {
        strategy_sql <- gsub("@eventTable", "#included_events", CUSTOM_ERA_CONDITION_TEMPLATE, fixed = TRUE)
        strategy_sql <- gsub("@ConditionCodesetId", as.character(cond_codeset), strategy_sql, fixed = TRUE)
        strategy_sql <- gsub("@cdm_database_schema", cdm_schema, strategy_sql, fixed = TRUE)
        strategy_ends_temp <- strategy_sql
        strategy_ends_cleanup <- "TRUNCATE TABLE #strategy_ends;\nDROP TABLE #strategy_ends;\n"
        cohort_end_unions <- c(cohort_end_unions, "-- CustomEra Condition\nselect event_id, person_id, end_date from #strategy_ends")
      }
    }
  }
  if (!has_custom_end) {
    cohort_end_unions <- c("-- By default, cohort exit at the event's op end date\nselect event_id, person_id, op_end_date as end_date from #included_events", cohort_end_unions)
  }

  # Censoring criteria
  censoring <- get_key(cohort, c("CensoringCriteria", "censoringCriteria")) %||% list()
  if (length(censoring) > 0) {
    censor_queries <- character(0)
    for (cc in censoring) {
      cq <- get_criteria_sql(cc, cdm_schema)
      censor_queries <- c(censor_queries, paste0(
        "select i.event_id, i.person_id, MIN(c.start_date) as end_date\nfrom #included_events i\njoin\n(\n", cq, "\n) C on C.person_id = I.person_id and C.start_date >= I.start_date and C.START_DATE <= I.op_end_date\ngroup by i.event_id, i.person_id"))
    }
    cohort_end_unions <- c(cohort_end_unions, paste0("-- Censor Events\n", paste(censor_queries, collapse = "\nUNION ALL\n")))
  }

  cohort_end_unions <- paste(cohort_end_unions, collapse = "\nUNION ALL\n")

  # Final cohort query
  censor_window <- get_key(cohort, c("CensorWindow", "censorWindow"))
  start_date <- "start_date"
  end_date <- "end_date"
  if (!is.null(censor_window) && (!is.null(censor_window$startDate) || !is.null(censor_window$endDate))) {
    if (!is.null(censor_window$startDate)) start_date <- paste0("CASE WHEN start_date > ", date_string_to_sql(censor_window$startDate), " THEN start_date ELSE ", date_string_to_sql(censor_window$startDate), " END")
    if (!is.null(censor_window$endDate)) end_date <- paste0("CASE WHEN end_date < ", date_string_to_sql(censor_window$endDate), " THEN end_date ELSE ", date_string_to_sql(censor_window$endDate), " END")
  }
  final_cohort_query <- paste0("select ", cohort_id, " as ", cohort_id_field, ", person_id, ", start_date, ", ", end_date, " \nFROM #final_cohort CO")

  # Tail fragments (for structured output and batch assembler)
  cohort_rows_and_final_block <- sub("@cohort_end_unions", cohort_end_unions, COHORT_ROWS_AND_FINAL_TEMPLATE, fixed = TRUE)
  cohort_rows_and_final_block <- gsub("@eraconstructorpad", as.character(era_pad), cohort_rows_and_final_block, fixed = TRUE)
  target_insert_block <- paste0(
    "delete from @target_database_schema.@target_cohort_table where @cohort_id_field_name = @target_cohort_id;\n",
    "insert into @target_database_schema.@target_cohort_table (@cohort_id_field_name, subject_id, cohort_start_date, cohort_end_date)\n",
    final_cohort_query, "\n;\n"
  )

  # Assembly
  result_sql <- COHORT_QUERY_TEMPLATE
  result_sql <- sub("@codesetQuery", codeset_query, result_sql)
  result_sql <- sub("@primaryEventsQuery", primary_events_query, result_sql)
  result_sql <- sub("@inclusionCohortInserts", inclusion_cohort_inserts, result_sql)
  result_sql <- sub("@includedEventsQuery", included_events_query, result_sql)
  result_sql <- sub("@strategy_ends_temp_tables", strategy_ends_temp, result_sql)
  result_sql <- sub("@cohortRowsAndFinalBlock", cohort_rows_and_final_block, result_sql)
  result_sql <- sub("@finalCohortQuery", final_cohort_query, result_sql)
  generate_stats <- isTRUE(options$generate_stats)
  # Resolve all conditionals at R generation time: generate_stats and rule_count are known.
  # Do NOT emit {N != 0}?{...} SqlRender conditionals — translate() may be called before
  # render() in the batch path, causing DuckDB parser errors.
  if (generate_stats) {
    censored <- "\n-- BEGIN: Censored Stats\n\ndelete from @results_database_schema.cohort_censor_stats where @cohort_id_field_name = @target_cohort_id;\n\n-- END: Censored Stats\n\n"
  } else {
    censored <- ""
  }
  if (generate_stats && rule_count != 0) {
    inclusion_rule_table <- if (rule_count == 0) {
      "CREATE TABLE #inclusion_rules (rule_sequence int);"
    } else {
      ir_unions <- paste0("SELECT CAST(", seq_len(rule_count) - 1, " as int) as rule_sequence", collapse = " UNION ALL ")
      sub("@inclusionRuleUnions", ir_unions, INCLUSION_RULE_TEMP_TABLE_TEMPLATE)
    }
    inc_impact_event <- gsub("@eventTable", "#qualified_events", gsub("@inclusionImpactMode", "0", COHORT_INCLUSION_ANALYSIS_TEMPLATE))
    inc_impact_person <- gsub("@eventTable", "#best_events", gsub("@inclusionImpactMode", "1", COHORT_INCLUSION_ANALYSIS_TEMPLATE))
    inc_section <- paste0(
      inclusion_rule_table, "\n",
      BEST_EVENTS_QUERY, "\n",
      "-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)\n",
      "-- 0: all events\n-- 1: best event\n\n",
      "-- BEGIN: Inclusion Impact Analysis - event\n", inc_impact_event,
      "-- END: Inclusion Impact Analysis - event\n\n",
      "-- BEGIN: Inclusion Impact Analysis - person\n", inc_impact_person,
      "-- END: Inclusion Impact Analysis - person\n\n",
      "TRUNCATE TABLE #best_events;\nDROP TABLE #best_events;\n\nTRUNCATE TABLE #inclusion_rules;\nDROP TABLE #inclusion_rules;\n"
    )
    inc_block <- paste0("\n\n", inc_section, "\n")
  } else {
    inc_block <- ""
  }
  inclusion_analysis_query <- paste0(censored, inc_block)
  result_sql <- sub("@inclusionAnalysisQuery", inclusion_analysis_query, result_sql)
  result_sql <- sub("@strategy_ends_cleanup", strategy_ends_cleanup, result_sql)
  result_sql <- sub("@cleanupBlock", CLEANUP_BLOCK, result_sql)

  result_sql <- gsub("@cdm_database_schema", cdm_schema, result_sql)
  result_sql <- gsub("@vocabulary_database_schema", vocab_schema, result_sql)
  result_sql <- gsub("@target_database_schema.@target_cohort_table", target_table, result_sql, fixed = TRUE)
  result_sql <- gsub("@target_cohort_id", as.character(cohort_id), result_sql)
  result_sql <- gsub("@cohort_id_field_name", cohort_id_field, result_sql)

  tail_sql <- paste(
    inclusion_cohort_inserts,
    included_events_query,
    strategy_ends_temp,
    cohort_rows_and_final_block,
    target_insert_block,
    inclusion_analysis_query,
    strategy_ends_cleanup,
    CLEANUP_BLOCK,
    sep = "\n"
  )

  list(
    full_sql = result_sql,
    codeset_sql = codeset_query,
    codeset_union_parts = union_parts,
    primary_events_sql = primary_events_query,
    inclusion_sql = inclusion_cohort_inserts,
    included_events_sql = included_events_query,
    strategy_ends_sql = strategy_ends_temp,
    cohort_end_unions = cohort_end_unions,
    final_cohort_sql = final_cohort_query,
    cleanup_sql = CLEANUP_BLOCK,
    strategy_ends_cleanup = strategy_ends_cleanup,
    inclusion_analysis_sql = inclusion_analysis_query,
    era_pad = as.character(era_pad),
    cohort_id = cohort_id,
    has_qualified_limit = !is.null(ql) && !identical(toupper(ql$Type %||% ql$type %||% ""), "ALL"),
    has_additional_criteria = !is.null(add_criteria) && !is_group_empty(add_criteria),
    tail_sql = tail_sql
  )
}
# CIRCE API - cohort JSON and SQL generation (internal)

#' Load cohort expression from JSON
#' @param json_str Character string containing cohort definition JSON (or path to JSON file)
#' @return List structure representing the cohort expression
cohortExpressionFromJson <- function(json_str) {
  if (length(json_str) != 1 || !is.character(json_str)) {
    stop("json_str must be a single character string")
  }
  if (file.exists(json_str)) {
    json_str <- paste(readLines(json_str, warn = FALSE), collapse = "\n")
  }
  cohort_expression_from_json(json_str)
}

#' Generate SQL from cohort expression
#' @param cohort List or character - cohort expression (from cohortExpressionFromJson) or JSON string
#' @param options List - build options (optional)
#' @return Character - SQL query string
buildCohortQuery <- function(cohort, options = NULL) {
  if (is.null(options)) options <- list()
  if (is.character(cohort)) {
    cohort <- cohortExpressionFromJson(cohort)
  }
  result <- build_cohort_query_internal(cohort, options)
  if (is.list(result)) result$full_sql else result
}
