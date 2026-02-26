# execution_graph.R
# Execution graph (DAG) for batch cohort SQL generation.
# Decomposes cohort definitions into computation nodes with content-based keys.
# Identical sub-computations across cohorts are detected and computed once.

#' Build a schema-qualified table name from a bare name and options.
#' @param bare_name Bare table name (e.g., "codesets", "pe_a1b2c3d4").
#' @param options List with \code{results_schema} and \code{table_prefix}.
#' @return Qualified name like \code{"@results_schema.atlas_xxx_codesets"}.
#' @noRd
qualify_table <- function(bare_name, options) {
  paste0(options$results_schema, ".", options$table_prefix, tolower(bare_name))
}

# ---- Canonical hashing ----

#' Fast deterministic hash from a character string.
#' Uses DJB2-like algorithm with double arithmetic to avoid R integer overflow.
#' Returns 16-char hex string.
#' @param s Character string
#' @return Character hash
#' @noRd
string_hash <- function(s) {
  raw_bytes <- charToRaw(s)
  MOD <- 2^32  # use doubles with modular arithmetic to avoid integer overflow
  h1 <- 5381
  h2 <- 7919
  for (b in as.integer(raw_bytes)) {
    h1 <- ((h1 * 33) + b) %% MOD
    h2 <- ((h2 * 131) + b) %% MOD
  }
  sprintf("%08x%08x", as.integer(h1 %% 2^31), as.integer(h2 %% 2^31))
}

#' Compute canonical hash using string_hash on normalized JSON.
#' @noRd
canonical_hash <- function(type, definition) {
  normalized <- normalize_for_hash(type, definition)
  json_str <- jsonlite::toJSON(normalized, auto_unbox = TRUE, null = "null",
                                digits = NA, force = TRUE)
  string_hash(as.character(json_str))
}

#' Dispatch normalization by node type.
#' @noRd
normalize_for_hash <- function(type, definition) {
  fn <- switch(type,
    concept_set = normalize_concept_set_for_hash,
    primary_events = normalize_primary_events_for_hash,
    qualified_events = normalize_qualified_events_for_hash,
    inclusion_rule = normalize_inclusion_rule_for_hash,
    included_events = normalize_included_events_for_hash,
    cohort_exit = normalize_cohort_exit_for_hash,
    final_cohort = normalize_final_cohort_for_hash,
    criteria_group = normalize_criteria_group_for_hash,
    NULL
  )
  if (is.null(fn)) return(list(type = type, raw = definition))
  fn(definition)
}

# ---- Per-type normalizers ----

#' Normalize concept set expression for hashing.
#' Hashes by items: (concept_id, is_excluded, include_descendants, include_mapped).
#' Two cohorts with different codeset IDs but identical expressions get the same hash.
#' @noRd
normalize_concept_set_for_hash <- function(expr) {
  items <- expr$items %||% list()
  # Normalize each item and sort by concept_id for determinism
  normalized <- lapply(items, function(item) {
    list(
      concept_id = as.integer(item$concept$CONCEPT_ID %||% item$concept$conceptId %||%
                                item$concept$ConceptId %||% 0L),
      is_excluded = isTRUE(item$isExcluded %||% item$is_excluded),
      include_descendants = isTRUE(item$includeDescendants %||% item$include_descendants),
      include_mapped = isTRUE(item$includeMapped %||% item$include_mapped)
    )
  })
  normalized <- normalized[order(vapply(normalized, `[[`, integer(1), "concept_id"))]
  list(t = "cs", items = normalized)
}

#' Normalize a single primary criterion for hashing.
#' Extracts criteria_type + all domain-specific fields that affect SQL.
#' @param criterion A single criteria item from CriteriaList
#' @param cs_map Concept set map for resolving codeset references
#' @param cohort_idx Cohort index in the batch
#' @noRd
normalize_criterion_for_hash <- function(criterion, cs_map, cohort_idx) {
  ext <- extract_criteria(criterion)
  if (is.null(ext)) return(list(t = "null_criterion"))

  data <- ext$data
  codeset_id <- data$CodesetId %||% data$codesetId

  # Resolve codeset_id to concept set expression hash
  cs_hash <- ""
  if (!is.null(codeset_id)) {
    key <- paste0(cohort_idx, ":", codeset_id)
    cs_hash <- cs_map$lookup[[key]] %||% ""
  }

  # Collect all fields that affect generated SQL for this criteria type
  # These mirror what the build_*_sql functions use
  fields <- list(
    criteria_type = ext$type,
    cs_hash = cs_hash,
    first = isTRUE(data$First %||% data$first)
  )

  # DateAdjustment
  da <- data$DateAdjustment %||% data$dateAdjustment
  if (!is.null(da)) {
    fields$date_adj <- list(
      start_with = da$StartWith %||% da$startWith %||% "start_date",
      end_with = da$EndWith %||% da$endWith %||% "end_date",
      start_offset = as.integer(da$StartOffset %||% da$startOffset %||% 0L),
      end_offset = as.integer(da$EndOffset %||% da$endOffset %||% 0L)
    )
  }

  # Source concept codesets (resolve each to expression hash)
  for (field_name in c("ConditionSourceConcept", "DrugSourceConcept",
                        "ProcedureSourceConcept", "VisitSourceConcept",
                        "MeasurementSourceConcept", "ObservationSourceConcept",
                        "DeviceSourceConcept", "SpecimenSourceConcept",
                        "DeathSourceConcept", "CauseSourceConcept")) {
    src_csid <- data[[field_name]] %||% data[[tolower(substr(field_name, 1, 1))]]
    if (!is.null(src_csid)) {
      src_key <- paste0(cohort_idx, ":", src_csid)
      fields[[paste0("src_", field_name)]] <- cs_map$lookup[[src_key]] %||% ""
    }
  }

  # Filter fields: Age, Gender, OccurrenceStartDate, etc.
  for (fname in c("Age", "Gender", "OccurrenceStartDate", "OccurrenceEndDate",
                   "ConditionType", "DrugType", "ProcedureType", "VisitType",
                   "StopReason", "Quantity", "ValueAsNumber", "ValueAsConcept",
                   "ValueAsString", "Unit", "RangeLow", "RangeHigh", "Refills", "DaysSupply")) {
    val <- data[[fname]] %||% data[[paste0(tolower(substr(fname, 1, 1)),
                                            substring(fname, 2))]]
    if (!is.null(val)) {
      fields[[tolower(fname)]] <- val
    }
  }

  # CorrelatedCriteria for ObservationPeriod
  corr <- data$CorrelatedCriteria %||% data$correlatedCriteria
  if (!is.null(corr) && !is_group_empty(corr)) {
    fields$correlated_hash <- normalize_criteria_group_for_hash(corr, cs_map, cohort_idx)
  }

  fields
}

#' Normalize primary events for hashing.
#' Key: (sorted criterion hashes, obs_window, primary_limit)
#' @noRd
normalize_primary_events_for_hash <- function(def) {
  list(
    t = "pe",
    criteria_hashes = sort(as.character(def$criteria_hashes)),
    prior_days = as.integer(def$prior_days %||% 0L),
    post_days = as.integer(def$post_days %||% 0L),
    event_sort = toupper(def$event_sort %||% "ASC"),
    primary_limit_type = toupper(def$primary_limit_type %||% "ALL")
  )
}

#' Normalize qualified events for hashing.
#' Key: (primary_events_hash, additional_criteria_hash, qualified_sort, qualified_limit)
#' @noRd
normalize_qualified_events_for_hash <- function(def) {
  list(
    t = "qe",
    pe_hash = as.character(def$pe_hash),
    ac_hash = as.character(def$ac_hash %||% ""),
    q_sort = toupper(def$q_sort %||% "ASC"),
    q_limit = toupper(def$q_limit %||% "ALL")
  )
}

#' Normalize a criteria group recursively for hashing.
#' Used for AdditionalCriteria and InclusionRule expressions.
#' @noRd
normalize_criteria_group_for_hash <- function(group, cs_map = NULL, cohort_idx = NULL) {
  if (!is.list(group)) return(list(t = "empty_group"))
  criteria_list <- group$CriteriaList %||% group$criteriaList %||% list()
  groups <- group$Groups %||% group$groups %||% list()
  demo_list <- group$DemographicCriteriaList %||% group$demographicCriteriaList %||% list()
  grp_type <- toupper(group$Type %||% group$type %||% "ALL")
  grp_count <- as.integer(group$Count %||% group$count %||% 0L)

  # Normalize each correlated criteria item
  cr_norms <- lapply(criteria_list, function(cc) {
    inner <- cc$Criteria %||% cc$criteria
    occ <- cc$Occurrence %||% cc$occurrence
    sw <- cc$StartWindow %||% cc$startWindow
    ew <- cc$EndWindow %||% cc$endWindow
    rv <- cc$RestrictVisit %||% cc$restrictVisit
    ignore_op <- cc$IgnoreObservationPeriod %||% cc$ignoreObservationPeriod
    list(
      criteria = if (!is.null(inner)) normalize_inner_criteria(inner, cs_map, cohort_idx) else NULL,
      occ_type = as.integer((occ$Type %||% occ$type) %||% 2L),
      occ_count = as.integer((occ$Count %||% occ$count) %||% 1L),
      occ_distinct = isTRUE(occ$IsDistinct %||% occ$isDistinct),
      start_window = normalize_window(sw),
      end_window = normalize_window(ew),
      restrict_visit = isTRUE(rv),
      ignore_op = isTRUE(ignore_op)
    )
  })

  demo_norms <- lapply(demo_list, function(dc) {
    list(
      age = dc$Age %||% dc$age,
      gender = normalize_concept_list(dc$Gender %||% dc$gender),
      race = normalize_concept_list(dc$Race %||% dc$race),
      ethnicity = normalize_concept_list(dc$Ethnicity %||% dc$ethnicity),
      occ_start = dc$OccurrenceStartDate %||% dc$occurrenceStartDate,
      occ_end = dc$OccurrenceEndDate %||% dc$occurrenceEndDate
    )
  })

  sub_groups <- lapply(groups, normalize_criteria_group_for_hash, cs_map = cs_map, cohort_idx = cohort_idx)

  list(
    t = "cg",
    type = grp_type,
    count = grp_count,
    criteria = cr_norms,
    demographics = demo_norms,
    groups = sub_groups
  )
}

#' Normalize inner criteria (the domain-specific part) for hashing within a group.
#' @param cs_map Concept set map for resolving codeset references (optional)
#' @param cohort_idx Cohort index for resolving codeset references (optional)
#' @noRd
normalize_inner_criteria <- function(inner, cs_map = NULL, cohort_idx = NULL) {
  ext <- extract_criteria(list(Criteria = inner))
  if (is.null(ext)) ext <- extract_criteria(inner)
  if (is.null(ext)) return(list(t = "unknown"))
  data <- ext$data
  codeset_id <- data$CodesetId %||% data$codesetId

  # Resolve codeset_id to concept set content hash when cs_map available
  cs_hash <- ""
  if (!is.null(codeset_id) && !is.null(cs_map) && !is.null(cohort_idx)) {
    key <- paste0(cohort_idx, ":", codeset_id)
    cs_hash <- cs_map$lookup[[key]] %||% ""
  }

  fields <- list(
    type = ext$type,
    cs_hash = if (nzchar(cs_hash)) cs_hash else codeset_id,
    first = isTRUE(data$First %||% data$first)
  )

  # CorrelatedCriteria (nested criteria within this criterion) - critical for
  # distinguishing inclusion rules that differ only by nested criteria
  corr <- data$CorrelatedCriteria %||% data$correlatedCriteria
  if (!is.null(corr) && !is_group_empty(corr)) {
    fields$correlated_hash <- normalize_criteria_group_for_hash(corr, cs_map, cohort_idx)
  }

  # Compact hash of all remaining filter fields that affect SQL generation.
  # Instead of including each field individually (expensive to serialize),
  # compute a single hash of the data minus already-captured fields.
  filter_data <- data
  filter_data$CodesetId <- NULL; filter_data$codesetId <- NULL
  filter_data$First <- NULL; filter_data$first <- NULL
  filter_data$CorrelatedCriteria <- NULL; filter_data$correlatedCriteria <- NULL
  if (length(filter_data) > 0) {
    # Resolve any CodesetId references within filter fields to content hashes
    for (fname in names(filter_data)) {
      val <- filter_data[[fname]]
      if (is.list(val) && !is.null(val$CodesetId %||% val$codesetId) && !is.null(cs_map) && !is.null(cohort_idx)) {
        fkey <- paste0(cohort_idx, ":", val$CodesetId %||% val$codesetId)
        filter_data[[fname]]$CodesetId <- cs_map$lookup[[fkey]] %||% ""
        filter_data[[fname]]$codesetId <- NULL
      }
    }
    fj <- jsonlite::toJSON(filter_data, auto_unbox = TRUE, null = "null", force = TRUE)
    fields$filter_hash <- string_hash(as.character(fj))
  }

  fields
}

#' Normalize a window definition for hashing.
#' @noRd
normalize_window <- function(w) {
  if (is.null(w)) return(NULL)
  s <- w$Start %||% w$start
  e <- w$End %||% w$end
  list(
    use_index_end = isTRUE(scalar(w$UseIndexEnd %||% w$useIndexEnd)),
    use_event_end = isTRUE(scalar(w$UseEventEnd %||% w$useEventEnd)),
    start_days = as.integer(scalar(s$Days %||% s$days) %||% NA_integer_),
    start_coeff = as.integer(scalar(s$Coeff %||% s$coeff) %||% 1L),
    end_days = as.integer(scalar(e$Days %||% e$days) %||% NA_integer_),
    end_coeff = as.integer(scalar(e$Coeff %||% e$coeff) %||% 1L)
  )
}

#' Normalize a concept list (for gender, race, etc.) for hashing.
#' @noRd
normalize_concept_list <- function(concepts) {
  if (is.null(concepts) || length(concepts) == 0) return(NULL)
  ids <- sort(get_concept_ids(concepts))
  if (length(ids) == 0) return(NULL)
  ids
}

#' Normalize inclusion rule for hashing.
#' Key: (qualified_events_hash, expression_hash)
#' @noRd
normalize_inclusion_rule_for_hash <- function(def) {
  list(
    t = "ir",
    qe_hash = as.character(def$qe_hash),
    expr_hash = as.character(def$expr_hash)
  )
}

#' Normalize included events for hashing.
#' Key: (qualified_events_hash, sorted inclusion_rule_hashes, expression_limit)
#' @noRd
normalize_included_events_for_hash <- function(def) {
  list(
    t = "ie",
    qe_hash = as.character(def$qe_hash),
    ir_hashes = sort(as.character(def$ir_hashes)),
    el_sort = toupper(def$el_sort %||% "ASC"),
    el_type = toupper(def$el_type %||% "ALL")
  )
}

#' Normalize cohort exit for hashing.
#' Key: (included_events_hash, end_strategy, censoring_criteria)
#' @noRd
normalize_cohort_exit_for_hash <- function(def) {
  es <- def$end_strategy
  strategy <- list(type = "default")
  if (!is.null(es)) {
    doff <- es$DateOffset %||% es$dateOffset
    cera <- es$CustomEra %||% es$customEra
    if (!is.null(doff)) {
      strategy <- list(
        type = "date_offset",
        offset = as.integer(doff$Offset %||% doff$offset %||% 0L),
        date_field = doff$DateField %||% doff$dateField %||% "StartDate"
      )
    } else if (!is.null(cera)) {
      drug_cs <- cera$DrugCodesetId %||% cera$drugCodesetId
      cond_cs <- cera$ConditionCodesetId %||% cera$conditionCodesetId
      strategy <- list(
        type = "custom_era",
        drug_codeset_id = drug_cs,
        condition_codeset_id = cond_cs,
        gap_days = as.integer(cera$GapDays %||% cera$gapDays %||% 0L)
      )
    }
  }
  # Normalize censoring criteria
  censoring <- def$censoring %||% list()
  censor_norms <- lapply(censoring, function(cc) {
    ext <- extract_criteria(cc)
    if (is.null(ext)) return(NULL)
    list(type = ext$type, codeset_id = ext$data$CodesetId %||% ext$data$codesetId)
  })
  censor_norms <- Filter(Negate(is.null), censor_norms)

  list(
    t = "ce",
    ie_hash = as.character(def$ie_hash),
    strategy = strategy,
    censoring = censor_norms
  )
}

#' Normalize final cohort for hashing.
#' Key: (cohort_exit_hash, era_pad, cohort_id)
#' @noRd
normalize_final_cohort_for_hash <- function(def) {
  list(
    t = "fc",
    ce_hash = as.character(def$ce_hash),
    era_pad = as.integer(def$era_pad %||% 0L),
    cohort_id = as.integer(def$cohort_id)
  )
}

# ---- Node creation ----

#' Create an execution graph node.
#' @param type Character node type
#' @param hash Character content hash (the node ID)
#' @param definition List — the definition used to compute the hash
#' @param depends_on Character vector of parent node hashes
#' @param cohort_ids Integer vector of cohort IDs using this node
#' @return Node list
#' @noRd
create_node <- function(type, hash, definition, depends_on = character(0), cohort_ids = integer(0)) {
  prefix <- switch(type,
    concept_set = "cs",
    primary_events = "pe",
    qualified_events = "qe",
    inclusion_rule = "ir",
    included_events = "ie",
    cohort_exit = "ce",
    final_cohort = "fc",
    "nd"
  )
  list(
    id = hash,
    type = type,
    definition = definition,
    depends_on = depends_on,
    cohort_ids = cohort_ids,
    temp_table = paste0(prefix, "_", substr(hash, 1, 8))
  )
}

#' Add or update a node in the nodes list.
#' If node with same hash exists, add cohort_id. Otherwise create new.
#' @noRd
upsert_node <- function(nodes, type, hash, definition, depends_on, cohort_id) {
  if (!is.null(nodes[[hash]])) {
    nodes[[hash]]$cohort_ids <- unique(c(nodes[[hash]]$cohort_ids, as.integer(cohort_id)))
  } else {
    nodes[[hash]] <- create_node(type, hash, definition, depends_on, as.integer(cohort_id))
  }
  nodes
}

# ---- Concept set map ----

#' Build a global concept set map across all cohorts.
#' Resolves (cohort_index, local_codeset_id) -> expression_hash.
#' Assigns a globally unique codeset_id to each unique expression.
#' @param cohort_list List of cohort expression lists
#' @return List with:
#'   nodes: named list (hash -> node) of concept set nodes
#'   lookup: named list ("idx:csid" -> hash)
#'   global_id: named list (hash -> integer) global codeset_id assignment
#'   local_to_global: named list ("idx:csid" -> integer) for codeset_id remapping
#' @noRd
build_concept_set_map <- function(cohort_list) {
  nodes <- list()
  lookup <- list()       # "idx:local_csid" -> hash
  expr_hashes <- character(0)  # ordered unique hashes

  for (i in seq_along(cohort_list)) {
    cohort <- cohort_list[[i]]
    concept_sets <- get_key(cohort, c("ConceptSets", "conceptSets")) %||% list()
    for (cs in concept_sets) {
      csid <- get_key(cs, c("id", "ID", "Id"))
      if (is.null(csid)) next
      expr <- get_key(cs, c("expression", "Expression"))
      if (is.null(expr)) next

      h <- canonical_hash("concept_set", expr)
      lookup_key <- paste0(i, ":", csid)
      lookup[[lookup_key]] <- h

      if (is.null(nodes[[h]])) {
        nodes[[h]] <- create_node("concept_set", h, expr, character(0), integer(0))
        expr_hashes <- c(expr_hashes, h)
      }
      nodes[[h]]$cohort_ids <- unique(c(nodes[[h]]$cohort_ids, as.integer(i)))
    }
  }

  # Assign globally unique codeset_ids (0-based)
  global_id <- setNames(seq_along(expr_hashes) - 1L, expr_hashes)
  # Build local-to-global map
  local_to_global <- list()
  for (key in names(lookup)) {
    local_to_global[[key]] <- global_id[[lookup[[key]]]]
  }

  list(
    nodes = nodes,
    lookup = lookup,
    global_id = global_id,
    local_to_global = local_to_global
  )
}

# ---- DAG construction ----

#' Build execution DAG from multiple cohort definitions.
#' @param cohort_list List of cohort expression lists (from cohortExpressionFromJson)
#' @param cohort_ids Integer vector
#' @param options List with cdm_schema, results_schema, vocabulary_schema
#' @return DAG list with nodes, cohort_finals, cs_map, used_tables
#' @noRd
build_execution_dag <- function(cohort_list, cohort_ids, options = list()) {
  cs_map <- build_concept_set_map(cohort_list)
  nodes <- cs_map$nodes

  cohort_finals <- list()
  used_tables <- character(0)

  for (i in seq_along(cohort_list)) {
    result <- decompose_cohort(cohort_list[[i]], cohort_ids[[i]], i, cs_map, nodes, options)
    nodes <- result$nodes
    cohort_finals[[as.character(cohort_ids[[i]])]] <- result$final_hash
    used_tables <- unique(c(used_tables, result$used_tables))
  }

  list(
    nodes = nodes,
    cohort_finals = cohort_finals,
    cs_map = cs_map,
    used_tables = used_tables,
    cohort_ids = cohort_ids
  )
}

#' Decompose a single cohort into DAG nodes.
#' @param cohort Cohort expression list
#' @param cohort_id Integer cohort ID
#' @param cohort_idx Integer index in batch (for concept set map lookup)
#' @param cs_map Concept set map
#' @param nodes Current nodes list (modified in place semantically)
#' @param options Build options
#' @return List with nodes, final_hash, used_tables
#' @noRd
decompose_cohort <- function(cohort, cohort_id, cohort_idx, cs_map, nodes, options) {
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  used_tables <- "OBSERVATION_PERIOD"  # always needed

  # ---- Primary Events ----
  pc <- get_key(cohort, c("PrimaryCriteria", "primaryCriteria"))
  if (is.null(pc)) stop("Cohort must have PrimaryCriteria")

  criteria_list <- get_key(pc, c("CriteriaList", "criteriaList")) %||% list()
  criteria_hashes <- character(0)
  for (cr in criteria_list) {
    ext <- extract_criteria(cr)
    if (!is.null(ext)) {
      tbl <- CRITERIA_TYPE_TO_CDM_TABLE[ext$type]
      if (!is.na(tbl)) used_tables <- c(used_tables, tbl)
      # VisitType filter generates JOIN to VISIT_OCCURRENCE
      vt <- ext$data$VisitType %||% ext$data$visitType
      vtc <- ext$data$VisitTypeCS %||% ext$data$visitTypeCS
      if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId %||% vtc$codesetId)))
        used_tables <- c(used_tables, "VISIT_OCCURRENCE")
      # CorrelatedCriteria within this criterion may reference additional domains
      corr <- ext$data$CorrelatedCriteria %||% ext$data$correlatedCriteria
      if (!is.null(corr)) {
        corr_types <- collect_criteria_types_from_group(corr)
        for (tp in corr_types) {
          tbl2 <- CRITERIA_TYPE_TO_CDM_TABLE[tp]
          if (!is.na(tbl2)) used_tables <- c(used_tables, tbl2)
        }
      }
    }
    cr_norm <- normalize_criterion_for_hash(cr, cs_map, cohort_idx)
    cr_hash <- string_hash(jsonlite::toJSON(cr_norm, auto_unbox = TRUE, null = "null", force = TRUE))
    criteria_hashes <- c(criteria_hashes, cr_hash)
  }

  # Observation window + PrimaryLimit
  ow <- get_key(pc, c("ObservationWindow", "observationWindow"))
  pl <- get_key(pc, c("PrimaryCriteriaLimit", "PrimaryLimit", "primaryLimit"))
  prior_days <- (ow$PriorDays %||% ow$priorDays) %||% 0L
  post_days <- (ow$PostDays %||% ow$postDays) %||% 0L
  pl_type <- (pl$Type %||% pl$type) %||% "All"
  event_sort <- if (identical(pl_type, "Last")) "DESC" else "ASC"

  pe_def <- list(
    criteria_hashes = criteria_hashes,
    prior_days = prior_days,
    post_days = post_days,
    event_sort = event_sort,
    primary_limit_type = pl_type,
    # Store original structures for SQL generation
    criteria_list = criteria_list,
    cohort_idx = cohort_idx
  )
  pe_hash <- canonical_hash("primary_events", pe_def)
  nodes <- upsert_node(nodes, "primary_events", pe_hash, pe_def, character(0), cohort_id)

  # ---- Qualified Events ----
  add_criteria <- get_key(cohort, c("AdditionalCriteria", "additionalCriteria"))
  ac_hash <- ""
  if (!is.null(add_criteria) && !is_group_empty(add_criteria)) {
    ac_norm <- normalize_criteria_group_for_hash(add_criteria, cs_map, cohort_idx)
    ac_hash <- string_hash(jsonlite::toJSON(ac_norm, auto_unbox = TRUE, null = "null", force = TRUE))
    # Collect used tables from additional criteria
    ac_types <- collect_criteria_types_from_group(add_criteria)
    for (tp in ac_types) {
      tbl <- CRITERIA_TYPE_TO_CDM_TABLE[tp]
      if (!is.na(tbl)) used_tables <- c(used_tables, tbl)
    }
  }

  ql <- get_key(cohort, c("QualifiedLimit", "qualifiedLimit"))
  ql_type <- (ql$Type %||% ql$type) %||% "All"
  q_sort <- if (identical(ql_type, "Last")) "DESC" else "ASC"

  qe_def <- list(
    pe_hash = pe_hash,
    ac_hash = ac_hash,
    q_sort = q_sort,
    q_limit = ql_type,
    # Original structures for SQL generation
    additional_criteria = add_criteria,
    cohort_idx = cohort_idx  # for codeset ID remapping
  )
  qe_hash <- canonical_hash("qualified_events", qe_def)
  nodes <- upsert_node(nodes, "qualified_events", qe_hash, qe_def, pe_hash, cohort_id)

  # ---- Inclusion Rules ----
  inclusion_rules <- get_key(cohort, c("InclusionRules", "inclusionRules")) %||% list()
  ir_hashes <- character(0)
  for (ir in inclusion_rules) {
    ir_expr <- get_key(ir, c("expression", "Expression"))
    if (is.null(ir_expr)) next

    # Collect used tables from inclusion rules
    ir_types <- collect_criteria_types_from_group(ir_expr)
    for (tp in ir_types) {
      tbl <- CRITERIA_TYPE_TO_CDM_TABLE[tp]
      if (!is.na(tbl)) used_tables <- c(used_tables, tbl)
    }

    ir_expr_norm <- normalize_criteria_group_for_hash(ir_expr, cs_map, cohort_idx)
    ir_expr_hash <- string_hash(jsonlite::toJSON(ir_expr_norm, auto_unbox = TRUE,
                                                  null = "null", force = TRUE))
    ir_def <- list(
      qe_hash = qe_hash,
      expr_hash = ir_expr_hash,
      # Original for SQL generation
      expression = ir_expr,
      cohort_idx = cohort_idx  # for codeset ID remapping
    )
    ir_hash <- canonical_hash("inclusion_rule", ir_def)
    nodes <- upsert_node(nodes, "inclusion_rule", ir_hash, ir_def, qe_hash, cohort_id)
    ir_hashes <- c(ir_hashes, ir_hash)
  }

  # ---- Included Events ----
  el <- get_key(cohort, c("ExpressionLimit", "expressionLimit"))
  el_type <- (el$Type %||% el$type) %||% "All"
  el_sort <- if (identical(el_type, "Last")) "DESC" else "ASC"

  ie_def <- list(
    qe_hash = qe_hash,
    ir_hashes = ir_hashes,
    el_sort = el_sort,
    el_type = el_type
  )
  ie_hash <- canonical_hash("included_events", ie_def)
  nodes <- upsert_node(nodes, "included_events", ie_hash, ie_def,
                        c(qe_hash, ir_hashes), cohort_id)

  # ---- Cohort Exit ----
  end_strategy <- get_key(cohort, c("EndStrategy", "endStrategy"))
  censoring <- get_key(cohort, c("CensoringCriteria", "censoringCriteria")) %||% list()

  # Collect used tables from censoring
  for (cc in censoring) {
    ext <- extract_criteria(cc)
    if (!is.null(ext)) {
      tbl <- CRITERIA_TYPE_TO_CDM_TABLE[ext$type]
      if (!is.na(tbl)) used_tables <- c(used_tables, tbl)
      # VisitType filter generates JOIN to VISIT_OCCURRENCE
      vt <- ext$data$VisitType %||% ext$data$visitType
      vtc <- ext$data$VisitTypeCS %||% ext$data$visitTypeCS
      if (length(vt) > 0 || (!is.null(vtc) && !is.null(vtc$CodesetId %||% vtc$codesetId)))
        used_tables <- c(used_tables, "VISIT_OCCURRENCE")
      # CorrelatedCriteria within censoring criterion
      corr <- ext$data$CorrelatedCriteria %||% ext$data$correlatedCriteria
      if (!is.null(corr)) {
        corr_types <- collect_criteria_types_from_group(corr)
        for (tp in corr_types) {
          tbl2 <- CRITERIA_TYPE_TO_CDM_TABLE[tp]
          if (!is.na(tbl2)) used_tables <- c(used_tables, tbl2)
        }
      }
    }
  }

  # CustomEra builds eras from raw exposure data (not pre-built era tables)
  if (!is.null(end_strategy)) {
    cera <- end_strategy$CustomEra %||% end_strategy$customEra
    if (!is.null(cera)) {
      if (!is.null(cera$DrugCodesetId %||% cera$drugCodesetId))
        used_tables <- c(used_tables, "DRUG_EXPOSURE")
      if (!is.null(cera$ConditionCodesetId %||% cera$conditionCodesetId))
        used_tables <- c(used_tables, "CONDITION_OCCURRENCE")
    }
  }

  ce_def <- list(
    ie_hash = ie_hash,
    end_strategy = end_strategy,
    censoring = censoring,
    cohort_idx = cohort_idx  # for codeset ID remapping in CustomEra
  )
  ce_hash <- canonical_hash("cohort_exit", ce_def)
  nodes <- upsert_node(nodes, "cohort_exit", ce_hash, ce_def, ie_hash, cohort_id)

  # ---- Final Cohort ----
  collapse <- get_key(cohort, c("CollapseSettings", "collapseSettings"))
  era_pad <- (collapse$EraPad %||% collapse$eraPad) %||% 0L
  censor_window <- get_key(cohort, c("CensorWindow", "censorWindow"))

  fc_def <- list(
    ce_hash = ce_hash,
    era_pad = era_pad,
    cohort_id = cohort_id,
    censor_window = censor_window
  )
  fc_hash <- canonical_hash("final_cohort", fc_def)
  nodes <- upsert_node(nodes, "final_cohort", fc_hash, fc_def, ce_hash, cohort_id)

  list(
    nodes = nodes,
    final_hash = fc_hash,
    used_tables = unique(used_tables)
  )
}

# ---- Topological sort ----

#' Topological sort of DAG nodes using Kahn's algorithm.
#' Uses index-based queue to avoid O(n^2) from repeated c(queue, ...) / queue[-1].
#' @param nodes Named list of nodes
#' @return Character vector of node IDs in execution order
#' @noRd
topological_sort <- function(nodes) {
  ids <- names(nodes)
  n <- length(ids)
  if (n == 0L) return(character(0))

  id_set <- new.env(hash = TRUE, parent = emptyenv(), size = n)
  for (id in ids) id_set[[id]] <- TRUE

  # Build reverse adjacency list: dep -> list of dependents
  dependents <- new.env(hash = TRUE, parent = emptyenv(), size = n)
  in_deg <- setNames(integer(n), ids)
  for (id in ids) {
    for (dep in nodes[[id]]$depends_on) {
      if (!is.null(id_set[[dep]])) {
        in_deg[[id]] <- in_deg[[id]] + 1L
        dependents[[dep]] <- c(dependents[[dep]], id)
      }
    }
  }

  # Pre-allocate queue (max size = n) with head/tail pointers (avoids O(n^2) vector ops)
  queue <- character(n)
  initial_ids <- ids[in_deg == 0L]
  n_initial <- length(initial_ids)
  queue[seq_len(n_initial)] <- initial_ids
  q_head <- 1L
  q_tail <- n_initial

  result <- character(n)
  ri <- 0L

  while (q_head <= q_tail) {
    current <- queue[q_head]
    q_head <- q_head + 1L
    ri <- ri + 1L
    result[ri] <- current

    for (dep_id in dependents[[current]]) {
      in_deg[[dep_id]] <- in_deg[[dep_id]] - 1L
      if (in_deg[[dep_id]] == 0L) {
        q_tail <- q_tail + 1L
        queue[q_tail] <- dep_id
      }
    }
  }

  if (ri != n) {
    stop("Cycle detected in execution graph")
  }
  result
}

# ---- SQL emission ----

#' Emit the complete batch SQL from an execution DAG.
#' Uses list accumulator to avoid O(n^2) vector growth.
#' @param dag DAG from build_execution_dag
#' @param options List with cdm_schema, results_schema, vocabulary_schema
#' @return Single SQL string
#' @noRd
emit_dag_sql <- function(dag, options) {
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  results_schema <- options$results_schema %||% "@results_schema"
  vocab_schema <- options$vocabulary_schema %||% "@vocabulary_database_schema"

  sorted <- topological_sort(dag$nodes)

  # -- Mark single-consumer nodes as VIEW candidates --
  # VIEWs avoid materializing intermediate tables; the DB optimizer can merge
  # the view logic into the consumer's query plan for better performance.
  ref_count <- new.env(hash = TRUE, parent = emptyenv())
  for (nid in names(dag$nodes)) {
    d <- dag$nodes[[nid]]$definition
    refs <- c(d$pe_hash, d$qe_hash, d$ie_hash, d$ce_hash, unlist(d$ir_hashes))
    for (r in unique(refs[!is.na(refs) & nzchar(refs)])) {
      cur <- ref_count[[r]]
      ref_count[[r]] <- if (is.null(cur)) 1L else cur + 1L
    }
  }
  view_types <- c("primary_events", "qualified_events", "inclusion_rule",
                   "included_events", "cohort_exit")
  for (nid in names(dag$nodes)) {
    node <- dag$nodes[[nid]]
    consumers <- ref_count[[nid]] %||% 0L
    can_view <- (consumers <= 1L) && (node$type %in% view_types)
    # cohort_exit with CustomEra needs intermediate dt_tbl — can't be a VIEW
    if (can_view && node$type == "cohort_exit") {
      es <- node$definition$end_strategy
      if (!is.null(es) && (!is.null(es$CustomEra) || !is.null(es$customEra)))
        can_view <- FALSE
    }
    # VIEWs avoid materialization but cause issues in embedded DBs (DuckDB):
    # nested view expansion creates huge query trees and can cause correctness
    # issues with non-deterministic row_number(). Default to tables for now.
    # TODO: enable for server DBs (PostgreSQL, SQL Server) where materialization
    # round-trip is expensive and the optimizer handles view merging better.
    dag$nodes[[nid]]$is_view <- FALSE
  }

  # Pre-allocate list accumulator: each slot holds a character vector chunk.
  # Avoids O(n^2) from repeated c(parts, ...) in a loop.
  n_sorted <- length(sorted)
  chunks <- vector("list", n_sorted + 8L)
  ci <- 0L

  # 1. Preamble
  ci <- ci + 1L; chunks[[ci]] <- emit_preamble(options)

  # 2. Global #Codesets from concept set nodes
  ci <- ci + 1L; chunks[[ci]] <- emit_codesets(dag, options)

  # 3. Domain filtered tables
  ci <- ci + 1L; chunks[[ci]] <- emit_domain_filtered(dag$used_tables, cdm_schema, options)

  # 3b. ANALYZE hints for server DBs — helps query planner build statistics
  # on the domain-filtered tables and codesets before they're heavily scanned.
  # DuckDB auto-analyzes so the translator strips these; PostgreSQL uses them
  # natively; SQL Server translates to UPDATE STATISTICS.
  ci <- ci + 1L; chunks[[ci]] <- emit_analyze_hints(dag$used_tables, options)

  # 4. Emit each non-concept-set node in topological order
  # Set .sql_context so circe builders use qualified codesets table
  old_cs <- .sql_context$codesets_table
  .sql_context$codesets_table <- qualify_table("codesets", options)
  on.exit(.sql_context$codesets_table <- old_cs, add = TRUE)

  for (node_id in sorted) {
    node <- dag$nodes[[node_id]]
    if (node$type == "concept_set") next  # already handled in codesets

    node_sql <- emit_node_sql(node, dag, options)
    if (nzchar(node_sql)) {
      ci <- ci + 1L
      chunks[[ci]] <- c(
        sprintf("-- [%s] %s (cohorts: %s)", node$type, substr(node$id, 1, 8),
                paste(node$cohort_ids, collapse = ",")),
        node_sql, "")
    }
  }

  # 5. Finalize: staging -> output tables
  ci <- ci + 1L; chunks[[ci]] <- emit_finalize(dag, options)

  # 6. Cleanup: drop all temp tables
  ci <- ci + 1L; chunks[[ci]] <- emit_cleanup(dag, options)

  stringi::stri_join(unlist(chunks[seq_len(ci)]), collapse = "\n")
}

#' Emit preamble: staging tables.
#' @noRd
emit_preamble <- function(options) {
  cs <- qualify_table("cohort_stage", options)
  ies <- qualify_table("inclusion_events_stage", options)
  iss <- qualify_table("inclusion_stats_stage", options)
  c(
    "/*",
    "  Execution Graph Batch Script",
    "  Generated by atlasCohortGenerator",
    "*/",
    "",
    "-- Staging tables",
    paste0("DROP TABLE IF EXISTS ", cs, ";"),
    paste0("CREATE TABLE ", cs, " (cohort_definition_id INT NOT NULL, subject_id BIGINT NOT NULL, cohort_start_date DATE NOT NULL, cohort_end_date DATE NOT NULL);"),
    "",
    paste0("DROP TABLE IF EXISTS ", ies, ";"),
    paste0("CREATE TABLE ", ies, " (cohort_definition_id INT NOT NULL, inclusion_rule_id INT NOT NULL, person_id BIGINT NOT NULL, event_id BIGINT NOT NULL);"),
    "",
    paste0("DROP TABLE IF EXISTS ", iss, ";"),
    paste0("CREATE TABLE ", iss, " (cohort_definition_id INT NOT NULL, inclusion_rule_id INT NOT NULL, person_count BIGINT NULL, gain_count BIGINT NULL, person_total BIGINT NULL);"),
    ""
  )
}

#' Emit global #Codesets table from concept set nodes.
#' Batches concept set INSERTs into groups using UNION ALL to reduce
#' round-trip overhead (fewer INSERT statements sent to the database).
#' @noRd
emit_codesets <- function(dag, options) {
  vocab_schema <- options$vocabulary_schema %||% "@vocabulary_database_schema"
  cs_map <- dag$cs_map
  cs_tbl <- qualify_table("codesets", options)
  ac_tbl <- qualify_table("all_concepts", options)

  cs_names <- names(cs_map$global_id)
  n_cs <- length(cs_names)

  # Batch size for UNION ALL grouping: balance between fewer round trips

  # and SQL statement size. 50 is conservative for cross-platform safety.
  batch_size <- 50L

  n_batches <- ceiling(n_cs / batch_size)
  # Pre-allocate: header(1) + batches(n_batches) + footer(1)
  chunks <- vector("list", n_batches + 2L)
  ci <- 0L

  ci <- ci + 1L
  chunks[[ci]] <- c(
    "-- Global codesets (one row per globally-assigned codeset_id + concept_id)",
    paste0("DROP TABLE IF EXISTS ", cs_tbl, ";"),
    paste0("CREATE TABLE ", cs_tbl, " (codeset_id INT NOT NULL, concept_id BIGINT NOT NULL);"),
    ""
  )

  # Build SELECT fragments for each concept set, then batch into UNION ALL groups
  if (n_cs > 0L) {
    select_parts <- vector("list", n_cs)
    for (i in seq_len(n_cs)) {
      h <- cs_names[i]
      global_csid <- cs_map$global_id[[h]]
      node <- cs_map$nodes[[h]]
      expr_sql <- build_concept_set_expression_query(node$definition, vocab_schema)
      select_parts[[i]] <- paste0(
        "SELECT ", global_csid, " as codeset_id, c.concept_id FROM (\n",
        expr_sql, "\n) C"
      )
    }

    # Emit in batches of batch_size
    for (b in seq_len(n_batches)) {
      start_idx <- (b - 1L) * batch_size + 1L
      end_idx <- min(b * batch_size, n_cs)
      batch_parts <- select_parts[start_idx:end_idx]
      batch_sql <- paste0(
        "INSERT INTO ", cs_tbl, " (codeset_id, concept_id)\n",
        paste(batch_parts, collapse = "\nUNION ALL\n"),
        ";"
      )
      ci <- ci + 1L
      chunks[[ci]] <- c(
        sprintf("-- Concept sets batch %d/%d (ids %d-%d)",
                b, n_batches, start_idx - 1L, end_idx - 1L),
        batch_sql, "")
    }
  }

  ci <- ci + 1L
  chunks[[ci]] <- c(
    "",
    paste0("DROP TABLE IF EXISTS ", ac_tbl, ";"),
    paste0("SELECT DISTINCT concept_id INTO ", ac_tbl, " FROM ", cs_tbl, ";"),
    ""
  )
  unlist(chunks[seq_len(ci)])
}

#' Emit domain filtered tables.
#' Uses list accumulator to avoid O(n^2) vector growth.
#' @noRd
emit_domain_filtered <- function(used_tables, cdm_schema, options) {
  ac_tbl <- qualify_table("all_concepts", options)
  # Max: 1 (obs_period) + 7 (domain tables) = 8 chunks

  chunks <- vector("list", 8L)
  ci <- 0L

  if ("OBSERVATION_PERIOD" %in% used_tables) {
    op_tbl <- qualify_table("atlas_observation_period", options)
    ci <- ci + 1L
    chunks[[ci]] <- c(
      paste0("DROP TABLE IF EXISTS ", op_tbl, ";"),
      sprintf("SELECT * INTO %s FROM %s.OBSERVATION_PERIOD;", op_tbl, cdm_schema), "")
  }

  for (dc in DOMAIN_CONFIG) {
    if (!dc$table %in% used_tables) next
    a <- dc$alias; ft <- qualify_table(dc$filtered, options); std <- dc$std_col; src <- dc$src_col
    ci <- ci + 1L
    chunks[[ci]] <- c(
      sprintf("DROP TABLE IF EXISTS %s;", ft),
      sprintf(paste0("SELECT * INTO %s FROM %s.%s %s WHERE %s.%s IN ",
                     "(SELECT concept_id FROM %s) OR %s.%s IN ",
                     "(SELECT concept_id FROM %s);"),
              ft, cdm_schema, dc$table, a, a, std, ac_tbl, a, src, ac_tbl),
      "")
  }
  if (ci == 0L) return(character(0))
  unlist(chunks[seq_len(ci)])
}

#' Emit ANALYZE hints for domain-filtered tables and codesets.
#'
#' Emits PostgreSQL-syntax `ANALYZE table_name;` statements that help the query
#' planner on server databases.  The DuckDB lightweight translator strips these
#' (DuckDB auto-analyzes), PostgreSQL uses them natively.  For other dialects
#' the SqlRender fallback translator converts or strips them.
#' @noRd
emit_analyze_hints <- function(used_tables, options) {
  tables <- c(qualify_table("codesets", options),
              qualify_table("all_concepts", options))
  if ("OBSERVATION_PERIOD" %in% used_tables) {
    tables <- c(tables, qualify_table("atlas_observation_period", options))
  }
  for (dc in DOMAIN_CONFIG) {
    if (dc$table %in% used_tables) {
      tables <- c(tables, qualify_table(dc$filtered, options))
    }
  }
  c("-- ANALYZE hints for server DB query planners",
    paste0("ANALYZE ", tables, ";"),
    "")
}

#' Emit SQL for a single node.
#' Dispatches by node type and applies domain filtered table substitution.
#' @noRd
emit_node_sql <- function(node, dag, options) {
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  sql <- switch(node$type,
    primary_events = emit_primary_events(node, dag, options),
    qualified_events = emit_qualified_events(node, dag, options),
    inclusion_rule = emit_inclusion_rule(node, dag, options),
    included_events = emit_included_events(node, dag, options),
    cohort_exit = emit_cohort_exit(node, dag, options),
    final_cohort = emit_final_cohort(node, dag, options),
    ""
  )
  # Apply domain filtered table substitution to node SQL
  # (not applied to preamble/domain table creation SQL to avoid circular references)
  if (nzchar(sql)) {
    sql <- rewrite_to_domain_caches(sql, cdm_schema, options = options)
  }
  sql
}

#' Emit primary events SQL.
#' Calls get_criteria_sql for each criterion, applies obs window + PrimaryLimit.
#' @noRd
emit_primary_events <- function(node, dag, options) {
  def <- node$definition
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  cs_map <- dag$cs_map
  cohort_idx <- def$cohort_idx  # index for concept set lookup

  # Build criteria queries using existing builders
  criteria_queries <- character(0)
  for (cr in def$criteria_list) {
    # Remap codeset IDs in the criteria to global IDs
    cr_remapped <- remap_codeset_ids(cr, cohort_idx, cs_map)
    criteria_queries <- c(criteria_queries, get_criteria_sql(cr_remapped, cdm_schema))
  }

  if (length(criteria_queries) == 0) return("")

  # Build the primary events subquery (mirrors PRIMARY_EVENTS_SUBQUERY_TEMPLATE)
  criteria_union <- paste(criteria_queries, collapse = "\nUNION ALL\n")
  prior_days <- as.integer(def$prior_days %||% 0L)
  post_days <- as.integer(def$post_days %||% 0L)
  event_sort <- def$event_sort %||% "ASC"
  pl_type <- toupper(def$primary_limit_type %||% "ALL")
  primary_event_limit <- if (pl_type == "ALL") "" else "WHERE P.ordinal = 1"

  primary_events_filter <- paste0(
    "DATEADD(day,", prior_days, ",OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND ",
    "DATEADD(day,", post_days, ",E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE"
  )

  subquery <- paste0(
    "select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, ",
    "op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id\n",
    "from\n(\n",
    "  select E.person_id, E.start_date, E.end_date,\n",
    "         row_number() over (partition by E.person_id order by E.sort_date ", event_sort, ", E.event_id) ordinal,\n",
    "         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, ",
    "cast(E.visit_occurrence_id as bigint) as visit_occurrence_id\n",
    "  FROM\n  (\n  ", criteria_union, "\n  ) E\n",
    "\tjoin ", cdm_schema, ".observation_period OP on E.person_id = OP.person_id ",
    "and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date\n",
    "  where ", primary_events_filter, "\n",
    ") P\n", primary_event_limit
  )

  tbl <- qualify_table(node$temp_table, options)
  use_view <- isTRUE(node$is_view)
  paste0(
    if (use_view) paste0("DROP VIEW IF EXISTS ", tbl, ";\nCREATE VIEW ", tbl, " AS\n")
    else paste0("DROP TABLE IF EXISTS ", tbl, ";\n"),
    "select event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id\n",
    if (!use_view) paste0("INTO ", tbl, "\n") else "",
    "from\n(\n",
    "  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, ",
    "cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id\n",
    "  from (-- Begin Primary Events\n", subquery, "\n-- End Primary Events\n) pe\n",
    ") QE;"
  )
}

#' Emit qualified events SQL.
#' Applies additional criteria and QualifiedLimit on top of primary events.
#' @noRd
emit_qualified_events <- function(node, dag, options) {
  def <- node$definition
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"

  pe_node <- dag$nodes[[def$pe_hash]]
  pe_table <- qualify_table(pe_node$temp_table, options)

  # Additional criteria
  add_criteria_join <- ""
  add_criteria <- def$additional_criteria
  if (!is.null(add_criteria) && !is_group_empty(add_criteria)) {
    event_table <- paste0("(\n-- Begin Primary Events\n",
      "select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, ",
      "cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id from ", pe_table, " pe\n",
      "-- End Primary Events\n)")
    # Remap codeset IDs in additional criteria to global IDs
    ac_remapped <- remap_codeset_ids(add_criteria, def$cohort_idx, dag$cs_map)
    ac_sql <- get_criteria_group_query(ac_remapped, event_table, cdm_schema)
    ac_sql <- gsub("@indexId", "0", ac_sql, fixed = TRUE)
    add_criteria_join <- paste0(
      "\nJOIN (\n", ac_sql, ") AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id"
    )
  }

  q_sort <- def$q_sort %||% "ASC"

  # NOTE: CirceR computes ordinal in #qualified_events but does NOT filter on it.
  # The QualifiedLimit is NOT applied here — it is only used to compute row order.
  # Filtering happens downstream (ExpressionLimit in included_events, or first_ends
  # in final_cohort). Applying WHERE QE.ordinal = 1 here was incorrect and caused
  # exactly 1 record per person.

  tbl <- qualify_table(node$temp_table, options)
  use_view <- isTRUE(node$is_view)
  paste0(
    if (use_view) paste0("DROP VIEW IF EXISTS ", tbl, ";\nCREATE VIEW ", tbl, " AS\n")
    else paste0("DROP TABLE IF EXISTS ", tbl, ";\n"),
    "select event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id\n",
    if (!use_view) paste0("INTO ", tbl, "\n") else "",
    "from\n(\n",
    "  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, ",
    "row_number() over (partition by pe.person_id order by pe.start_date ", q_sort, ", pe.event_id) as ordinal, ",
    "cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id\n",
    "  from ", pe_table, " pe", add_criteria_join, "\n",
    ") QE;"
  )
}

#' Emit inclusion rule SQL.
#' Creates a temp table with matching person_id, event_id for this rule.
#' @noRd
emit_inclusion_rule <- function(node, dag, options) {
  def <- node$definition
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  cs_map <- dag$cs_map

  qe_node <- dag$nodes[[def$qe_hash]]
  qe_table <- qualify_table(qe_node$temp_table, options)

  # Remap codeset IDs in the expression to global IDs
  expr_remapped <- remap_codeset_ids(def$expression, def$cohort_idx, cs_map)
  cg_sql <- get_criteria_group_query(expr_remapped, qe_table, cdm_schema)
  cg_sql <- gsub("@indexId", "0", cg_sql, fixed = TRUE)

  tbl <- qualify_table(node$temp_table, options)
  use_view <- isTRUE(node$is_view)
  paste0(
    if (use_view) paste0("DROP VIEW IF EXISTS ", tbl, ";\nCREATE VIEW ", tbl, " AS\n")
    else paste0("DROP TABLE IF EXISTS ", tbl, ";\n"),
    "select person_id, event_id\n",
    if (!use_view) paste0("INTO ", tbl, "\n") else "",
    "FROM (\n",
    "  select pe.person_id, pe.event_id\n",
    "  FROM ", qe_table, " pe\n",
    "  JOIN (\n", cg_sql, ") AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id\n",
    ") Results;"
  )
}

#' Emit included events SQL.
#' Unions inclusion rules, applies bitmask, applies ExpressionLimit.
#' @noRd
emit_included_events <- function(node, dag, options) {
  def <- node$definition
  qe_node <- dag$nodes[[def$qe_hash]]
  qe_table <- qualify_table(qe_node$temp_table, options)

  ir_hashes <- def$ir_hashes
  rule_count <- length(ir_hashes)

  tbl <- qualify_table(node$temp_table, options)

  # Build inclusion_events CTE (or empty subquery) from inclusion rule tables
  if (rule_count > 0) {
    ir_unions <- character(rule_count)
    for (idx in seq_along(ir_hashes)) {
      ir_node <- dag$nodes[[ir_hashes[idx]]]
      ir_unions[idx] <- paste0("select ", idx - 1L, " as inclusion_rule_id, person_id, event_id from ", qualify_table(ir_node$temp_table, options))
    }
    ie_cte <- paste0(
      "inclusion_events AS (\n",
      "  SELECT inclusion_rule_id, person_id, event_id FROM (\n",
      paste(ir_unions, collapse = "\nUNION ALL\n"),
      "\n  ) I\n)")
  } else {
    ie_cte <- paste0(
      "inclusion_events AS (\n",
      "  SELECT CAST(NULL AS bigint) as inclusion_rule_id, CAST(NULL AS bigint) as person_id, CAST(NULL AS bigint) as event_id WHERE 1=0\n)")
  }

  # Included events with bitmask
  el_sort <- def$el_sort %||% "ASC"
  el_type <- toupper(def$el_type %||% "ALL")
  result_limit <- if (el_type != "ALL") "where results.ordinal = 1" else ""

  if (rule_count > 0) {
    mask_filter <- paste0(
      "\n  where (MG.inclusion_rule_mask = POWER(cast(2 as bigint),", rule_count, ")-1)\n"
    )
  } else {
    mask_filter <- ""
  }

  # Single statement: DROP + WITH CTE + SELECT INTO (or CREATE VIEW)
  use_view <- isTRUE(node$is_view)

  # When el_type = "ALL", skip the unnecessary row_number() + outer subquery.
  # The ordinal is only needed when filtering to first/last event per person.
  bitmask_query <- paste0(
    "    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, ",
    "SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask\n",
    "    from ", qe_table, " Q\n",
    "    left join inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id\n",
    "    group by Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date\n"
  )

  if (el_type == "ALL") {
    # No row_number needed — just filter by mask and pass through
    select_body <- paste0(
      "select event_id, person_id, start_date, end_date, op_start_date, op_end_date\n",
      if (!use_view) paste0("into ", tbl, "\n") else "",
      "from (\n",
      bitmask_query,
      "  ) MG -- matching groups\n",
      mask_filter, ";"
    )
  } else {
    # Need row_number + WHERE ordinal = 1
    select_body <- paste0(
      "select event_id, person_id, start_date, end_date, op_start_date, op_end_date\n",
      if (!use_view) paste0("into ", tbl, "\n") else "",
      "from (\n",
      "  select event_id, person_id, start_date, end_date, op_start_date, op_end_date, ",
      "row_number() over (partition by person_id order by start_date ", el_sort, ") as ordinal\n",
      "  from\n  (\n",
      bitmask_query,
      "  ) MG -- matching groups\n",
      mask_filter,
      ") Results\n",
      "where results.ordinal = 1;"
    )
  }

  paste0(
    if (use_view) paste0("DROP VIEW IF EXISTS ", tbl, ";\nCREATE VIEW ", tbl, " AS\n")
    else paste0("DROP TABLE IF EXISTS ", tbl, ";\n"),
    "WITH ", ie_cte, "\n",
    select_body
  )
}

#' Emit cohort exit SQL.
#' Handles DateOffset, CustomEra, default exit, and censoring.
#' @noRd
emit_cohort_exit <- function(node, dag, options) {
  def <- node$definition
  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"

  ie_node <- dag$nodes[[def$ie_hash]]
  ie_table <- qualify_table(ie_node$temp_table, options)
  tbl <- qualify_table(node$temp_table, options)
  cs_tbl <- qualify_table("codesets", options)

  end_strategy <- def$end_strategy
  cs_map <- dag$cs_map
  cohort_idx <- def$cohort_idx
  cohort_end_unions <- character(0)
  pre_sql <- ""    # SQL to run before main SELECT INTO (e.g., create dt_tbl)
  post_sql <- ""   # SQL to run after main SELECT INTO (e.g., drop dt_tbl)

  has_custom_end <- FALSE
  if (!is.null(end_strategy)) {
    doff <- end_strategy$DateOffset %||% end_strategy$dateOffset
    if (!is.null(doff)) {
      has_custom_end <- TRUE
      offset <- as.character(doff$Offset %||% doff$offset %||% 0)
      date_field <- doff$DateField %||% doff$dateField %||% "StartDate"
      date_col <- if (identical(date_field, "StartDate")) "start_date" else "end_date"

      # Inline DateOffset as subquery — no intermediate table needed
      cohort_end_unions <- c(cohort_end_unions,
        paste0("select event_id, person_id,\n",
               "  case when DATEADD(day,", offset, ",", date_col, ") > op_end_date ",
               "then op_end_date else DATEADD(day,", offset, ",", date_col, ") end as end_date\n",
               "from ", ie_table))
    }

    custom_era <- end_strategy$CustomEra %||% end_strategy$customEra
    if (!is.null(custom_era)) {
      has_custom_end <- TRUE
      drug_cs <- custom_era$DrugCodesetId %||% custom_era$drugCodesetId
      cond_cs <- custom_era$ConditionCodesetId %||% custom_era$conditionCodesetId
      # Remap local codeset IDs to global IDs
      if (!is.null(drug_cs) && !is.null(cohort_idx)) {
        key <- paste0(cohort_idx, ":", drug_cs)
        drug_cs <- cs_map$local_to_global[[key]] %||% drug_cs
      }
      if (!is.null(cond_cs) && !is.null(cohort_idx)) {
        key <- paste0(cohort_idx, ":", cond_cs)
        cond_cs <- cs_map$local_to_global[[key]] %||% cond_cs
      }

      gap_days <- as.integer(custom_era$GapDays %||% custom_era$gapDays %||% 0L)
      offset <- as.integer(custom_era$Offset %||% custom_era$offset %||% 0L)
      dt_tbl <- qualify_table(paste0(node$temp_table, "_dt"), options)

      # Era-building subquery (shared between drug and condition paths)
      # Era-fy 2.0: unified sort order for both window functions
      era_subquery <- paste0(
        "select person_id, min(start_date) as era_start_date, DATEADD(day,-1 * ", gap_days, ", max(end_date)) as era_end_date\n",
        "  from (\n",
        "    select person_id, start_date, end_date, sum(is_start) over (partition by person_id order by start_date rows unbounded preceding) group_idx\n",
        "    from (\n",
        "      select person_id, start_date, end_date, \n",
        "        case when max(end_date) over (partition by person_id order by start_date rows between unbounded preceding and 1 preceding) >= start_date then 0 else 1 end is_start\n",
        "      from (\n")

      if (!is.null(drug_cs)) {
        # Build drug eras from raw DRUG_EXPOSURE (matching CirceR behavior)
        # Step 1: drugTarget needs a real table (referenced in era subquery)
        pre_sql <- paste0(
          "DROP TABLE IF EXISTS ", dt_tbl, ";\n",
          "with ctePersons(person_id) as (\n",
          "\tselect distinct person_id from ", ie_table, "\n)\n",
          "select person_id, drug_exposure_start_date, drug_exposure_end_date\n",
          "INTO ", dt_tbl, "\nFROM (\n",
          "\tselect de.PERSON_ID, DRUG_EXPOSURE_START_DATE, COALESCE(DRUG_EXPOSURE_END_DATE, DATEADD(day,DAYS_SUPPLY,DRUG_EXPOSURE_START_DATE), DATEADD(day,1,DRUG_EXPOSURE_START_DATE)) as DRUG_EXPOSURE_END_DATE \n",
          "\tFROM ", cdm_schema, ".DRUG_EXPOSURE de\n",
          "\tJOIN ctePersons p on de.person_id = p.person_id\n",
          "\tJOIN ", cs_tbl, " cs on cs.codeset_id = ", drug_cs, " AND de.drug_concept_id = cs.concept_id\n",
          "\n\tUNION ALL\n\n",
          "\tselect de.PERSON_ID, DRUG_EXPOSURE_START_DATE, COALESCE(DRUG_EXPOSURE_END_DATE, DATEADD(day,DAYS_SUPPLY,DRUG_EXPOSURE_START_DATE), DATEADD(day,1,DRUG_EXPOSURE_START_DATE)) as DRUG_EXPOSURE_END_DATE \n",
          "\tFROM ", cdm_schema, ".DRUG_EXPOSURE de\n",
          "\tJOIN ctePersons p on de.person_id = p.person_id\n",
          "\tJOIN ", cs_tbl, " cs on cs.codeset_id = ", drug_cs, " AND de.drug_source_concept_id = cs.concept_id\n",
          ") E;"
        )
        post_sql <- paste0("DROP TABLE IF EXISTS ", dt_tbl, ";")
        # Step 2: Inline strategy ends as subquery (no se_tbl needed)
        cohort_end_unions <- c(cohort_end_unions,
          paste0(
            "select et.event_id, et.person_id, ERAS.era_end_date as end_date\n",
            "from ", ie_table, " et\n",
            "JOIN \n(\n\n",
            era_subquery,
            "        select person_id, drug_exposure_start_date as start_date, DATEADD(day,(", gap_days, " + ", offset, "),DRUG_EXPOSURE_END_DATE) as end_date\n",
            "        FROM ", dt_tbl, "\n",
            "      ) DT\n",
            "    ) ST\n",
            "  ) GR\n",
            "  group by person_id, group_idx\n",
            ") ERAS on ERAS.person_id = et.person_id \n",
            "WHERE et.start_date between ERAS.era_start_date and ERAS.era_end_date"))
      } else if (!is.null(cond_cs)) {
        # Build condition eras from raw CONDITION_OCCURRENCE (matching CirceR behavior)
        pre_sql <- paste0(
          "DROP TABLE IF EXISTS ", dt_tbl, ";\n",
          "with ctePersons(person_id) as (\n",
          "\tselect distinct person_id from ", ie_table, "\n)\n",
          "select person_id, condition_start_date, condition_end_date\n",
          "INTO ", dt_tbl, "\nFROM (\n",
          "\tselect co.PERSON_ID, CONDITION_START_DATE, COALESCE(CONDITION_END_DATE, DATEADD(day,1,CONDITION_START_DATE)) as CONDITION_END_DATE \n",
          "\tFROM ", cdm_schema, ".CONDITION_OCCURRENCE co\n",
          "\tJOIN ctePersons p on co.person_id = p.person_id\n",
          "\tJOIN ", cs_tbl, " cs on cs.codeset_id = ", cond_cs, " AND co.condition_concept_id = cs.concept_id\n",
          ") E;"
        )
        post_sql <- paste0("DROP TABLE IF EXISTS ", dt_tbl, ";")
        # Inline strategy ends as subquery
        cohort_end_unions <- c(cohort_end_unions,
          paste0(
            "select et.event_id, et.person_id, ERAS.era_end_date as end_date\n",
            "from ", ie_table, " et\n",
            "JOIN \n(\n\n",
            era_subquery,
            "        select person_id, condition_start_date as start_date, DATEADD(day,(", gap_days, " + ", offset, "),CONDITION_END_DATE) as end_date\n",
            "        FROM ", dt_tbl, "\n",
            "      ) DT\n",
            "    ) ST\n",
            "  ) GR\n",
            "  group by person_id, group_idx\n",
            ") ERAS on ERAS.person_id = et.person_id \n",
            "WHERE et.start_date between ERAS.era_start_date and ERAS.era_end_date"))
      }
    }
  }

  if (!has_custom_end) {
    # No end strategy: default exit at observation period end date
    cohort_end_unions <- c(
      paste0("select event_id, person_id, op_end_date as end_date from ", ie_table),
      cohort_end_unions
    )
  } else {
    # For CustomEra, also include op_end_date as a fallback/ceiling (matching CirceR).
    # CustomEra eras may not cover all events, so op_end_date ensures a valid end date.
    # DateOffset always produces an end date for every event, so no fallback needed.
    custom_era_check <- if (!is.null(end_strategy))
      end_strategy$CustomEra %||% end_strategy$customEra else NULL
    if (!is.null(custom_era_check)) {
      cohort_end_unions <- c(
        paste0("select event_id, person_id, op_end_date as end_date from ", ie_table),
        cohort_end_unions
      )
    }
  }

  # Censoring criteria
  censoring <- def$censoring %||% list()
  if (length(censoring) > 0) {
    censor_queries <- character(0)
    for (cc in censoring) {
      cc_remapped <- remap_codeset_ids(cc, cohort_idx, cs_map)
      cq <- get_criteria_sql(cc_remapped, cdm_schema)
      censor_queries <- c(censor_queries, paste0(
        "select i.event_id, i.person_id, MIN(c.start_date) as end_date\n",
        "from ", ie_table, " i\n",
        "join\n(\n", cq, "\n) C on C.person_id = I.person_id ",
        "and C.start_date >= I.start_date and C.START_DATE <= I.op_end_date\n",
        "group by i.event_id, i.person_id"))
    }
    cohort_end_unions <- c(cohort_end_unions,
      paste0("-- Censor Events\n", paste(censor_queries, collapse = "\nUNION ALL\n")))
  }

  # Assemble: pre_sql + main SELECT INTO/VIEW + post_sql
  # CustomEra nodes have pre_sql/post_sql (dt_tbl lifecycle) — can't be VIEWs
  # because the VIEW would reference dt_tbl which gets dropped in post_sql.
  use_view <- isTRUE(node$is_view) && !nzchar(pre_sql)
  all_ends <- paste(cohort_end_unions, collapse = "\nUNION ALL\n")

  main_sql <- paste0(
    if (use_view) paste0("DROP VIEW IF EXISTS ", tbl, ";\nCREATE VIEW ", tbl, " AS\n")
    else paste0("DROP TABLE IF EXISTS ", tbl, ";\n"),
    "SELECT event_id, person_id, end_date\n",
    if (!use_view) paste0("INTO ", tbl, "\n") else "",
    "FROM (\n",
    all_ends, "\n) CE;"
  )

  paste0(
    if (nzchar(pre_sql)) paste0(pre_sql, "\n") else "",
    main_sql,
    if (nzchar(post_sql)) paste0("\n", post_sql) else ""
  )
}

#' Emit final cohort SQL.
#' Uses CTEs to avoid creating intermediate tables (cohort_rows, final_cohort).
#' Everything is done in a single INSERT ... WITH ... SELECT statement.
#' @noRd
emit_final_cohort <- function(node, dag, options) {
  def <- node$definition
  ce_node <- dag$nodes[[def$ce_hash]]
  ie_node <- dag$nodes[[dag$nodes[[def$ce_hash]]$definition$ie_hash]]

  cohort_id <- def$cohort_id
  era_pad <- as.integer(def$era_pad %||% 0L)

  ie_tbl <- qualify_table(ie_node$temp_table, options)
  ce_tbl <- qualify_table(ce_node$temp_table, options)
  cs_tbl <- qualify_table("cohort_stage", options)

  # CensorWindow handling
  censor_window <- def$censor_window
  start_date_expr <- "start_date"
  end_date_expr <- "end_date"
  if (!is.null(censor_window) && is.list(censor_window) && length(censor_window) > 0) {
    if (!is.null(censor_window$startDate) && nzchar(censor_window$startDate %||% ""))
      start_date_expr <- paste0("CASE WHEN start_date > ", date_string_to_sql(censor_window$startDate),
                                 " THEN start_date ELSE ", date_string_to_sql(censor_window$startDate), " END")
    if (!is.null(censor_window$endDate) && nzchar(censor_window$endDate %||% ""))
      end_date_expr <- paste0("CASE WHEN end_date < ", date_string_to_sql(censor_window$endDate),
                               " THEN end_date ELSE ", date_string_to_sql(censor_window$endDate), " END")
  }

  # Single INSERT with CTEs — no intermediate tables created.
  # CTEs must come before INSERT INTO (SQL Server requires this).
  paste0(
    "-- Final cohort for cohort_id=", cohort_id, "\n",
    "WITH cohort_rows AS (\n",
    "  select person_id, start_date, end_date\n",
    "  from (\n",
    "\tselect F.person_id, F.start_date, F.end_date\n",
    "\tFROM (\n",
    "\t  select I.event_id, I.person_id, I.start_date, CE.end_date, ",
    "row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal\n",
    "\t  from ", ie_tbl, " I\n",
    "\t  join ", ce_tbl, " CE on I.event_id = CE.event_id and I.person_id = CE.person_id ",
    "and CE.end_date >= I.start_date\n",
    "\t) F\n",
    "\tWHERE F.ordinal = 1\n",
    "  ) FE\n",
    "),\n",
    "-- Era-fy 2.0: pre-aggregate duplicates, then unified sort for both window functions\n",
    "cohort_rows_deduped AS (\n",
    "  select person_id, start_date, max(DATEADD(day,", era_pad, ",end_date)) as end_date\n",
    "  from cohort_rows\n",
    "  group by person_id, start_date\n",
    "),\n",
    "final_cohort AS (\n",
    "  select person_id, min(start_date) as start_date, ",
    "DATEADD(day,-1 * ", era_pad, ", max(end_date)) as end_date\n",
    "  from (\n",
    "    select person_id, start_date, end_date, ",
    "sum(is_start) over (partition by person_id order by start_date rows unbounded preceding) group_idx\n",
    "    from (\n",
    "      select person_id, start_date, end_date,\n",
    "        case when max(end_date) over (partition by person_id order by start_date rows between unbounded preceding and 1 preceding) >= start_date then 0 else 1 end is_start\n",
    "      from cohort_rows_deduped\n",
    "    ) ST\n",
    "  ) GR\n",
    "  group by person_id, group_idx\n",
    ")\n",
    "INSERT INTO ", cs_tbl, " (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)\n",
    "select ", cohort_id, " as cohort_definition_id, person_id, ",
    start_date_expr, ", ", end_date_expr, "\n",
    "FROM final_cohort CO;"
  )
}

#' Emit finalize block: staging -> output tables.
#' @noRd
emit_finalize <- function(dag, options) {
  results_schema <- options$results_schema %||% "@results_schema"
  batch_ids <- as.integer(dag$cohort_ids)
  batch_id_list <- paste(batch_ids, collapse = ",")

  finalize_tables <- list(
    list(tbl = "@target_database_schema.@target_cohort_table", cols = "cohort_definition_id, subject_id, cohort_start_date, cohort_end_date", stage = qualify_table("cohort_stage", options)),
    list(tbl = paste0(results_schema, ".inclusion_events"), cols = "cohort_definition_id, inclusion_rule_id, person_id, event_id", stage = qualify_table("inclusion_events_stage", options)),
    list(tbl = paste0(results_schema, ".inclusion_stats"), cols = "cohort_definition_id, inclusion_rule_id, person_count, gain_count, person_total", stage = qualify_table("inclusion_stats_stage", options))
  )

  # Pre-allocate: header(1) + 2 lines per finalize table(3)
  out <- character(1L + 2L * length(finalize_tables))
  out[1L] <- "-- Finalize: delete this batch's cohort IDs, then insert from staging."
  oi <- 1L
  for (ft in finalize_tables) {
    oi <- oi + 1L; out[oi] <- sprintf("DELETE FROM %s WHERE cohort_definition_id IN (%s);", ft$tbl, batch_id_list)
    oi <- oi + 1L; out[oi] <- sprintf("INSERT INTO %s (%s)\nSELECT %s FROM %s;", ft$tbl, ft$cols, ft$cols, ft$stage)
  }
  c("", out)
}

#' Emit cleanup: drop all DAG temp tables plus staging.
#' Uses pre-allocated vector to avoid O(n^2) growth.
#' @noRd
emit_cleanup <- function(dag, options) {
  # Drop node temp tables (reverse topological order)
  sorted <- rev(topological_sort(dag$nodes))

  # Pre-allocate: header(2) + up to 3 drops per node + shared(5) + domain(1+7)
  n_sorted <- length(sorted)
  out <- character(2L + 3L * n_sorted + 5L + 1L + length(DOMAIN_CONFIG))
  oi <- 0L

  oi <- oi + 1L; out[oi] <- ""
  oi <- oi + 1L; out[oi] <- "-- Cleanup: drop all temp tables."

  for (node_id in sorted) {
    node <- dag$nodes[[node_id]]
    if (node$type == "concept_set") next  # concept sets are in codesets table

    drop_type <- if (isTRUE(node$is_view)) "DROP VIEW IF EXISTS " else "DROP TABLE IF EXISTS "
    oi <- oi + 1L; out[oi] <- paste0(drop_type, qualify_table(node$temp_table, options), ";")

    # Drop auxiliary tables for cohort_exit (_se still exists for CustomEra)
    if (node$type == "cohort_exit") {
      oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table(paste0(node$temp_table, "_se"), options), ";")
    }
  }

  # Drop shared tables
  oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table("codesets", options), ";")
  oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table("all_concepts", options), ";")
  oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table("cohort_stage", options), ";")
  oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table("inclusion_events_stage", options), ";")
  oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table("inclusion_stats_stage", options), ";")

  # Drop domain filtered tables
  oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table("atlas_observation_period", options), ";")
  for (dc in DOMAIN_CONFIG) {
    oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table(dc$filtered, options), ";")
  }

  out[seq_len(oi)]
}

# ---- Codeset ID remapping ----

#' Remap CodesetId references in a criteria item from local to global IDs.
#' Deep-clones the criteria and replaces all CodesetId fields.
#' @param item Criteria list item (from CriteriaList)
#' @param cohort_idx Integer cohort index in batch
#' @param cs_map Concept set map
#' @return Modified criteria item
#' @noRd
remap_codeset_ids <- function(item, cohort_idx, cs_map) {
  if (!is.list(item)) return(item)

  # Deep clone
  item <- rapply(item, identity, how = "replace")

  # Replace CodesetId at all levels
  remap_recursive <- function(x) {
    if (!is.list(x)) return(x)
    nms <- names(x)
    if (!is.null(nms)) {
      for (field_name in c("CodesetId", "codesetId",
                            "DrugCodesetId", "drugCodesetId",
                            "ConditionCodesetId", "conditionCodesetId",
                            "ConditionSourceConcept", "conditionSourceConcept",
                            "DrugSourceConcept", "drugSourceConcept",
                            "ProcedureSourceConcept", "procedureSourceConcept",
                            "VisitSourceConcept", "visitSourceConcept",
                            "MeasurementSourceConcept", "measurementSourceConcept",
                            "ObservationSourceConcept", "observationSourceConcept",
                            "DeviceSourceConcept", "deviceSourceConcept",
                            "SpecimenSourceConcept", "specimenSourceConcept",
                            "DeathSourceConcept", "deathSourceConcept",
                            "CauseSourceConcept", "causeSourceConcept")) {
        if (field_name %in% nms) {
          val <- x[[field_name]]
          if (!is.null(val) && is.numeric(val)) {
            key <- paste0(cohort_idx, ":", val)
            global <- cs_map$local_to_global[[key]]
            if (!is.null(global)) {
              x[[field_name]] <- global
            }
          }
        }
      }
    }
    for (i in seq_along(x)) {
      if (!is.null(x[[i]])) {
        # Use x[i] <- list(...) so that NULL return value does not remove the element
        x[i] <- list(remap_recursive(x[[i]]))
      }
    }
    x
  }

  remap_recursive(item)
}
