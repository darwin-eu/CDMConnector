# dag_cache.R
# Persistent caching layer for the execution graph (DAG).
# Stores content-hashed intermediate computation tables in the database,
# allowing incremental recomputation when cohort definitions change.
#
# The cache uses a registry table that maps node hashes to materialized table
# names.  Because hashes are Merkle-tree style (each node's hash includes
# its dependency hashes), a change in any upstream definition automatically
# invalidates all downstream nodes -- no explicit invalidation logic needed.

# ---- Constants ----

#' Fixed table prefix used for all cached DAG tables.
#' Unlike the random `atlas_<uuid>_` prefix used for ephemeral runs, cached
#' tables use this stable prefix so the same hash always maps to the same
#' table name.
#' @noRd
CACHE_TABLE_PREFIX <- "dagcache_"

#' Registry table bare name (without schema or prefix).
#' @noRd
CACHE_REGISTRY_TABLE <- "dag_cache_registry"

# ---- Registry DDL ----

#' Build the fully qualified registry table name.
#' @param schema Character schema name (e.g. "results" or a resolved schema string).
#' @return Qualified name like "results.dag_cache_registry".
#' @noRd
cache_registry_qname <- function(schema) {
  paste0(schema, ".", CACHE_REGISTRY_TABLE)
}

#' SQL to create the cache registry table if it does not exist.
#' The registry stores one row per cached node.
#'
#' Columns:
#'   node_hash   - 16-char hex content hash (primary key)
#'   node_type   - node type (concept_set, primary_events, etc.)
#'   table_name  - fully qualified materialized table name
#'   created_at  - timestamp when the node was materialized
#'   last_used_at - timestamp of last access (for GC decisions)
#'   cohort_ids  - comma-separated list of cohort IDs that used this node
#'
#' @param schema Character schema name.
#' @return Character vector of SQL statements.
#' @noRd
cache_registry_ddl <- function(schema) {
  tbl <- cache_registry_qname(schema)
  c(
    sprintf("CREATE TABLE IF NOT EXISTS %s (", tbl),
    "  node_hash VARCHAR(16) NOT NULL,",
    "  node_type VARCHAR(32) NOT NULL,",
    "  table_name VARCHAR(255) NOT NULL,",
    "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
    "  last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
    "  cohort_ids VARCHAR(1000) DEFAULT '',",
    sprintf("  PRIMARY KEY (node_hash));")
  )
}

#' Ensure the cache registry table exists.
#' @param con DBI connection.
#' @param schema Character schema name (resolved, not a placeholder).
#' @noRd
ensure_cache_registry <- function(con, schema) {
  ddl <- paste(cache_registry_ddl(schema), collapse = "\n")
  tryCatch(
    DBI::dbExecute(con, ddl),
    error = function(e) {
      # Table may already exist; that's fine
      if (!grepl("already exists", conditionMessage(e), ignore.case = TRUE)) {
        stop(e)
      }
    }
  )
  invisible(TRUE)
}

# ---- Registry queries ----

#' Look up which node hashes are already cached.
#'
#' @param con DBI connection.
#' @param schema Character schema name.
#' @param hashes Character vector of node hashes to check.
#' @return Named logical vector: TRUE if hash exists in registry.
#' @noRd
cache_lookup <- function(con, schema, hashes) {
  if (length(hashes) == 0L) return(logical(0))
  tbl <- cache_registry_qname(schema)
  # Query in batches to avoid SQL IN-clause limits
  found <- character(0)
  batch_size <- 500L
  for (start in seq(1L, length(hashes), by = batch_size)) {
    end <- min(start + batch_size - 1L, length(hashes))
    batch <- hashes[start:end]
    quoted <- paste0("'", batch, "'", collapse = ",")
    sql <- sprintf("SELECT node_hash FROM %s WHERE node_hash IN (%s)", tbl, quoted)
    result <- tryCatch(DBI::dbGetQuery(con, sql), error = function(e) data.frame(node_hash = character(0)))
    found <- c(found, result$node_hash)
  }
  setNames(hashes %in% found, hashes)
}

#' Validate that cached tables still physically exist in the database.
#'
#' @param con DBI connection.
#' @param schema Character schema name.
#' @param hashes Character vector of node hashes to validate.
#' @return Named logical vector: TRUE if hash is in registry AND table exists.
#' @noRd
cache_validate <- function(con, schema, hashes) {
  if (length(hashes) == 0L) return(logical(0))
  tbl <- cache_registry_qname(schema)
  # Get table names for these hashes
  quoted <- paste0("'", hashes, "'", collapse = ",")
  sql <- sprintf("SELECT node_hash, table_name FROM %s WHERE node_hash IN (%s)", tbl, quoted)
  registry <- tryCatch(DBI::dbGetQuery(con, sql), error = function(e) {
    data.frame(node_hash = character(0), table_name = character(0))
  })
  if (nrow(registry) == 0L) return(setNames(rep(FALSE, length(hashes)), hashes))

  # Check each table exists
  all_tables <- tryCatch(tolower(DBI::dbListTables(con)), error = function(e) character(0))
  valid <- vapply(seq_len(nrow(registry)), function(i) {
    # Extract bare table name from qualified name (schema.table -> table)
    parts <- strsplit(registry$table_name[i], ".", fixed = TRUE)[[1]]
    bare <- tolower(parts[length(parts)])
    bare %in% all_tables
  }, logical(1))

  result <- setNames(rep(FALSE, length(hashes)), hashes)
  for (i in seq_len(nrow(registry))) {
    result[[registry$node_hash[i]]] <- valid[i]
  }
  result
}

#' Register a newly materialized node in the cache.
#'
#' @param con DBI connection.
#' @param schema Character schema name.
#' @param node_hash Character hash.
#' @param node_type Character node type.
#' @param table_name Character fully qualified table name.
#' @param cohort_ids Integer vector of cohort IDs.
#' @noRd
cache_register <- function(con, schema, node_hash, node_type, table_name, cohort_ids = integer(0)) {
  reg_tbl <- cache_registry_qname(schema)
  cids <- paste(as.integer(cohort_ids), collapse = ",")
  sql <- sprintf(
    "INSERT INTO %s (node_hash, node_type, table_name, created_at, last_used_at, cohort_ids) VALUES ('%s', '%s', '%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '%s')",
    reg_tbl, node_hash, node_type, table_name, cids
  )
  tryCatch(DBI::dbExecute(con, sql), error = function(e) {
    # If duplicate key, update last_used_at instead
    if (grepl("duplicate|unique|constraint|already exists", conditionMessage(e), ignore.case = TRUE)) {
      update_sql <- sprintf(
        "UPDATE %s SET last_used_at = CURRENT_TIMESTAMP, cohort_ids = '%s' WHERE node_hash = '%s'",
        reg_tbl, cids, node_hash
      )
      DBI::dbExecute(con, update_sql)
    } else {
      stop(e)
    }
  })
  invisible(TRUE)
}

#' Touch (update last_used_at) for cache hits so GC knows they are still active.
#'
#' @param con DBI connection.
#' @param schema Character schema name.
#' @param hashes Character vector of node hashes that were cache hits.
#' @noRd
cache_touch <- function(con, schema, hashes) {
  if (length(hashes) == 0L) return(invisible(NULL))
  reg_tbl <- cache_registry_qname(schema)
  quoted <- paste0("'", hashes, "'", collapse = ",")
  sql <- sprintf("UPDATE %s SET last_used_at = CURRENT_TIMESTAMP WHERE node_hash IN (%s)",
                 reg_tbl, quoted)
  tryCatch(DBI::dbExecute(con, sql), error = function(e) NULL)
  invisible(NULL)
}

# ---- Cache-aware SQL emission ----

#' Build the stable cached table name for a node.
#' Uses the fixed CACHE_TABLE_PREFIX so the same hash always yields the same name.
#'
#' @param node A DAG node (list with type, id, temp_table fields).
#' @param schema Character schema name.
#' @return Fully qualified table name like "results.dagcache_pe_a1b2c3d4".
#' @noRd
cache_table_name <- function(node, schema) {
  paste0(schema, ".", CACHE_TABLE_PREFIX, node$temp_table)
}

#' Emit DAG SQL with caching support.
#'
#' Like \code{emit_dag_sql} but:
#' 1. Uses stable \code{CACHE_TABLE_PREFIX} instead of random prefix.
#' 2. Before emitting each node, checks if its hash exists in the registry
#'    (and the table physically exists). If so, skips emission.
#' 3. After emission, registers newly materialized nodes.
#' 4. Does NOT drop cached node tables in cleanup (only drops ephemeral tables).
#'
#' @param dag DAG from \code{build_execution_dag}.
#' @param options List with cdm_schema, results_schema, vocabulary_schema, table_prefix.
#' @param con DBI connection (required for registry lookups).
#' @param schema Character resolved schema name (for registry table).
#' @return List with:
#'   \code{sql} - SQL string to execute (only non-cached portions),
#'   \code{cache_hits} - character vector of node hashes that were cache hits,
#'   \code{cache_misses} - character vector of node hashes that need computation.
#' @noRd
emit_dag_sql_cached <- function(dag, options, con, schema) {
  # Override table_prefix to the stable cache prefix
  options$table_prefix <- CACHE_TABLE_PREFIX

  cdm_schema <- options$cdm_schema %||% "@cdm_database_schema"
  results_schema <- options$results_schema %||% "@results_schema"
  vocab_schema <- options$vocabulary_schema %||% "@vocabulary_database_schema"

  sorted <- topological_sort(dag$nodes)

  # Determine which nodes are already cached
  all_hashes <- names(dag$nodes)
  # Exclude concept_set nodes (they go into the global codesets table, not individual tables)
  computable_hashes <- vapply(all_hashes, function(h) dag$nodes[[h]]$type != "concept_set", logical(1))
  computable_hashes <- all_hashes[computable_hashes]

  cached <- cache_validate(con, schema, computable_hashes)
  cache_hits <- names(cached[cached])
  cache_misses <- names(cached[!cached])

  # Touch cache hits so GC knows they are active
  if (length(cache_hits) > 0) cache_touch(con, schema, cache_hits)

  # Pre-allocate list accumulator to avoid O(n^2) vector growth
  n_sorted <- length(sorted)
  chunks <- vector("list", n_sorted + 6L)
  ci <- 0L

  # 1. Preamble (staging tables) -- always emitted
  ci <- ci + 1L; chunks[[ci]] <- emit_preamble(options)

  # 2. Global #Codesets -- always emitted (concept sets are cheap and shared)
  ci <- ci + 1L; chunks[[ci]] <- emit_codesets(dag, options)

  # 3. Domain filtered tables -- always emitted (ephemeral, not cached)
  ci <- ci + 1L; chunks[[ci]] <- emit_domain_filtered(dag$used_tables, cdm_schema, options)

  # 4. Set sql_context for circe builders

  old_cs <- .sql_context$codesets_table
  .sql_context$codesets_table <- qualify_table("codesets", options)
  on.exit(.sql_context$codesets_table <- old_cs, add = TRUE)

  # 5. Emit each node in topological order, skipping cache hits
  for (node_id in sorted) {
    node <- dag$nodes[[node_id]]
    if (node$type == "concept_set") next  # handled in codesets

    if (node_id %in% cache_hits) {
      ci <- ci + 1L
      chunks[[ci]] <- c(
        sprintf("-- [%s] %s CACHE HIT (skip)", node$type, substr(node$id, 1, 8)),
        "")
      next
    }

    node_sql <- emit_node_sql(node, dag, options)
    if (nzchar(node_sql)) {
      ci <- ci + 1L
      chunks[[ci]] <- c(
        sprintf("-- [%s] %s (cohorts: %s)", node$type, substr(node$id, 1, 8),
                paste(node$cohort_ids, collapse = ",")),
        node_sql, "")
    }
  }

  # 6. Finalize: staging -> output tables
  ci <- ci + 1L; chunks[[ci]] <- emit_finalize(dag, options)

  # 7. Selective cleanup: drop ephemeral tables but NOT cached node tables
  ci <- ci + 1L; chunks[[ci]] <- emit_cleanup_cached(dag, options, cache_hits, cache_misses)

  list(
    sql = stringi::stri_join(unlist(chunks[seq_len(ci)]), collapse = "\n"),
    cache_hits = cache_hits,
    cache_misses = cache_misses
  )
}

#' Register newly computed nodes after successful SQL execution.
#'
#' Call this after the SQL from \code{emit_dag_sql_cached} has been executed
#' successfully. Inserts registry entries for all cache misses.
#'
#' @param dag DAG from \code{build_execution_dag}.
#' @param options Options with table_prefix = CACHE_TABLE_PREFIX.
#' @param con DBI connection.
#' @param schema Character resolved schema.
#' @param cache_misses Character vector of node hashes that were computed.
#' @noRd
cache_register_new_nodes <- function(dag, options, con, schema, cache_misses) {
  # Skip concept_set (stored globally) and final_cohort (dropped in cleanup
  # because its data is INSERT'd into cohort_stage; not worth caching since
  # recomputing from cached cohort_exit is cheap).
  skip_types <- c("concept_set", "final_cohort")
  for (h in cache_misses) {
    node <- dag$nodes[[h]]
    if (is.null(node) || node$type %in% skip_types) next
    tbl_name <- cache_table_name(node, schema)
    cache_register(con, schema, h, node$type, tbl_name, node$cohort_ids)
  }
  invisible(TRUE)
}

# ---- Selective cleanup ----

#' Emit cleanup SQL that preserves cached node tables.
#'
#' Drops:
#'   - Staging tables (cohort_stage, inclusion_events_stage, inclusion_stats_stage)
#'   - Domain filtered tables
#'   - Codesets / all_concepts tables
#'   - Auxiliary tables (_ie, _se suffixed)
#'
#' Does NOT drop:
#'   - Node tables for cache_hits (already existed)
#'   - Node tables for cache_misses (newly computed, to be cached)
#'
#' @param dag DAG
#' @param options Options
#' @param cache_hits Character vector of cached node hashes
#' @param cache_misses Character vector of newly computed node hashes
#' @noRd
emit_cleanup_cached <- function(dag, options, cache_hits, cache_misses) {
  # Drop auxiliary tables for ALL nodes (these are intermediate join tables, not cacheable)
  sorted <- rev(topological_sort(dag$nodes))
  n_sorted <- length(sorted)

  # Pre-allocate: header(2) + up to 2 drops per node + shared(5) + domain(1+7)
  out <- character(2L + 2L * n_sorted + 5L + 1L + length(DOMAIN_CONFIG))
  oi <- 0L

  oi <- oi + 1L; out[oi] <- ""
  oi <- oi + 1L; out[oi] <- "-- Cleanup: drop ephemeral tables (cached node tables preserved)."

  for (node_id in sorted) {
    node <- dag$nodes[[node_id]]
    if (node$type == "concept_set") next
    # Auxiliary tables (_ie for included_events, _se for cohort_exit) -- always dropped
    if (node$type == "included_events") {
      oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table(paste0(node$temp_table, "_ie"), options), ";")
    }
    if (node$type == "cohort_exit") {
      oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table(paste0(node$temp_table, "_se"), options), ";")
    }
    # Drop _cr auxiliary tables for final_cohort nodes
    if (node$type == "final_cohort") {
      oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table(paste0(node$temp_table, "_cr"), options), ";")
      # Final cohort main table is also ephemeral (its data goes to staging)
      oi <- oi + 1L; out[oi] <- paste0("DROP TABLE IF EXISTS ", qualify_table(node$temp_table, options), ";")
    }
  }

  # Drop shared ephemeral tables
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

# ---- Garbage collection ----

#' List all entries in the cache registry.
#'
#' @param con DBI connection.
#' @param schema Character schema name.
#' @return Data frame with columns: node_hash, node_type, table_name,
#'   created_at, last_used_at, cohort_ids.
#' @noRd
dag_cache_list <- function(con, schema) {
  ensure_cache_registry(con, schema)
  tbl <- cache_registry_qname(schema)
  tryCatch(
    DBI::dbGetQuery(con, sprintf("SELECT * FROM %s ORDER BY last_used_at DESC", tbl)),
    error = function(e) {
      data.frame(node_hash = character(0), node_type = character(0),
                 table_name = character(0), created_at = character(0),
                 last_used_at = character(0), cohort_ids = character(0))
    }
  )
}

#' Garbage-collect stale cache entries.
#'
#' Removes cache entries (and their backing tables) that have not been used
#' within the specified retention period, or that reference tables which no
#' longer exist in the database.
#'
#' @param con DBI connection.
#' @param schema Character schema name (resolved, not a placeholder).
#' @param max_age_days Numeric; entries not used in this many days are eligible
#'   for collection. Default 30. Set to 0 to collect all entries.
#' @param dry_run Logical; if TRUE, report what would be dropped without
#'   actually dropping anything. Default FALSE.
#' @return Data frame of removed (or would-be-removed) entries, invisibly.
#' @noRd
dag_cache_gc <- function(con, schema, max_age_days = 30, dry_run = FALSE) {
  ensure_cache_registry(con, schema)
  registry <- dag_cache_list(con, schema)
  if (nrow(registry) == 0L) {
    message("Cache is empty, nothing to collect.")
    return(invisible(registry))
  }

  # Identify stale entries by age
  now <- Sys.time()
  if (max_age_days > 0) {
    cutoff <- now - as.difftime(max_age_days, units = "days")
    last_used <- as.POSIXct(registry$last_used_at)
    stale_by_age <- is.na(last_used) | last_used < cutoff
  } else {
    stale_by_age <- rep(TRUE, nrow(registry))
  }

  # Identify orphaned entries (table no longer exists)
  all_tables <- tryCatch(tolower(DBI::dbListTables(con)), error = function(e) character(0))
  orphaned <- vapply(registry$table_name, function(tn) {
    parts <- strsplit(tn, ".", fixed = TRUE)[[1]]
    bare <- tolower(parts[length(parts)])
    !(bare %in% all_tables)
  }, logical(1))

  to_remove <- stale_by_age | orphaned
  removable <- registry[to_remove, , drop = FALSE]

  if (nrow(removable) == 0L) {
    message("No stale or orphaned entries found.")
    return(invisible(removable))
  }

  if (dry_run) {
    message(sprintf("Would remove %d cache entries (dry run):", nrow(removable)))
    for (i in seq_len(nrow(removable))) {
      reason <- if (orphaned[to_remove][i]) "orphaned" else "stale"
      message(sprintf("  %s [%s] %s (%s)", removable$node_hash[i], removable$node_type[i],
                       removable$table_name[i], reason))
    }
    return(invisible(removable))
  }

  # Drop tables and remove registry entries
  reg_tbl <- cache_registry_qname(schema)
  for (i in seq_len(nrow(removable))) {
    tbl_name <- removable$table_name[i]
    tryCatch(DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", tbl_name, ";")),
             error = function(e) NULL)
    tryCatch(DBI::dbExecute(con, sprintf("DELETE FROM %s WHERE node_hash = '%s'",
                                          reg_tbl, removable$node_hash[i])),
             error = function(e) NULL)
  }

  message(sprintf("Removed %d cache entries (%d stale, %d orphaned).",
                  nrow(removable), sum(stale_by_age[to_remove]), sum(orphaned[to_remove])))
  invisible(removable)
}

#' Clear the entire DAG cache.
#'
#' Drops all cached node tables and truncates the registry.
#'
#' @param con DBI connection.
#' @param schema Character schema name (resolved, not a placeholder).
#' @return Invisible TRUE.
#' @noRd
dag_cache_clear <- function(con, schema) {
  registry <- dag_cache_list(con, schema)
  if (nrow(registry) > 0L) {
    for (i in seq_len(nrow(registry))) {
      tryCatch(DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", registry$table_name[i], ";")),
               error = function(e) NULL)
    }
  }
  reg_tbl <- cache_registry_qname(schema)
  tryCatch(DBI::dbExecute(con, sprintf("DELETE FROM %s", reg_tbl)),
           error = function(e) NULL)
  message(sprintf("Cleared %d cache entries.", nrow(registry)))
  invisible(TRUE)
}

#' Get cache statistics.
#'
#' @param con DBI connection.
#' @param schema Character schema name.
#' @return List with total_entries, by_type (named integer vector), and
#'   oldest/newest timestamps.
#' @noRd
dag_cache_stats <- function(con, schema) {
  registry <- dag_cache_list(con, schema)
  if (nrow(registry) == 0L) {
    return(list(total_entries = 0L, by_type = integer(0),
                oldest = NA, newest = NA))
  }
  by_type <- table(registry$node_type)
  list(
    total_entries = nrow(registry),
    by_type = as.integer(by_type) |> setNames(names(by_type)),
    oldest = min(registry$created_at, na.rm = TRUE),
    newest = max(registry$created_at, na.rm = TRUE)
  )
}
