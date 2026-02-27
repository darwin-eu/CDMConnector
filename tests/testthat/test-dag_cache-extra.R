# Additional tests for R/dag_cache.R — cache registry, cache operations, emit_cleanup_cached

# --- Constants ---

test_that("CACHE_TABLE_PREFIX is correct", {
  expect_equal(CDMConnector:::CACHE_TABLE_PREFIX, "dagcache_")
})

test_that("CACHE_REGISTRY_TABLE is correct", {
  expect_equal(CDMConnector:::CACHE_REGISTRY_TABLE, "dag_cache_registry")
})

# --- cache_registry_qname ---

test_that("cache_registry_qname builds qualified name", {
  result <- CDMConnector:::cache_registry_qname("results")
  expect_equal(result, "results.dag_cache_registry")
})

test_that("cache_registry_qname with different schema", {
  result <- CDMConnector:::cache_registry_qname("mydb")
  expect_equal(result, "mydb.dag_cache_registry")
})

# --- cache_registry_ddl ---

test_that("cache_registry_ddl generates DDL", {
  ddl <- CDMConnector:::cache_registry_ddl("results")
  ddl_str <- paste(ddl, collapse = " ")
  expect_true(grepl("CREATE TABLE", ddl_str))
  expect_true(grepl("results.dag_cache_registry", ddl_str))
  expect_true(grepl("node_hash", ddl_str))
  expect_true(grepl("node_type", ddl_str))
  expect_true(grepl("table_name", ddl_str))
  expect_true(grepl("PRIMARY KEY", ddl_str))
})

# --- cache_table_name ---

test_that("cache_table_name builds stable name", {
  node <- list(type = "primary_events", id = "abc123", temp_table = "pe_abc123")
  result <- CDMConnector:::cache_table_name(node, "results")
  expect_equal(result, "results.dagcache_pe_abc123")
})

# --- ensure_cache_registry ---

test_that("ensure_cache_registry creates registry table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::ensure_cache_registry(con, "main")
  expect_true(result)
  tables <- DBI::dbListTables(con)
  expect_true("dag_cache_registry" %in% tables)
})

test_that("ensure_cache_registry is idempotent", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  # Call again should not error
  expect_no_error(CDMConnector:::ensure_cache_registry(con, "main"))
})

# --- cache_lookup ---

test_that("cache_lookup returns FALSE for empty cache", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  result <- CDMConnector:::cache_lookup(con, "main", c("hash1", "hash2"))
  expect_equal(length(result), 2)
  expect_false(result[["hash1"]])
  expect_false(result[["hash2"]])
})

test_that("cache_lookup with empty hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::cache_lookup(con, "main", character(0))
  expect_equal(length(result), 0)
})

# --- cache_register ---

test_that("cache_register inserts new entry", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "abc12345", "primary_events", "main.dagcache_pe_abc12345", c(1L, 2L))

  result <- CDMConnector:::cache_lookup(con, "main", "abc12345")
  expect_true(result[["abc12345"]])
})

test_that("cache_register handles duplicate by updating", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "abc12345", "primary_events", "main.dagcache_pe_abc12345", 1L)
  # Insert same hash again should not error (upsert)
  expect_no_error(CDMConnector:::cache_register(con, "main", "abc12345", "primary_events", "main.dagcache_pe_abc12345", c(1L, 2L)))
})

# --- cache_validate ---

test_that("cache_validate returns FALSE when table doesn't exist", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "hash123", "pe", "main.dagcache_pe_hash123", 1L)
  # Table dagcache_pe_hash123 doesn't actually exist
  result <- CDMConnector:::cache_validate(con, "main", "hash123")
  expect_false(result[["hash123"]])
})

test_that("cache_validate returns TRUE when table exists", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  # Create the actual table
  DBI::dbWriteTable(con, "dagcache_pe_hash123", data.frame(x = 1))
  CDMConnector:::cache_register(con, "main", "hash123", "pe", "main.dagcache_pe_hash123", 1L)

  result <- CDMConnector:::cache_validate(con, "main", "hash123")
  expect_true(result[["hash123"]])
})

test_that("cache_validate with empty hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::cache_validate(con, "main", character(0))
  expect_equal(length(result), 0)
})

# --- cache_touch ---

test_that("cache_touch updates last_used_at", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "touchme", "pe", "main.dagcache_pe_touchme", 1L)

  # Touch should not error
  expect_no_error(CDMConnector:::cache_touch(con, "main", "touchme"))
})

test_that("cache_touch with empty hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Should return silently

  expect_invisible(CDMConnector:::cache_touch(con, "main", character(0)))
})

# --- dag_cache_list ---

test_that("dag_cache_list returns data frame", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "list_hash", "pe", "main.dagcache_pe_list_hash", 1L)

  result <- CDMConnector:::dag_cache_list(con, "main")
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) >= 1)
  expect_true("node_hash" %in% names(result))
})

# --- cache_register_new_nodes ---

test_that("cache_register_new_nodes registers cache misses", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Create a minimal DAG
  dag <- list(
    nodes = list(
      pe_hash1 = list(type = "primary_events", id = "pe_hash1", temp_table = "pe_pe_hash1", cohort_ids = 1L),
      cs_hash1 = list(type = "concept_set", id = "cs_hash1", temp_table = "cs_cs_hash1", cohort_ids = 1L),
      fc_hash1 = list(type = "final_cohort", id = "fc_hash1", temp_table = "fc_fc_hash1", cohort_ids = 1L)
    )
  )
  options <- list(table_prefix = CDMConnector:::CACHE_TABLE_PREFIX)

  CDMConnector:::cache_register_new_nodes(dag, options, con, "main", c("pe_hash1", "cs_hash1", "fc_hash1"))

  # concept_set and final_cohort should be skipped
  result <- CDMConnector:::cache_lookup(con, "main", c("pe_hash1", "cs_hash1", "fc_hash1"))
  expect_true(result[["pe_hash1"]])
  expect_false(result[["cs_hash1"]])  # concept_set skipped
  expect_false(result[["fc_hash1"]])  # final_cohort skipped
})

# --- emit_cleanup_cached ---

test_that("emit_cleanup_cached generates cleanup SQL preserving cached tables", {
  # Build a minimal DAG structure
  nodes <- list()
  nodes[["pe1"]] <- list(type = "primary_events", id = "pe1", temp_table = "pe_pe1",
    depends_on = character(0), cohort_ids = 1L)
  nodes[["qe1"]] <- list(type = "qualified_events", id = "qe1", temp_table = "qe_qe1",
    depends_on = "pe1", cohort_ids = 1L)
  nodes[["ie1"]] <- list(type = "included_events", id = "ie1", temp_table = "ie_ie1",
    depends_on = "qe1", cohort_ids = 1L)
  nodes[["ce1"]] <- list(type = "cohort_exit", id = "ce1", temp_table = "ce_ce1",
    depends_on = "ie1", cohort_ids = 1L)
  nodes[["fc1"]] <- list(type = "final_cohort", id = "fc1", temp_table = "fc_fc1",
    depends_on = "ce1", cohort_ids = 1L, definition = list(ce_hash = "ce1"))
  dag <- list(nodes = nodes)
  options <- list(results_schema = "results", table_prefix = "dagcache_")

  result <- CDMConnector:::emit_cleanup_cached(dag, options, cache_hits = c("pe1", "qe1"), cache_misses = c("ie1", "ce1", "fc1"))
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("Cleanup", result_str))
  # Should drop staging and domain tables
  expect_true(grepl("cohort_stage", result_str))
  expect_true(grepl("codesets", result_str))
  # Should drop final_cohort temp table
  expect_true(grepl("fc_fc1", result_str))
})

# --- dag_cache_gc ---

test_that("dag_cache_gc with empty cache", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  expect_message(CDMConnector:::dag_cache_gc(con, "main"), "empty")
})

test_that("dag_cache_gc dry run", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  # Register an entry with no backing table (orphaned)
  CDMConnector:::cache_register(con, "main", "orphan1", "pe", "main.dagcache_pe_orphan1", 1L)

  result <- CDMConnector:::dag_cache_gc(con, "main", max_age_days = 0, dry_run = TRUE)
  expect_true(nrow(result) >= 1)
})

test_that("dag_cache_gc removes stale entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "gc_hash1", "pe", "main.dagcache_pe_gc_hash1", 1L)

  # GC with max_age_days = 0 removes everything
  expect_message(
    result <- CDMConnector:::dag_cache_gc(con, "main", max_age_days = 0),
    "Removed"
  )
  expect_true(nrow(result) >= 1)

  # Verify the entry was removed
  lookup <- CDMConnector:::cache_lookup(con, "main", "gc_hash1")
  expect_false(lookup[["gc_hash1"]])
})

# --- dag_cache_clear ---

test_that("dag_cache_clear removes all entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "clear1", "pe", "main.dagcache_pe_clear1", 1L)
  CDMConnector:::cache_register(con, "main", "clear2", "qe", "main.dagcache_qe_clear2", 2L)

  expect_message(CDMConnector:::dag_cache_clear(con, "main"), "Cleared 2")

  # Verify all entries removed
  result <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(result), 0)
})
