# Tests for R/dag_cache.R — DAG caching layer
# Uses in-memory DuckDB to test registry operations without external dependencies.

test_that("cache_registry_qname returns qualified name", {
  result <- CDMConnector:::cache_registry_qname("results")
  expect_equal(result, "results.dag_cache_registry")
})

test_that("cache_registry_ddl returns valid SQL", {
  ddl <- CDMConnector:::cache_registry_ddl("main")
  expect_type(ddl, "character")
  expect_true(any(grepl("CREATE TABLE", ddl)))
  expect_true(any(grepl("node_hash", ddl)))
  expect_true(any(grepl("node_type", ddl)))
  expect_true(any(grepl("table_name", ddl)))
  expect_true(any(grepl("PRIMARY KEY", ddl)))
})

test_that("ensure_cache_registry creates table in DuckDB", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::ensure_cache_registry(con, "main")
  expect_true(result)

  # Table should exist
  tables <- DBI::dbListTables(con)
  expect_true("dag_cache_registry" %in% tables)

  # Calling again should not error (idempotent)

  result2 <- CDMConnector:::ensure_cache_registry(con, "main")
  expect_true(result2)
})

test_that("cache_lookup returns empty for no hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::cache_lookup(con, "main", character(0))
  expect_length(result, 0)
  expect_type(result, "logical")
})

test_that("cache_lookup returns FALSE for missing hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  result <- CDMConnector:::cache_lookup(con, "main", c("abc123", "def456"))
  expect_length(result, 2)
  expect_false(result[["abc123"]])
  expect_false(result[["def456"]])
})

test_that("cache_register and cache_lookup round-trip", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Register a node
  CDMConnector:::cache_register(con, "main", "hash001", "primary_events",
                                 "main.dagcache_pe_hash001", cohort_ids = c(1L, 2L))

  # Lookup should find it
  result <- CDMConnector:::cache_lookup(con, "main", c("hash001", "hash999"))
  expect_true(result[["hash001"]])
  expect_false(result[["hash999"]])
})

test_that("cache_register handles duplicate insert (upsert)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # First insert
  CDMConnector:::cache_register(con, "main", "hashdup", "concept_set",
                                 "main.dagcache_cs_hashdup", cohort_ids = 1L)

  # Second insert with same hash should not error
  expect_no_error(
    CDMConnector:::cache_register(con, "main", "hashdup", "concept_set",
                                   "main.dagcache_cs_hashdup", cohort_ids = c(1L, 2L))
  )

  # Should still be found
  result <- CDMConnector:::cache_lookup(con, "main", "hashdup")
  expect_true(result[["hashdup"]])
})

test_that("cache_validate returns FALSE when table doesn't exist", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Register a node pointing to a non-existent table
  CDMConnector:::cache_register(con, "main", "hashval1", "primary_events",
                                 "main.dagcache_pe_hashval1")

  result <- CDMConnector:::cache_validate(con, "main", "hashval1")
  expect_false(result[["hashval1"]])
})

test_that("cache_validate returns TRUE when table exists", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Create an actual table
  DBI::dbExecute(con, "CREATE TABLE dagcache_pe_hashval2 (id INTEGER)")

  # Register it
  CDMConnector:::cache_register(con, "main", "hashval2", "primary_events",
                                 "main.dagcache_pe_hashval2")

  result <- CDMConnector:::cache_validate(con, "main", "hashval2")
  expect_true(result[["hashval2"]])
})

test_that("cache_validate returns empty for no hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::cache_validate(con, "main", character(0))
  expect_length(result, 0)
})

test_that("cache_touch does not error on empty hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_no_error(CDMConnector:::cache_touch(con, "main", character(0)))
})

test_that("cache_touch updates last_used_at for existing hashes", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "touchhash", "primary_events",
                                 "main.dagcache_pe_touchhash")

  expect_no_error(CDMConnector:::cache_touch(con, "main", "touchhash"))
})

test_that("cache_table_name returns qualified cached table name", {
  node <- list(type = "primary_events", id = "someid", temp_table = "pe_abc123")
  result <- CDMConnector:::cache_table_name(node, "results")
  expect_equal(result, "results.dagcache_pe_abc123")
})

test_that("dag_cache_list returns empty data frame when no entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- CDMConnector:::dag_cache_list(con, "main")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})

test_that("dag_cache_list returns registered entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "listhash1", "concept_set",
                                 "main.dagcache_cs_listhash1", cohort_ids = 1L)
  CDMConnector:::cache_register(con, "main", "listhash2", "primary_events",
                                 "main.dagcache_pe_listhash2", cohort_ids = 2L)

  result <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(result), 2)
  expect_true("listhash1" %in% result$node_hash)
  expect_true("listhash2" %in% result$node_hash)
})

test_that("dag_cache_stats returns correct stats", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Empty cache
  CDMConnector:::ensure_cache_registry(con, "main")
  stats <- CDMConnector:::dag_cache_stats(con, "main")
  expect_equal(stats$total_entries, 0L)
  expect_length(stats$by_type, 0)

  # Add entries
  CDMConnector:::cache_register(con, "main", "stathash1", "concept_set",
                                 "main.dagcache_cs_stathash1")
  CDMConnector:::cache_register(con, "main", "stathash2", "concept_set",
                                 "main.dagcache_cs_stathash2")
  CDMConnector:::cache_register(con, "main", "stathash3", "primary_events",
                                 "main.dagcache_pe_stathash3")

  stats <- CDMConnector:::dag_cache_stats(con, "main")
  expect_equal(stats$total_entries, 3)
  expect_true("concept_set" %in% names(stats$by_type))
  expect_true("primary_events" %in% names(stats$by_type))
  expect_equal(stats$by_type[["concept_set"]], 2L)
  expect_equal(stats$by_type[["primary_events"]], 1L)
})

test_that("dag_cache_gc handles empty cache", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  expect_message(
    result <- CDMConnector:::dag_cache_gc(con, "main"),
    "Cache is empty"
  )
  expect_equal(nrow(result), 0)
})

test_that("dag_cache_gc dry_run reports without deleting", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  CDMConnector:::cache_register(con, "main", "gchash1", "primary_events",
                                 "main.dagcache_pe_gchash1")

  # dry_run with max_age_days = 0 should report all as stale
  expect_message(
    result <- CDMConnector:::dag_cache_gc(con, "main", max_age_days = 0, dry_run = TRUE),
    "Would remove"
  )
  expect_equal(nrow(result), 1)

  # Entry should still be in the cache
  still_there <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(still_there), 1)
})

test_that("dag_cache_gc removes orphaned entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Register a node whose table doesn't exist (orphaned)
  CDMConnector:::cache_register(con, "main", "orphan1", "primary_events",
                                 "main.dagcache_pe_orphan1")

  # Use very large max_age_days so age doesn't trigger, but orphaned status does
  expect_message(
    result <- CDMConnector:::dag_cache_gc(con, "main", max_age_days = 99999),
    "Removed"
  )
  expect_equal(nrow(result), 1)

  # Cache should now be empty
  after <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(after), 0)
})

test_that("dag_cache_gc with max_age_days = 0 clears all entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  DBI::dbExecute(con, "CREATE TABLE dagcache_pe_gc2 (id INTEGER)")
  CDMConnector:::cache_register(con, "main", "gchash2", "primary_events",
                                 "main.dagcache_pe_gc2")

  expect_message(
    result <- CDMConnector:::dag_cache_gc(con, "main", max_age_days = 0),
    "Removed"
  )

  # Cache should be empty
  after <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(after), 0)
})

test_that("dag_cache_clear drops all entries and tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Create tables and register them
  DBI::dbExecute(con, "CREATE TABLE dagcache_pe_clear1 (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE dagcache_cs_clear2 (id INTEGER)")
  CDMConnector:::cache_register(con, "main", "clearhash1", "primary_events",
                                 "main.dagcache_pe_clear1")
  CDMConnector:::cache_register(con, "main", "clearhash2", "concept_set",
                                 "main.dagcache_cs_clear2")

  expect_message(
    CDMConnector:::dag_cache_clear(con, "main"),
    "Cleared 2 cache entries"
  )

  # Registry should be empty
  after <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(after), 0)

  # Backing tables should be dropped
  tables <- DBI::dbListTables(con)
  expect_false("dagcache_pe_clear1" %in% tables)
  expect_false("dagcache_cs_clear2" %in% tables)
})

test_that("dag_cache_clear handles empty cache", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")
  expect_message(
    CDMConnector:::dag_cache_clear(con, "main"),
    "Cleared 0 cache entries"
  )
})

test_that("cache_register_new_nodes skips concept_set and final_cohort types", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  CDMConnector:::ensure_cache_registry(con, "main")

  # Build a mock DAG with different node types
  dag <- list(
    nodes = list(
      "hash_cs" = list(type = "concept_set", id = "hash_cs", temp_table = "cs_test",
                        cohort_ids = 1L),
      "hash_pe" = list(type = "primary_events", id = "hash_pe", temp_table = "pe_test",
                        cohort_ids = 1L),
      "hash_fc" = list(type = "final_cohort", id = "hash_fc", temp_table = "fc_test",
                        cohort_ids = 1L)
    )
  )

  options <- list(table_prefix = CDMConnector:::CACHE_TABLE_PREFIX)
  cache_misses <- c("hash_cs", "hash_pe", "hash_fc")

  CDMConnector:::cache_register_new_nodes(dag, options, con, "main", cache_misses)

  # Only primary_events should be registered (concept_set and final_cohort are skipped)
  registry <- CDMConnector:::dag_cache_list(con, "main")
  expect_equal(nrow(registry), 1)
  expect_equal(registry$node_type, "primary_events")
})
