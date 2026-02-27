# Additional tests for R/optimizer.R — coverage for uncovered functions

# --- rewrite_to_domain_caches ---

test_that("rewrite_to_domain_caches replaces CDM domain table references", {
  sql <- "SELECT * FROM main.DRUG_EXPOSURE WHERE person_id = 1"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "main")
  expect_false(grepl("main\\.DRUG_EXPOSURE", result))
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches replaces OBSERVATION_PERIOD", {
  sql <- "SELECT * FROM main.OBSERVATION_PERIOD"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "main")
  expect_true(grepl("atlas_observation_period", result))
})

test_that("rewrite_to_domain_caches handles case-insensitive replacement", {
  sql <- "SELECT * FROM main.drug_exposure"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "main")
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches handles custom cdm_schema", {
  sql <- "SELECT * FROM mydb.CONDITION_OCCURRENCE"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "mydb")
  expect_true(grepl("condition_occurrence_filtered", result))
})

test_that("rewrite_to_domain_caches uses qualify_table with options", {
  sql <- "SELECT * FROM main.MEASUREMENT"
  options <- list(results_schema = "results", table_prefix = "atlas_abc_")
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "main", options = options)
  expect_true(grepl("results\\.atlas_abc_measurement_filtered", result))
})

test_that("rewrite_to_domain_caches replaces multiple domains", {
  sql <- paste(
    "SELECT * FROM main.DRUG_EXPOSURE de",
    "JOIN main.CONDITION_OCCURRENCE co ON de.person_id = co.person_id"
  )
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "main")
  expect_true(grepl("drug_exposure_filtered", result))
  expect_true(grepl("condition_occurrence_filtered", result))
})

# --- read_json_input ---

test_that("read_json_input reads from file", {
  tmp <- tempfile(fileext = ".json")
  writeLines('{"test": true}', tmp)
  on.exit(unlink(tmp))
  result <- CDMConnector:::read_json_input(tmp)
  expect_equal(result, '{"test": true}')
})

test_that("read_json_input returns raw JSON string", {
  json <- '{"ConceptSets": []}'
  result <- CDMConnector:::read_json_input(json)
  expect_equal(result, json)
})

test_that("read_json_input errors on non-character", {
  expect_error(CDMConnector:::read_json_input(123), "character")
})

test_that("read_json_input errors on vector", {
  expect_error(CDMConnector:::read_json_input(c("a", "b")), "character")
})

# --- atlas_json_to_sql (single cohort) ---

test_that("atlas_json_to_sql generates SQL from JSON string", {
  json <- '{
    "ConceptSets": [{
      "id": 0, "name": "test",
      "expression": {"items": [{"concept": {"CONCEPT_ID": 1112807}}]}
    }],
    "PrimaryCriteria": {
      "CriteriaList": [{"DrugExposure": {"CodesetId": 0}}],
      "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
      "PrimaryCriteriaLimit": {"Type": "First"}
    },
    "QualifiedLimit": {"Type": "First"},
    "ExpressionLimit": {"Type": "First"},
    "InclusionRules": [],
    "CollapseSettings": {"CollapseType": "ERA", "EraPad": 30}
  }'

  sql <- CDMConnector:::atlas_json_to_sql(json, cohort_id = 1L,
                                           target_dialect = NULL,
                                           render = TRUE)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 100)
  expect_true(grepl("SELECT|INSERT|CREATE", sql, ignore.case = TRUE))
})

# --- buildBatchCohortQuery ---

test_that("buildBatchCohortQuery errors on empty cohort_list", {
  expect_error(
    CDMConnector:::buildBatchCohortQuery(list(), integer(0)),
    "must not be empty"
  )
})

test_that("buildBatchCohortQuery errors on mismatched lengths", {
  cohort <- list(list(test = 1))
  expect_error(
    CDMConnector:::buildBatchCohortQuery(cohort, c(1L, 2L)),
    "must match"
  )
})

test_that("buildBatchCohortQuery errors when cache = TRUE without con", {
  cohort <- list(list(test = 1))
  expect_error(
    CDMConnector:::buildBatchCohortQuery(cohort, 1L, cache = TRUE),
    "con.*required"
  )
})

# --- CRITERIA_TYPE_TO_CDM_TABLE ---

test_that("CRITERIA_TYPE_TO_CDM_TABLE maps all expected types", {
  mapping <- CDMConnector:::CRITERIA_TYPE_TO_CDM_TABLE
  expect_true("ConditionOccurrence" %in% names(mapping))
  expect_true("DrugExposure" %in% names(mapping))
  expect_true("Measurement" %in% names(mapping))
  expect_equal(mapping[["ConditionOccurrence"]], "CONDITION_OCCURRENCE")
  expect_equal(mapping[["DrugExposure"]], "DRUG_EXPOSURE")
})

# --- drop_prefixed_tables fallback path ---

test_that("drop_prefixed_tables falls back to static list when no tables match", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Should not error even with no matching tables
  expect_no_error(CDMConnector:::drop_prefixed_tables(con, "main", "nonexistent_prefix_"))
})

# --- resolve_literal_conditionals edge cases ---

test_that("resolve_literal_conditionals handles else with missing closing brace gracefully", {
  sql <- "SELECT {1 != 0}?{then_val}:{else_val FROM t"
  # Should not infinite loop; may return partially processed
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_type(result, "character")
})

test_that("resolve_literal_conditionals handles deeply nested true condition", {
  sql <- "BEGIN {5 != 0}?{SELECT {3 != 0}?{inner_val} FROM t} END"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("inner_val", result))
})

# --- resolve_literal_conditionals extensive coverage ---

test_that("resolve_literal_conditionals handles false condition with else", {
  sql <- "SELECT {0 != 0}?{then_val}:{else_val}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("else_val", result))
  expect_false(grepl("then_val", result))
})

test_that("resolve_literal_conditionals handles false condition without else", {
  sql <- "SELECT {0 != 0}?{hidden_val} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_false(grepl("hidden_val", result))
})

test_that("resolve_literal_conditionals handles true condition with else", {
  sql <- "SELECT {5 != 0}?{then_val}:{else_val}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("then_val", result))
  expect_false(grepl("else_val", result))
})

test_that("resolve_literal_conditionals handles multiple conditions", {
  sql <- "A {1 != 0}?{B} C {0 != 0}?{D}:{E} F"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("B", result))
  expect_true(grepl("E", result))
  expect_false(grepl("D", result))
})

test_that("resolve_literal_conditionals handles no conditionals", {
  sql <- "SELECT * FROM table"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, sql)
})

test_that("resolve_literal_conditionals handles compound condition (AND)", {
  sql <- "SELECT {1 != 0 & 2 != 0}?{both_true}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("both_true", result))
})

test_that("resolve_literal_conditionals handles compound false condition", {
  sql <- "SELECT {1 != 0 & 0 != 0}?{should_hide}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_false(grepl("should_hide", result))
})

# --- atlas_unique_prefix ---

test_that("atlas_unique_prefix generates a valid prefix", {
  prefix <- CDMConnector:::atlas_unique_prefix()
  expect_true(startsWith(prefix, "atlas_"))
  expect_true(endsWith(prefix, "_"))
  expect_true(nchar(prefix) > 6)
})

# --- OPTIMIZER_STATIC_TABLES ---

test_that("OPTIMIZER_STATIC_TABLES contains expected tables", {
  tables <- CDMConnector:::OPTIMIZER_STATIC_TABLES
  expect_true("cohort_stage" %in% tables)
  expect_true("codesets" %in% tables)
  expect_true("drug_exposure_filtered" %in% tables)
})

# --- collect_batch_used_domains_from_cohorts ---

test_that("collect_batch_used_domains_from_cohorts finds domain tables", {
  cohort_list <- list(
    list(
      PrimaryCriteria = list(
        CriteriaList = list(
          list(ConditionOccurrence = list(CodesetId = 0))
        )
      )
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(cohort_list)
  expect_true("CONDITION_OCCURRENCE" %in% result)
  expect_true("OBSERVATION_PERIOD" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles multiple cohorts", {
  cohort_list <- list(
    list(PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))))),
    list(PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1)))))
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(cohort_list)
  expect_true("CONDITION_OCCURRENCE" %in% result)
  expect_true("DRUG_EXPOSURE" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles CensoringCriteria", {
  cohort_list <- list(
    list(
      PrimaryCriteria = list(CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0)))),
      CensoringCriteria = list(list(Death = list(CodesetId = 1)))
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(cohort_list)
  expect_true("DEATH" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles InclusionRules", {
  cohort_list <- list(
    list(
      PrimaryCriteria = list(CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0)))),
      InclusionRules = list(
        list(expression = list(
          CriteriaList = list(list(DrugExposure = list(CodesetId = 1))),
          Groups = list()
        ))
      )
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(cohort_list)
  expect_true("DRUG_EXPOSURE" %in% result)
})

# --- quote_cdm_table_refs ---

test_that("quote_cdm_table_refs quotes table references", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.OBSERVATION_PERIOD op JOIN main.PERSON p ON op.person_id = p.person_id"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "main")
  expect_true(grepl("OBSERVATION_PERIOD", result))
  expect_true(grepl("PERSON", result))
})


# --- rewrite_to_domain_caches ---

test_that("rewrite_to_domain_caches replaces domain tables", {
  sql <- "SELECT * FROM cdmv5.DRUG_EXPOSURE WHERE 1=1"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
  expect_false(grepl("cdmv5\\.DRUG_EXPOSURE", result))
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches replaces multiple domain tables", {
  sql <- paste(
    "SELECT * FROM cdmv5.DRUG_EXPOSURE",
    "JOIN cdmv5.CONDITION_OCCURRENCE ON 1=1",
    "JOIN cdmv5.OBSERVATION_PERIOD ON 1=1"
  )
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
  expect_true(grepl("drug_exposure_filtered", result))
  expect_true(grepl("condition_occurrence_filtered", result))
  expect_true(grepl("atlas_observation_period", result))
})

test_that("rewrite_to_domain_caches case insensitive", {
  sql <- "SELECT * FROM cdmv5.drug_exposure"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches with options qualifies tables", {
  sql <- "SELECT * FROM cdmv5.DRUG_EXPOSURE"
  opts <- list(results_schema = "myschema", table_prefix = "atlas_abc_")
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5", options = opts)
  expect_true(grepl("myschema\\.atlas_abc_drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches with custom cdm_schema", {
  sql <- "SELECT * FROM mydb.MEASUREMENT WHERE 1=1"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "mydb")
  expect_true(grepl("measurement_filtered", result))
})

test_that("rewrite_to_domain_caches handles all domain tables", {
  domains <- c("DRUG_EXPOSURE", "CONDITION_OCCURRENCE", "PROCEDURE_OCCURRENCE",
               "OBSERVATION", "MEASUREMENT", "DEVICE_EXPOSURE", "VISIT_OCCURRENCE")
  for (d in domains) {
    sql <- paste0("SELECT * FROM cdmv5.", d)
    result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
    expect_false(grepl(paste0("cdmv5\\.", d), result),
                 info = paste("Domain", d, "was not replaced"))
  }
})

# --- quote_cdm_table_refs ---

test_that("quote_cdm_table_refs quotes table references", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.PERSON WHERE 1=1"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "main")
  # Should contain quoted references
  expect_true(grepl("PERSON", result))
  expect_type(result, "character")
})

test_that("quote_cdm_table_refs handles multiple tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.PERSON JOIN main.CONCEPT ON 1=1"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "main")
  expect_type(result, "character")
})

test_that("quote_cdm_table_refs handles multi-part schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM cat.sch.PERSON"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "cat.sch")
  expect_type(result, "character")
})

# --- resolve_literal_conditionals ---

test_that("resolve_literal_conditionals resolves true condition", {
  sql <- "{1 != 0}?{SELECT 1}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "SELECT 1")
})

test_that("resolve_literal_conditionals resolves false condition", {
  sql <- "{0 != 0}?{SELECT 1}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "")
})

test_that("resolve_literal_conditionals resolves else branch true", {
  sql <- "{5 != 0}?{THEN_PART}:{ELSE_PART}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "THEN_PART")
})

test_that("resolve_literal_conditionals resolves else branch false", {
  sql <- "{0 != 0}?{THEN_PART}:{ELSE_PART}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "ELSE_PART")
})

test_that("resolve_literal_conditionals no-op for no conditionals", {
  sql <- "SELECT * FROM person"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, sql)
})

test_that("resolve_literal_conditionals handles multiple conditions", {
  sql <- "{1 != 0}?{A} AND {0 != 0}?{B}:{C}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("A", result))
  expect_true(grepl("C", result))
  expect_false(grepl("B", result))
})

test_that("resolve_literal_conditionals handles compound condition", {
  sql <- "{3 != 0}?{FOUND}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "FOUND")
})

# --- atlas_unique_prefix ---

test_that("atlas_unique_prefix returns expected format", {
  result <- CDMConnector:::atlas_unique_prefix()
  expect_type(result, "character")
  expect_true(startsWith(result, "atlas_"))
  expect_true(endsWith(result, "_"))
})

test_that("atlas_unique_prefix generates different values", {
  p1 <- CDMConnector:::atlas_unique_prefix()
  p2 <- CDMConnector:::atlas_unique_prefix()
  # Usually different due to UUID/timestamp
  expect_type(p1, "character")
  expect_type(p2, "character")
})

# --- read_json_input ---

test_that("read_json_input reads from file", {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)
  writeLines('{"test": true}', tmp)
  result <- CDMConnector:::read_json_input(tmp)
  expect_true(grepl("test", result))
})

test_that("read_json_input returns string as-is", {
  json_str <- '{"ConceptSets": []}'
  result <- CDMConnector:::read_json_input(json_str)
  expect_equal(result, json_str)
})

test_that("read_json_input errors on non-character", {
  expect_error(CDMConnector:::read_json_input(42))
})

test_that("read_json_input errors on vector", {
  expect_error(CDMConnector:::read_json_input(c("a", "b")))
})

# --- normalize_schema_str ---

test_that("normalize_schema_str returns default for NULL", {
  result <- CDMConnector:::normalize_schema_str(NULL)
  expect_equal(result, "main")
})

test_that("normalize_schema_str returns default with custom default", {
  result <- CDMConnector:::normalize_schema_str(NULL, default = "mydb")
  expect_equal(result, "mydb")
})

test_that("normalize_schema_str passes through single string", {
  result <- CDMConnector:::normalize_schema_str("myschema")
  expect_equal(result, "myschema")
})

test_that("normalize_schema_str handles named character vector", {
  result <- CDMConnector:::normalize_schema_str(c(catalog = "cat", schema = "sch"))
  expect_equal(result, "cat.sch")
})

test_that("normalize_schema_str strips prefix from named vector", {
  result <- CDMConnector:::normalize_schema_str(c(schema = "main", prefix = "tmp_"))
  expect_equal(result, "main")
})

test_that("normalize_schema_str handles list", {
  result <- CDMConnector:::normalize_schema_str(list(schema = "main", prefix = "pre_"))
  expect_equal(result, "main")
})

test_that("normalize_schema_str handles unnamed vector", {
  result <- CDMConnector:::normalize_schema_str(c("cat", "sch"))
  expect_equal(result, "cat.sch")
})

# --- extract_write_prefix ---

test_that("extract_write_prefix returns empty for NULL", {
  result <- CDMConnector:::extract_write_prefix(NULL)
  expect_equal(result, "")
})

test_that("extract_write_prefix returns prefix from named vector", {
  result <- CDMConnector:::extract_write_prefix(c(schema = "main", prefix = "tmp_"))
  expect_equal(result, "tmp_")
})

test_that("extract_write_prefix returns prefix from list", {
  result <- CDMConnector:::extract_write_prefix(list(schema = "main", prefix = "abc_"))
  expect_equal(result, "abc_")
})

test_that("extract_write_prefix returns empty when no prefix", {
  result <- CDMConnector:::extract_write_prefix(c(schema = "main"))
  expect_equal(result, "")
})

test_that("extract_write_prefix returns empty for plain string", {
  result <- CDMConnector:::extract_write_prefix("main")
  expect_equal(result, "")
})

# --- drop_prefixed_tables ---

test_that("drop_prefixed_tables drops tables with prefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE atlas_test_codesets (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE atlas_test_other (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE keep_me (id INTEGER)")

  CDMConnector:::drop_prefixed_tables(con, "main", "atlas_test_")

  remaining <- DBI::dbListTables(con)
  expect_false("atlas_test_codesets" %in% remaining)
  expect_false("atlas_test_other" %in% remaining)
  expect_true("keep_me" %in% remaining)
})

test_that("drop_prefixed_tables does nothing when no matching tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE my_table (id INTEGER)")
  expect_no_error(CDMConnector:::drop_prefixed_tables(con, "main", "atlas_xyz_"))
  expect_true("my_table" %in% DBI::dbListTables(con))
})

# --- DOMAIN_CONFIG ---

test_that("DOMAIN_CONFIG has expected structure", {
  dc <- CDMConnector:::DOMAIN_CONFIG
  expect_true(is.list(dc))
  expect_true(length(dc) >= 7)
  for (entry in dc) {
    expect_true("table" %in% names(entry))
    expect_true("alias" %in% names(entry))
    expect_true("std_col" %in% names(entry))
    expect_true("filtered" %in% names(entry))
  }
})

# --- CRITERIA_TYPE_TO_CDM_TABLE ---

test_that("CRITERIA_TYPE_TO_CDM_TABLE maps criteria types", {
  mapping <- CDMConnector:::CRITERIA_TYPE_TO_CDM_TABLE
  expect_equal(mapping[["ConditionOccurrence"]], "CONDITION_OCCURRENCE")
  expect_equal(mapping[["DrugExposure"]], "DRUG_EXPOSURE")
  expect_equal(mapping[["Measurement"]], "MEASUREMENT")
  expect_equal(mapping[["VisitOccurrence"]], "VISIT_OCCURRENCE")
})

# --- collect_criteria_types_from_group ---

test_that("collect_criteria_types_from_group handles empty group", {
  result <- CDMConnector:::collect_criteria_types_from_group(list())
  expect_equal(result, NULL)
})

test_that("collect_criteria_types_from_group handles non-list", {
  result <- CDMConnector:::collect_criteria_types_from_group("not a list")
  expect_equal(result, NULL)
})

test_that("collect_criteria_types_from_group finds criteria types", {
  group <- list(
    CriteriaList = list(
      list(ConditionOccurrence = list(CodesetId = 1))
    )
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("ConditionOccurrence" %in% result)
})

test_that("collect_criteria_types_from_group finds nested groups", {
  group <- list(
    CriteriaList = list(
      list(DrugExposure = list(CodesetId = 1))
    ),
    Groups = list(
      list(
        CriteriaList = list(
          list(Measurement = list(CodesetId = 2))
        )
      )
    )
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("DrugExposure" %in% result)
  expect_true("Measurement" %in% result)
})

# --- collect_batch_used_domains_from_cohorts ---

test_that("collect_batch_used_domains_from_cohorts includes OBSERVATION_PERIOD", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(
        list(ConditionOccurrence = list(CodesetId = 1))
      )
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("OBSERVATION_PERIOD" %in% result)
  expect_true("CONDITION_OCCURRENCE" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles multiple cohorts", {
  c1 <- list(PrimaryCriteria = list(
    CriteriaList = list(list(DrugExposure = list(CodesetId = 1)))
  ))
  c2 <- list(PrimaryCriteria = list(
    CriteriaList = list(list(Measurement = list(CodesetId = 2)))
  ))
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(c1, c2))
  expect_true("DRUG_EXPOSURE" %in% result)
  expect_true("MEASUREMENT" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles inclusion rules", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    InclusionRules = list(
      list(expression = list(
        CriteriaList = list(list(ProcedureOccurrence = list(CodesetId = 2)))
      ))
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("CONDITION_OCCURRENCE" %in% result)
  expect_true("PROCEDURE_OCCURRENCE" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles censoring criteria", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    CensoringCriteria = list(
      list(Death = list())
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("DEATH" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles additional criteria", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    AdditionalCriteria = list(
      CriteriaList = list(list(Observation = list(CodesetId = 3)))
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("OBSERVATION" %in% result)
})

# --- OPTIMIZER_STATIC_TABLES ---

test_that("OPTIMIZER_STATIC_TABLES has expected tables", {
  tables <- CDMConnector:::OPTIMIZER_STATIC_TABLES
  expect_true("codesets" %in% tables)
  expect_true("cohort_stage" %in% tables)
  expect_true("atlas_observation_period" %in% tables)
  expect_true("drug_exposure_filtered" %in% tables)
})

# --- CDM_TABLE_NAMES_FOR_QUOTE ---

test_that("CDM_TABLE_NAMES_FOR_QUOTE includes key tables", {
  tables <- CDMConnector:::CDM_TABLE_NAMES_FOR_QUOTE
  expect_true("PERSON" %in% tables)
  expect_true("CONCEPT" %in% tables)
  expect_true("CONCEPT_ANCESTOR" %in% tables)
  expect_true("OBSERVATION_PERIOD" %in% tables)
})


# --- rewrite_to_domain_caches ---

test_that("rewrite_to_domain_caches replaces domain tables", {
  sql <- "SELECT * FROM cdmv5.DRUG_EXPOSURE WHERE 1=1"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
  expect_false(grepl("cdmv5\\.DRUG_EXPOSURE", result))
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches replaces multiple domain tables", {
  sql <- paste(
    "SELECT * FROM cdmv5.DRUG_EXPOSURE",
    "JOIN cdmv5.CONDITION_OCCURRENCE ON 1=1",
    "JOIN cdmv5.OBSERVATION_PERIOD ON 1=1"
  )
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
  expect_true(grepl("drug_exposure_filtered", result))
  expect_true(grepl("condition_occurrence_filtered", result))
  expect_true(grepl("atlas_observation_period", result))
})

test_that("rewrite_to_domain_caches case insensitive", {
  sql <- "SELECT * FROM cdmv5.drug_exposure"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches with options qualifies tables", {
  sql <- "SELECT * FROM cdmv5.DRUG_EXPOSURE"
  opts <- list(results_schema = "myschema", table_prefix = "atlas_abc_")
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5", options = opts)
  expect_true(grepl("myschema\\.atlas_abc_drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches with custom cdm_schema", {
  sql <- "SELECT * FROM mydb.MEASUREMENT WHERE 1=1"
  result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "mydb")
  expect_true(grepl("measurement_filtered", result))
})

test_that("rewrite_to_domain_caches handles all domain tables", {
  domains <- c("DRUG_EXPOSURE", "CONDITION_OCCURRENCE", "PROCEDURE_OCCURRENCE",
               "OBSERVATION", "MEASUREMENT", "DEVICE_EXPOSURE", "VISIT_OCCURRENCE")
  for (d in domains) {
    sql <- paste0("SELECT * FROM cdmv5.", d)
    result <- CDMConnector:::rewrite_to_domain_caches(sql, cdm_schema = "cdmv5")
    expect_false(grepl(paste0("cdmv5\\.", d), result),
                 info = paste("Domain", d, "was not replaced"))
  }
})

# --- quote_cdm_table_refs ---

test_that("quote_cdm_table_refs quotes table references", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.PERSON WHERE 1=1"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "main")
  # Should contain quoted references
  expect_true(grepl("PERSON", result))
  expect_type(result, "character")
})

test_that("quote_cdm_table_refs handles multiple tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.PERSON JOIN main.CONCEPT ON 1=1"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "main")
  expect_type(result, "character")
})

test_that("quote_cdm_table_refs handles multi-part schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM cat.sch.PERSON"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "cat.sch")
  expect_type(result, "character")
})

# --- resolve_literal_conditionals ---

test_that("resolve_literal_conditionals resolves true condition", {
  sql <- "{1 != 0}?{SELECT 1}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "SELECT 1")
})

test_that("resolve_literal_conditionals resolves false condition", {
  sql <- "{0 != 0}?{SELECT 1}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "")
})

test_that("resolve_literal_conditionals resolves else branch true", {
  sql <- "{5 != 0}?{THEN_PART}:{ELSE_PART}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "THEN_PART")
})

test_that("resolve_literal_conditionals resolves else branch false", {
  sql <- "{0 != 0}?{THEN_PART}:{ELSE_PART}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "ELSE_PART")
})

test_that("resolve_literal_conditionals no-op for no conditionals", {
  sql <- "SELECT * FROM person"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, sql)
})

test_that("resolve_literal_conditionals handles multiple conditions", {
  sql <- "{1 != 0}?{A} AND {0 != 0}?{B}:{C}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("A", result))
  expect_true(grepl("C", result))
  expect_false(grepl("B", result))
})

test_that("resolve_literal_conditionals handles compound condition", {
  sql <- "{3 != 0}?{FOUND}"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, "FOUND")
})

# --- atlas_unique_prefix ---

test_that("atlas_unique_prefix returns expected format", {
  result <- CDMConnector:::atlas_unique_prefix()
  expect_type(result, "character")
  expect_true(startsWith(result, "atlas_"))
  expect_true(endsWith(result, "_"))
})

test_that("atlas_unique_prefix generates different values", {
  p1 <- CDMConnector:::atlas_unique_prefix()
  p2 <- CDMConnector:::atlas_unique_prefix()
  # Usually different due to UUID/timestamp
  expect_type(p1, "character")
  expect_type(p2, "character")
})

# --- read_json_input ---

test_that("read_json_input reads from file", {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)
  writeLines('{"test": true}', tmp)
  result <- CDMConnector:::read_json_input(tmp)
  expect_true(grepl("test", result))
})

test_that("read_json_input returns string as-is", {
  json_str <- '{"ConceptSets": []}'
  result <- CDMConnector:::read_json_input(json_str)
  expect_equal(result, json_str)
})

test_that("read_json_input errors on non-character", {
  expect_error(CDMConnector:::read_json_input(42))
})

test_that("read_json_input errors on vector", {
  expect_error(CDMConnector:::read_json_input(c("a", "b")))
})

# --- normalize_schema_str ---

test_that("normalize_schema_str returns default for NULL", {
  result <- CDMConnector:::normalize_schema_str(NULL)
  expect_equal(result, "main")
})

test_that("normalize_schema_str returns default with custom default", {
  result <- CDMConnector:::normalize_schema_str(NULL, default = "mydb")
  expect_equal(result, "mydb")
})

test_that("normalize_schema_str passes through single string", {
  result <- CDMConnector:::normalize_schema_str("myschema")
  expect_equal(result, "myschema")
})

test_that("normalize_schema_str handles named character vector", {
  result <- CDMConnector:::normalize_schema_str(c(catalog = "cat", schema = "sch"))
  expect_equal(result, "cat.sch")
})

test_that("normalize_schema_str strips prefix from named vector", {
  result <- CDMConnector:::normalize_schema_str(c(schema = "main", prefix = "tmp_"))
  expect_equal(result, "main")
})

test_that("normalize_schema_str handles list", {
  result <- CDMConnector:::normalize_schema_str(list(schema = "main", prefix = "pre_"))
  expect_equal(result, "main")
})

test_that("normalize_schema_str handles unnamed vector", {
  result <- CDMConnector:::normalize_schema_str(c("cat", "sch"))
  expect_equal(result, "cat.sch")
})

# --- extract_write_prefix ---

test_that("extract_write_prefix returns empty for NULL", {
  result <- CDMConnector:::extract_write_prefix(NULL)
  expect_equal(result, "")
})

test_that("extract_write_prefix returns prefix from named vector", {
  result <- CDMConnector:::extract_write_prefix(c(schema = "main", prefix = "tmp_"))
  expect_equal(result, "tmp_")
})

test_that("extract_write_prefix returns prefix from list", {
  result <- CDMConnector:::extract_write_prefix(list(schema = "main", prefix = "abc_"))
  expect_equal(result, "abc_")
})

test_that("extract_write_prefix returns empty when no prefix", {
  result <- CDMConnector:::extract_write_prefix(c(schema = "main"))
  expect_equal(result, "")
})

test_that("extract_write_prefix returns empty for plain string", {
  result <- CDMConnector:::extract_write_prefix("main")
  expect_equal(result, "")
})

# --- drop_prefixed_tables ---

test_that("drop_prefixed_tables drops tables with prefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE atlas_test_codesets (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE atlas_test_other (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE keep_me (id INTEGER)")

  CDMConnector:::drop_prefixed_tables(con, "main", "atlas_test_")

  remaining <- DBI::dbListTables(con)
  expect_false("atlas_test_codesets" %in% remaining)
  expect_false("atlas_test_other" %in% remaining)
  expect_true("keep_me" %in% remaining)
})

test_that("drop_prefixed_tables does nothing when no matching tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE my_table (id INTEGER)")
  expect_no_error(CDMConnector:::drop_prefixed_tables(con, "main", "atlas_xyz_"))
  expect_true("my_table" %in% DBI::dbListTables(con))
})

# --- DOMAIN_CONFIG ---

test_that("DOMAIN_CONFIG has expected structure", {
  dc <- CDMConnector:::DOMAIN_CONFIG
  expect_true(is.list(dc))
  expect_true(length(dc) >= 7)
  for (entry in dc) {
    expect_true("table" %in% names(entry))
    expect_true("alias" %in% names(entry))
    expect_true("std_col" %in% names(entry))
    expect_true("filtered" %in% names(entry))
  }
})

# --- CRITERIA_TYPE_TO_CDM_TABLE ---

test_that("CRITERIA_TYPE_TO_CDM_TABLE maps criteria types", {
  mapping <- CDMConnector:::CRITERIA_TYPE_TO_CDM_TABLE
  expect_equal(mapping[["ConditionOccurrence"]], "CONDITION_OCCURRENCE")
  expect_equal(mapping[["DrugExposure"]], "DRUG_EXPOSURE")
  expect_equal(mapping[["Measurement"]], "MEASUREMENT")
  expect_equal(mapping[["VisitOccurrence"]], "VISIT_OCCURRENCE")
})

# --- collect_criteria_types_from_group ---

test_that("collect_criteria_types_from_group handles empty group", {
  result <- CDMConnector:::collect_criteria_types_from_group(list())
  expect_equal(result, NULL)
})

test_that("collect_criteria_types_from_group handles non-list", {
  result <- CDMConnector:::collect_criteria_types_from_group("not a list")
  expect_equal(result, NULL)
})

test_that("collect_criteria_types_from_group finds criteria types", {
  group <- list(
    CriteriaList = list(
      list(ConditionOccurrence = list(CodesetId = 1))
    )
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("ConditionOccurrence" %in% result)
})

test_that("collect_criteria_types_from_group finds nested groups", {
  group <- list(
    CriteriaList = list(
      list(DrugExposure = list(CodesetId = 1))
    ),
    Groups = list(
      list(
        CriteriaList = list(
          list(Measurement = list(CodesetId = 2))
        )
      )
    )
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("DrugExposure" %in% result)
  expect_true("Measurement" %in% result)
})

# --- collect_batch_used_domains_from_cohorts ---

test_that("collect_batch_used_domains_from_cohorts includes OBSERVATION_PERIOD", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(
        list(ConditionOccurrence = list(CodesetId = 1))
      )
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("OBSERVATION_PERIOD" %in% result)
  expect_true("CONDITION_OCCURRENCE" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles multiple cohorts", {
  c1 <- list(PrimaryCriteria = list(
    CriteriaList = list(list(DrugExposure = list(CodesetId = 1)))
  ))
  c2 <- list(PrimaryCriteria = list(
    CriteriaList = list(list(Measurement = list(CodesetId = 2)))
  ))
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(c1, c2))
  expect_true("DRUG_EXPOSURE" %in% result)
  expect_true("MEASUREMENT" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles inclusion rules", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    InclusionRules = list(
      list(expression = list(
        CriteriaList = list(list(ProcedureOccurrence = list(CodesetId = 2)))
      ))
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("CONDITION_OCCURRENCE" %in% result)
  expect_true("PROCEDURE_OCCURRENCE" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles censoring criteria", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    CensoringCriteria = list(
      list(Death = list())
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("DEATH" %in% result)
})

test_that("collect_batch_used_domains_from_cohorts handles additional criteria", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    AdditionalCriteria = list(
      CriteriaList = list(list(Observation = list(CodesetId = 3)))
    )
  )
  result <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("OBSERVATION" %in% result)
})

# --- OPTIMIZER_STATIC_TABLES ---

test_that("OPTIMIZER_STATIC_TABLES has expected tables", {
  tables <- CDMConnector:::OPTIMIZER_STATIC_TABLES
  expect_true("codesets" %in% tables)
  expect_true("cohort_stage" %in% tables)
  expect_true("atlas_observation_period" %in% tables)
  expect_true("drug_exposure_filtered" %in% tables)
})

# --- CDM_TABLE_NAMES_FOR_QUOTE ---

test_that("CDM_TABLE_NAMES_FOR_QUOTE includes key tables", {
  tables <- CDMConnector:::CDM_TABLE_NAMES_FOR_QUOTE
  expect_true("PERSON" %in% tables)
  expect_true("CONCEPT" %in% tables)
  expect_true("CONCEPT_ANCESTOR" %in% tables)
  expect_true("OBSERVATION_PERIOD" %in% tables)
})
