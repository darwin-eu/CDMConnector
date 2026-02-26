# Tests for R/optimizer.R — Batch orchestration, schema handling, literal conditionals
# Covers: resolve_literal_conditionals, normalize_schema_str, extract_write_prefix,
#         quote_cdm_table_refs, collect_batch_used_domains_from_cohorts

# --- resolve_literal_conditionals ---

test_that("resolve_literal_conditionals evaluates true condition", {
  # {1 != 0}?{THEN_CONTENT}
  sql <- "SELECT {1 != 0}?{special_column} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("special_column", result))
  expect_false(grepl("\\{", result))
})

test_that("resolve_literal_conditionals evaluates false condition", {
  # {0 != 0}?{THEN_CONTENT}
  sql <- "SELECT {0 != 0}?{special_column} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_false(grepl("special_column", result))
})

test_that("resolve_literal_conditionals evaluates true with else", {
  # {1 != 0}?{THEN_CONTENT}:{ELSE_CONTENT}
  sql <- "SELECT {1 != 0}?{then_val}:{else_val} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("then_val", result))
  expect_false(grepl("else_val", result))
})

test_that("resolve_literal_conditionals evaluates false with else", {
  sql <- "SELECT {0 != 0}?{then_val}:{else_val} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_false(grepl("then_val", result))
  expect_true(grepl("else_val", result))
})

test_that("resolve_literal_conditionals handles compound AND condition", {
  # {1 != 0 & 2 != 0} -> true
  sql <- "SELECT {1 != 0 & 2 != 0}?{both_true} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("both_true", result))
})

test_that("resolve_literal_conditionals handles compound AND with false", {
  # {1 != 0 & 0 != 0} -> false (second part is 0)
  sql <- "SELECT {1 != 0 & 0 != 0}?{both_true}:{one_false} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_false(grepl("both_true", result))
  expect_true(grepl("one_false", result))
})

test_that("resolve_literal_conditionals handles multiple conditions", {
  sql <- "A {1 != 0}?{first} B {0 != 0}?{second}:{third} C"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("first", result))
  expect_false(grepl("second", result))
  expect_true(grepl("third", result))
})

test_that("resolve_literal_conditionals handles nested braces", {
  sql <- "SELECT {1 != 0}?{FUNC({a,b})} FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_true(grepl("FUNC", result))
})

test_that("resolve_literal_conditionals returns unchanged if no conditionals", {
  sql <- "SELECT * FROM t"
  result <- CDMConnector:::resolve_literal_conditionals(sql)
  expect_equal(result, sql)
})

# --- normalize_schema_str ---

test_that("normalize_schema_str handles NULL", {
  expect_equal(CDMConnector:::normalize_schema_str(NULL), "main")
})

test_that("normalize_schema_str handles NULL with custom default", {
  expect_equal(CDMConnector:::normalize_schema_str(NULL, "cdm"), "cdm")
})

test_that("normalize_schema_str handles simple string", {
  expect_equal(CDMConnector:::normalize_schema_str("my_schema"), "my_schema")
})

test_that("normalize_schema_str handles named character vector", {
  x <- c(catalog = "mydb", schema = "dbo")
  result <- CDMConnector:::normalize_schema_str(x)
  expect_true(grepl("mydb", result))
  expect_true(grepl("dbo", result))
})

test_that("normalize_schema_str handles named vector with prefix", {
  x <- c(schema = "scratch", prefix = "mdb123_")
  result <- CDMConnector:::normalize_schema_str(x)
  # prefix should be stripped
  expect_false(grepl("mdb123", result))
  expect_true(grepl("scratch", result))
})

test_that("normalize_schema_str handles list with prefix", {
  x <- list(schema = "scratch", prefix = "mdb123_")
  result <- CDMConnector:::normalize_schema_str(x)
  expect_false(grepl("mdb123", result))
  expect_true(grepl("scratch", result))
})

test_that("normalize_schema_str handles list with catalog + schema", {
  x <- list(catalog = "ATLAS", schema = "SCRATCH")
  result <- CDMConnector:::normalize_schema_str(x)
  expect_equal(result, "ATLAS.SCRATCH")
})

# --- extract_write_prefix ---

test_that("extract_write_prefix returns empty for NULL", {
  expect_equal(CDMConnector:::extract_write_prefix(NULL), "")
})

test_that("extract_write_prefix extracts from list", {
  x <- list(schema = "scratch", prefix = "mdb123_")
  expect_equal(CDMConnector:::extract_write_prefix(x), "mdb123_")
})

test_that("extract_write_prefix extracts from named vector", {
  x <- c(schema = "scratch", prefix = "mdb999_")
  expect_equal(CDMConnector:::extract_write_prefix(x), "mdb999_")
})

test_that("extract_write_prefix returns empty when no prefix", {
  x <- list(schema = "scratch")
  expect_equal(CDMConnector:::extract_write_prefix(x), "")
})

# --- collect_batch_used_domains_from_cohorts ---

test_that("collect_batch_used_domains_from_cohorts finds DRUG_EXPOSURE", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"CollapseType":"ERA","EraPad":0}}'
  expr <- CDMConnector:::cohortExpressionFromJson(json)
  domains <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(expr))
  expect_true("DRUG_EXPOSURE" %in% domains)
})

test_that("collect_batch_used_domains_from_cohorts finds CONDITION_OCCURRENCE", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"ConditionOccurrence":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"CollapseType":"ERA","EraPad":0}}'
  expr <- CDMConnector:::cohortExpressionFromJson(json)
  domains <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(expr))
  expect_true("CONDITION_OCCURRENCE" %in% domains)
})

test_that("collect_batch_used_domains_from_cohorts finds nested inclusion domains", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "has_measurement",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(Measurement = list(CodesetId = 1L)),
          StartWindow = list(
            Start = list(Days = 0L, Coeff = -1L),
            End = list(Days = 365L, Coeff = 1L),
            UseEventEnd = FALSE, UseIndexEnd = FALSE
          ),
          RestrictVisit = FALSE,
          IgnoreObservationPeriod = FALSE,
          Occurrence = list(Type = 2L, Count = 1L, IsDistinct = FALSE)
        ))
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  expr <- CDMConnector:::cohortExpressionFromJson(as.character(json))
  domains <- CDMConnector:::collect_batch_used_domains_from_cohorts(list(expr))
  expect_true("DRUG_EXPOSURE" %in% domains)
  expect_true("MEASUREMENT" %in% domains)
})

# --- quote_cdm_table_refs ---

test_that("quote_cdm_table_refs runs without error on DuckDB", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.PERSON JOIN main.CONCEPT ON 1=1"
  result <- CDMConnector:::quote_cdm_table_refs(sql, con, "main")
  # Result should still reference the tables
  expect_true(grepl("PERSON", result))
  expect_true(grepl("CONCEPT", result))
})

# --- drop_prefixed_tables ---

test_that("drop_prefixed_tables drops matching tables", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE atlas_test_tbl1 (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE atlas_test_tbl2 (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE other_table (id INTEGER)")
  CDMConnector:::drop_prefixed_tables(con, "main", "atlas_test_")
  tables <- DBI::dbListTables(con)
  expect_false("atlas_test_tbl1" %in% tables)
  expect_false("atlas_test_tbl2" %in% tables)
  expect_true("other_table" %in% tables)
})

test_that("atlas_unique_prefix returns a string", {
  prefix <- CDMConnector:::atlas_unique_prefix()
  expect_type(prefix, "character")
  expect_true(nchar(prefix) > 0)
  expect_true(grepl("atlas_", prefix))
})
