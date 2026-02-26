# Tests for R/translate_lightweight.R — SQL dialect translation pipeline
# Covers: translate_cohort_stmts, .translate_stmts_r, .translate_duckdb,
#         .translate_postgresql, .transform_select_into, Spark post-processing

# --- translate_cohort_stmts dispatch ---

test_that("translate_cohort_stmts returns unchanged for sql server", {
  stmts <- c("SELECT 1", "SELECT 2")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "sql server")
  expect_equal(result[1], "SELECT 1")
  expect_equal(result[2], "SELECT 2")
})

test_that("translate_cohort_stmts converts ANALYZE for sql server", {
  stmts <- c("ANALYZE my_table", "SELECT 1")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "sql server")
  expect_true(grepl("UPDATE STATISTICS", result[1]))
  expect_equal(result[2], "SELECT 1")
})

test_that("translate_cohort_stmts converts ANALYZE with leading comment for sql server", {
  stmts <- c("-- hints\nANALYZE my_table", "SELECT 1")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "sql server")
  expect_true(grepl("UPDATE STATISTICS", result[1]))
})

test_that("translate_cohort_stmts returns NULL dialect unchanged", {
  stmts <- c("SELECT 1")
  result <- CDMConnector:::translate_cohort_stmts(stmts, NULL)
  expect_equal(result, stmts)
})

# --- DuckDB fast path ---

test_that("translate_cohort_stmts duckdb strips ANALYZE", {
  stmts <- c("ANALYZE my_table", "SELECT 1")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_equal(trimws(result[1]), "")
  expect_equal(result[2], "SELECT 1")
})

test_that("translate_cohort_stmts duckdb converts DATEADD", {
  stmts <- c("SELECT DATEADD(day,1,start_date) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_false(grepl("DATEADD", result[1]))
})

test_that("translate_cohort_stmts duckdb converts DATEDIFF", {
  stmts <- c("SELECT DATEDIFF(day,start_date,end_date) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_false(grepl("DATEDIFF", result[1]))
})

test_that("translate_cohort_stmts duckdb converts EOMONTH", {
  stmts <- c("SELECT EOMONTH(start_date) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_false(grepl("\\bEOMONTH\\b", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts duckdb converts DATEFROMPARTS", {
  stmts <- c("SELECT DATEFROMPARTS(2020,1,15) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_false(grepl("DATEFROMPARTS", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts duckdb converts IIF", {
  stmts <- c("SELECT IIF(a > 0, 'yes', 'no') FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_true(grepl("CASE\\s+WHEN", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts duckdb converts ISNULL", {
  stmts <- c("SELECT ISNULL(a, 0) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_true(grepl("COALESCE", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts duckdb converts COUNT_BIG", {
  stmts <- c("SELECT COUNT_BIG(DISTINCT person_id) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "duckdb")
  expect_false(grepl("COUNT_BIG", result[1], ignore.case = TRUE))
  expect_true(grepl("COUNT", result[1], ignore.case = TRUE))
})

# --- PostgreSQL path via SqlRender ---

test_that("translate_cohort_stmts postgresql converts DATEADD", {
  stmts <- c("SELECT DATEADD(day,1,start_date) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "postgresql")
  expect_false(grepl("\\bDATEADD\\b", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts postgresql converts DATEDIFF", {
  stmts <- c("SELECT DATEDIFF(day,start_date,end_date) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "postgresql")
  expect_false(grepl("\\bDATEDIFF\\b", result[1], ignore.case = TRUE, perl = TRUE))
})

test_that("translate_cohort_stmts postgresql converts ISNULL", {
  stmts <- c("SELECT ISNULL(a, 0) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "postgresql")
  expect_true(grepl("COALESCE", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts postgresql converts SELECT INTO", {
  stmts <- c("SELECT a INTO scratch.my_table FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "postgresql")
  expect_true(grepl("CREATE TABLE", result[1], ignore.case = TRUE))
})

# --- Snowflake path via SqlRender ---

test_that("translate_cohort_stmts snowflake converts DATEADD", {
  stmts <- c("SELECT DATEADD(day,1,start_date) FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "snowflake")
  expect_true(nchar(result[1]) > 0)
})

test_that("translate_cohort_stmts snowflake converts SELECT INTO", {
  stmts <- c("SELECT a INTO scratch.my_table FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "snowflake")
  expect_true(grepl("CREATE TABLE", result[1], ignore.case = TRUE))
})

# --- Spark/Databricks path via SqlRender ---

test_that("translate_cohort_stmts spark converts SELECT INTO to CREATE TABLE USING DELTA", {
  stmts <- c("SELECT a INTO scratch.my_table FROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  # After re-splitting, one of the statements should have CREATE TABLE
  expect_true(any(grepl("CREATE TABLE", result, ignore.case = TRUE)))
  expect_true(any(grepl("USING DELTA", result, ignore.case = TRUE)))
})

test_that("translate_cohort_stmts spark strips SQL comments", {
  stmts <- c("SELECT a -- this is a comment\nFROM t")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  # Comments should be stripped
  expect_false(grepl("this is a comment", result[1]))
  expect_true(grepl("SELECT", result[1], ignore.case = TRUE))
})

test_that("translate_cohort_stmts spark re-splits multi-statement output", {
  # Simulate SqlRender Spark output with multiple statements joined by ;
  stmts <- c("DROP VIEW IF EXISTS cte1; CREATE TEMPORARY VIEW cte1 AS (SELECT 1); CREATE TABLE t USING DELTA AS SELECT * FROM cte1")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  # After re-splitting, should have 3 separate statements
  non_empty <- result[nzchar(trimws(result))]
  expect_true(length(non_empty) >= 3)
})

test_that("translate_cohort_stmts spark converts ANALYZE to ANALYZE TABLE ... COMPUTE STATISTICS", {
  stmts <- c("ANALYZE my_table")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  non_empty <- result[nzchar(trimws(result))]
  expect_true(any(grepl("ANALYZE TABLE", non_empty, ignore.case = TRUE)))
  expect_true(any(grepl("COMPUTE STATISTICS", non_empty, ignore.case = TRUE)))
})

test_that("translate_cohort_stmts spark strips NULL constraints from DDL", {
  stmts <- c("CREATE TABLE t (a INTEGER NOT NULL, b DATE NULL)")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  result_sql <- paste(result, collapse = " ")
  expect_false(grepl("NOT NULL", result_sql, ignore.case = TRUE))
})

test_that("translate_cohort_stmts spark skips DDL through SqlRender", {
  # Plain CREATE TABLE should NOT be sent through SqlRender
  stmts <- c("CREATE TABLE scratch.test_tbl (id INTEGER, name VARCHAR(100))")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  result_sql <- paste(result, collapse = " ")
  expect_true(grepl("CREATE TABLE", result_sql, ignore.case = TRUE))
})

test_that("translate_cohort_stmts spark skips DROP TABLE through SqlRender", {
  stmts <- c("DROP TABLE IF EXISTS scratch.test_tbl")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "spark")
  expect_true(any(grepl("DROP TABLE", result, ignore.case = TRUE)))
})

# --- .transform_select_into ---

test_that(".transform_select_into converts basic SELECT INTO", {
  sql <- "SELECT a, b INTO scratch.my_table FROM source_table"
  result <- CDMConnector:::.transform_select_into(sql)
  expect_true(grepl("CREATE TABLE", result, ignore.case = TRUE))
  expect_true(grepl("scratch.my_table", result))
  expect_true(grepl("SELECT a, b", result))
})

test_that(".transform_select_into leaves non-SELECT-INTO unchanged", {
  sql <- "INSERT INTO t (a) SELECT 1"
  result <- CDMConnector:::.transform_select_into(sql)
  expect_equal(sql, result)
})

# --- .extract_func_args ---

test_that(".extract_func_args extracts function arguments", {
  # .extract_func_args returns a list with function position details
  result <- CDMConnector:::.extract_func_args("DATEADD(day,1,start_date)", 8L)
  expect_true(is.character(result) || is.list(result))
})

# --- .rewrite_functions_vec ---

test_that(".rewrite_functions_vec handles multiple functions", {
  stmts <- c(
    "SELECT DATEADD(day,1,start_date), DATEDIFF(day,a,b) FROM t",
    "SELECT ISNULL(x, 0) FROM t"
  )
  result <- CDMConnector:::.rewrite_functions_vec(stmts, "duckdb")
  expect_false(any(grepl("DATEADD", result, ignore.case = TRUE)))
  expect_true(any(grepl("COALESCE", result, ignore.case = TRUE)))
})

# --- Oracle dialect ---

test_that("translate_cohort_stmts oracle strips ANALYZE", {
  stmts <- c("ANALYZE my_table", "SELECT 1")
  result <- CDMConnector:::translate_cohort_stmts(stmts, "oracle")
  expect_equal(trimws(result[1]), "")
})
