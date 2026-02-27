# Additional tests for R/translate_lightweight.R — PostgreSQL translation,
# helper functions, rewrite functions

# --- PostgreSQL function transformers ---

test_that(".dateadd_postgresql generates interval arithmetic for day", {
  result <- CDMConnector:::.dateadd_postgresql(c("day", "5", "start_date"))
  expect_true(grepl("INTERVAL '1 day'", result))
  expect_true(grepl("start_date", result))
})

test_that(".dateadd_postgresql generates interval arithmetic for month", {
  result <- CDMConnector:::.dateadd_postgresql(c("month", "3", "event_date"))
  expect_true(grepl("INTERVAL '1 month'", result))
})

test_that(".dateadd_postgresql generates interval arithmetic for year", {
  result <- CDMConnector:::.dateadd_postgresql(c("year", "2", "birth_date"))
  expect_true(grepl("INTERVAL '1 year'", result))
})

test_that(".dateadd_postgresql handles short unit codes", {
  result_d <- CDMConnector:::.dateadd_postgresql(c("d", "1", "dt"))
  expect_true(grepl("day", result_d))

  result_m <- CDMConnector:::.dateadd_postgresql(c("mm", "1", "dt"))
  expect_true(grepl("month", result_m))

  result_y <- CDMConnector:::.dateadd_postgresql(c("yyyy", "1", "dt"))
  expect_true(grepl("year", result_y))
})

test_that(".datefromparts_postgresql generates make_date", {
  result <- CDMConnector:::.datefromparts_postgresql(c("2020", "6", "15"))
  expect_true(grepl("make_date", result))
  expect_true(grepl("2020", result))
})

test_that(".datediff_postgresql generates day arithmetic", {
  result <- CDMConnector:::.datediff_postgresql(c("day", "d1", "d2"))
  expect_true(grepl("d2", result))
  expect_true(grepl("d1", result))
})

test_that(".datediff_postgresql generates year diff", {
  result <- CDMConnector:::.datediff_postgresql(c("year", "d1", "d2"))
  expect_true(grepl("YEAR", result))
})

test_that(".datediff_postgresql generates month diff", {
  result <- CDMConnector:::.datediff_postgresql(c("month", "d1", "d2"))
  expect_true(grepl("month", result, ignore.case = TRUE))
})

# --- Full PostgreSQL translator ---

test_that(".translate_postgresql replaces COUNT_BIG with COUNT", {
  stmts <- "SELECT COUNT_BIG(*) FROM tbl"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("COUNT\\(", result))
  expect_false(grepl("COUNT_BIG", result))
})

test_that(".translate_postgresql replaces GETDATE with CURRENT_DATE", {
  stmts <- "SELECT GETDATE()"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("CURRENT_DATE", result))
})

test_that(".translate_postgresql replaces TRY_CAST with CAST", {
  stmts <- "SELECT TRY_CAST(col AS INT)"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("CAST\\(", result))
  expect_false(grepl("TRY_CAST", result))
})

test_that(".translate_postgresql replaces TRUNCATE TABLE with DELETE FROM", {
  stmts <- "TRUNCATE TABLE my_table"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("DELETE FROM", result))
})

test_that(".translate_postgresql replaces VARCHAR(MAX) with TEXT", {
  stmts <- "CREATE TABLE t (col VARCHAR(MAX))"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("TEXT", result))
})

test_that(".translate_postgresql replaces DATETIME with TIMESTAMP", {
  stmts <- "CREATE TABLE t (col DATETIME)"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("TIMESTAMP", result))
})

test_that(".translate_postgresql replaces FLOAT with NUMERIC", {
  stmts <- "CREATE TABLE t (col FLOAT)"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("NUMERIC", result))
})

test_that(".translate_postgresql transforms SELECT INTO", {
  stmts <- "SELECT col1, col2 INTO new_table FROM old_table"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("CREATE TABLE", result))
})

test_that(".translate_postgresql rewrites DATEADD", {
  stmts <- "SELECT DATEADD(day, 5, start_date) FROM tbl"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("INTERVAL", result))
})

test_that(".translate_postgresql rewrites DATEDIFF", {
  stmts <- "SELECT DATEDIFF(day, d1, d2) FROM tbl"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("CAST", result))
})

test_that(".translate_postgresql rewrites DATEFROMPARTS", {
  stmts <- "SELECT DATEFROMPARTS(2020, 6, 15)"
  result <- CDMConnector:::.translate_postgresql(stmts)
  expect_true(grepl("make_date", result))
})

# --- DuckDB YEAR/MONTH/DAY wrapping ---

test_that(".translate_duckdb wraps YEAR with CAST AS DATE", {
  stmts <- "SELECT YEAR(birth_date) FROM person"
  result <- CDMConnector:::.translate_duckdb(stmts)
  expect_true(grepl("YEAR\\(CAST\\(", result))
})

test_that(".translate_duckdb wraps MONTH with CAST AS DATE", {
  stmts <- "SELECT MONTH(start_date) FROM obs"
  result <- CDMConnector:::.translate_duckdb(stmts)
  expect_true(grepl("MONTH\\(CAST\\(", result))
})

test_that(".translate_duckdb wraps DAY with CAST AS DATE", {
  stmts <- "SELECT DAY(end_date) FROM obs"
  result <- CDMConnector:::.translate_duckdb(stmts)
  expect_true(grepl("DAY\\(CAST\\(", result))
})

# --- .rewrite_func ---

test_that(".rewrite_func handles nested function calls", {
  sql <- "SELECT DATEADD(day, DATEADD(day, 1, d1), d2) FROM t"
  transformer <- function(args) paste0("REPLACED(", paste(args, collapse = ", "), ")")
  result <- CDMConnector:::.rewrite_func(sql, "DATEADD", transformer)
  # Should rewrite both occurrences
  expect_false(grepl("DATEADD", result, ignore.case = TRUE))
})

# --- .extract_balanced_args ---

test_that(".extract_func_args extracts simple args", {
  # The function expects sql with open paren at open_pos
  sql <- "(day, 5, start_date)"
  result <- CDMConnector:::.extract_func_args(sql, 1L)
  expect_equal(length(result$args), 3)
  expect_equal(trimws(result$args[1]), "day")
  expect_equal(trimws(result$args[2]), "5")
  expect_equal(trimws(result$args[3]), "start_date")
})

test_that(".extract_func_args handles nested parens", {
  sql <- "(CAST(x AS INT), y)"
  result <- CDMConnector:::.extract_func_args(sql, 1L)
  expect_equal(length(result$args), 2)
  expect_true(grepl("CAST", result$args[1]))
})

# --- EOMONTH transformers ---

test_that(".eomonth_duckdb generates end of month SQL", {
  result <- CDMConnector:::.eomonth_duckdb(c("start_date"))
  expect_true(grepl("DATE_TRUNC", result))
  expect_true(grepl("MONTH", result))
})

# --- DuckDB month/year DATEDIFF ---

test_that(".datediff_duckdb generates month diff", {
  result <- CDMConnector:::.datediff_duckdb(c("month", "d1", "d2"))
  expect_true(grepl("month", result, ignore.case = TRUE))
})

test_that(".datediff_duckdb generates year diff", {
  result <- CDMConnector:::.datediff_duckdb(c("year", "d1", "d2"))
  expect_true(grepl("YEAR", result))
})

# --- DuckDB DATEADD with month/year/hour ---

test_that(".dateadd_duckdb generates TO_MONTHS for month unit", {
  result <- CDMConnector:::.dateadd_duckdb(c("month", "3", "start_date"))
  expect_true(grepl("TO_MONTHS", result))
})

test_that(".dateadd_duckdb generates TO_YEARS for year unit", {
  result <- CDMConnector:::.dateadd_duckdb(c("year", "2", "birth_date"))
  expect_true(grepl("TO_YEARS", result))
})

test_that(".dateadd_duckdb generates TO_HOURS for hour unit", {
  result <- CDMConnector:::.dateadd_duckdb(c("hour", "1", "ts"))
  expect_true(grepl("TO_HOURS", result))
})

test_that(".dateadd_duckdb handles short unit codes", {
  expect_true(grepl("TO_DAYS", CDMConnector:::.dateadd_duckdb(c("dd", "1", "d"))))
  expect_true(grepl("TO_MONTHS", CDMConnector:::.dateadd_duckdb(c("mm", "1", "d"))))
  expect_true(grepl("TO_YEARS", CDMConnector:::.dateadd_duckdb(c("yyyy", "1", "d"))))
})
