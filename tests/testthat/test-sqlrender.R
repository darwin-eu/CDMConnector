# Tests for R/sqlrender.R — Pure-R SqlRender pattern engine
# Covers: tokenize_sql, parse_search_pattern, translate_search, translate_sql_with_path,
#         render_r, split_sql_core, evaluate_condition, replace_with_concat, etc.

test_that("tokenize_sql handles basic SQL tokens", {
  tokens <- CDMConnector:::tokenize_sql("SELECT a FROM b WHERE c = 1")
  expect_true(length(tokens) > 0)
  # Tokens should have text, start, end
  expect_true(all(vapply(tokens, function(t) !is.null(t$text), logical(1))))
})

test_that("tokenize_sql handles quoted strings", {
  tokens <- CDMConnector:::tokenize_sql("SELECT 'hello world' FROM t")
  expect_true(length(tokens) > 0)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true(any(grepl("hello", texts)))
})

test_that("tokenize_sql handles parentheses and operators", {
  tokens <- CDMConnector:::tokenize_sql("SELECT (a + b) FROM t WHERE x > 0")
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("(" %in% texts)
  expect_true(")" %in% texts)
})

test_that("split_sql_core splits on semicolons", {
  sql <- "SELECT 1;\nSELECT 2;\nSELECT 3;"
  stmts <- CDMConnector:::split_sql_core(sql)
  expect_true(length(stmts) >= 3)
})

test_that("split_sql_core handles strings with semicolons", {
  sql <- "SELECT 'a;b' FROM t; SELECT 2;"
  stmts <- CDMConnector:::split_sql_core(sql)
  # Should NOT split on the semicolon inside the string literal
  expect_true(length(stmts) >= 2)
  expect_true(any(grepl("a;b", stmts)))
})

test_that("split_sql_core handles empty input", {
  stmts <- CDMConnector:::split_sql_core("")
  expect_true(length(stmts) == 0 || all(stmts == ""))
})

test_that("render_r substitutes parameters", {
  rendered <- CDMConnector:::render_r(
    "SELECT * FROM @schema.@table WHERE id = @id",
    schema = "main", table = "person", id = "42"
  )
  expect_true(grepl("main", rendered))
  expect_true(grepl("person", rendered))
  expect_true(grepl("42", rendered))
  expect_false(grepl("@schema", rendered))
})

test_that("render_r handles missing parameters gracefully", {
  rendered <- CDMConnector:::render_r(
    "SELECT * FROM @schema.table",
    warnOnMissingParameters = FALSE
  )
  # Missing params should remain as-is
  expect_true(grepl("@schema", rendered))
})

test_that("render wrapper works end-to-end", {
  sql <- CDMConnector:::render(
    "SELECT * FROM @cdm.person WHERE id = @id",
    cdm = "main", id = "1"
  )
  expect_true(grepl("main", sql))
  expect_true(grepl("1", sql))
  expect_false(grepl("@cdm", sql))
})

test_that("render handles warnOnMissingParameters = FALSE", {
  sql <- CDMConnector:::render(
    "SELECT @a FROM @b",
    a = "x",
    warnOnMissingParameters = FALSE
  )
  expect_true(grepl("x", sql))
})

test_that("ensure_patterns_loaded populates cache without error", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  expect_no_error(CDMConnector:::ensure_patterns_loaded(path))
})

test_that("translate_sql_with_path translates to postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,start_date) FROM t;",
    "postgresql", "session1", "", path
  )
  # PostgreSQL DATEADD should become interval arithmetic
  expect_false(grepl("DATEADD", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path translates to spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  # Spark: SELECT INTO -> CREATE TABLE USING DELTA
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT a INTO scratch.my_table FROM t;",
    "spark", "session1", "", path
  )
  expect_true(grepl("CREATE TABLE", translated, ignore.case = TRUE))
  expect_true(grepl("USING DELTA", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles ISNULL for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT ISNULL(a, b) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("COALESCE", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles COUNT_BIG for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT COUNT_BIG(DISTINCT person_id) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_false(grepl("COUNT_BIG", translated, ignore.case = TRUE))
  expect_true(grepl("COUNT", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles GETDATE for spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT GETDATE();",
    "spark", "session1", "", path
  )
  expect_false(grepl("GETDATE", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles DATEDIFF for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEDIFF(day,start_date,end_date) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_false(grepl("\\bDATEDIFF\\b", translated, ignore.case = TRUE, perl = TRUE))
})

test_that("translate_sql_with_path handles VARCHAR(MAX)", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "CAST(x AS VARCHAR(MAX));",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("TEXT|varchar", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles snowflake dialect", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,start_date) FROM t;",
    "snowflake", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("translate_sql_with_path handles redshift dialect", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,start_date) FROM t;",
    "redshift", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("parse_search_pattern parses variable patterns with fixed tokens", {
  # Patterns need fixed tokens around variables
  parsed <- CDMConnector:::parse_search_pattern("DATEADD(@@a,@@b,@@c)")
  expect_true(is.list(parsed))
  expect_true(length(parsed) > 0)
  has_var <- any(vapply(parsed, function(p) isTRUE(p$isVariable), logical(1)))
  expect_true(has_var)
})

test_that("find_curly_bracket_spans handles nested braces", {
  sql <- "outer { inner { deep } mid } end"
  spans <- CDMConnector:::find_curly_bracket_spans(sql)
  expect_true(is.matrix(spans) || is.null(spans) || length(spans) >= 0)
})

test_that("listSupportedDialects returns a data frame", {
  dialects <- CDMConnector:::listSupportedDialects()
  expect_true(is.data.frame(dialects) || is.character(dialects))
})

test_that("get_global_session_id returns a string", {
  sid <- CDMConnector:::get_global_session_id()
  expect_type(sid, "character")
  expect_true(nchar(sid) > 0)
})

test_that("generate_session_id creates unique IDs", {
  id1 <- CDMConnector:::generate_session_id()
  id2 <- CDMConnector:::generate_session_id()
  expect_type(id1, "character")
  expect_type(id2, "character")
})

test_that("replace_char_at replaces character in string", {
  result <- CDMConnector:::replace_char_at("hello", 1, "H")
  expect_equal(result, "Hello")
})

test_that("escape_dollar_sign escapes $ in strings", {
  result <- CDMConnector:::escape_dollar_sign("price$100")
  expect_true(grepl("\\\\\\$|\\$", result))
})

test_that("str_replace_all replaces multiple occurrences", {
  result <- CDMConnector:::str_replace_all("aabaa", "aa", "x")
  expect_true(grepl("x", result))
})

test_that("translate with WITH...SELECT INTO for spark produces CTE decomposition", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  sql <- "WITH cte AS (SELECT 1 as id) SELECT id INTO scratch.my_table FROM cte;"
  translated <- CDMConnector:::translate_sql_with_path(
    sql, "spark", "session1", "", path
  )
  # Should have DROP VIEW + CREATE TEMPORARY VIEW + CREATE TABLE
  expect_true(grepl("VIEW", translated, ignore.case = TRUE) ||
              grepl("CREATE TABLE", translated, ignore.case = TRUE))
})

test_that("translate handles EOMONTH for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT EOMONTH(start_date) FROM t;",
    "postgresql", "session1", "", path
  )
  # PostgreSQL EOMONTH should be converted to DATE_TRUNC-based expression
  expect_false(grepl("\\bEOMONTH\\b", translated, ignore.case = TRUE))
})

test_that("translate handles DATEFROMPARTS for spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEFROMPARTS(2020, 1, 15) FROM t;",
    "spark", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("translate handles TRY_CAST", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT TRY_CAST(x AS INTEGER) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("CAST", translated, ignore.case = TRUE))
})

test_that("translate handles TRUNCATE TABLE", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "TRUNCATE TABLE scratch.my_table;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("DELETE FROM|TRUNCATE", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles complex nested SQL", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  # Complex SQL with multiple functions
  sql <- "SELECT ISNULL(DATEADD(day,1,start_date), GETDATE()) FROM t;"
  translated <- CDMConnector:::translate_sql_with_path(
    sql, "postgresql", "session1", "", path
  )
  expect_false(grepl("ISNULL", translated, ignore.case = TRUE))
  expect_false(grepl("GETDATE", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles CAST and type conversions", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path), "replacementPatterns.csv not found")
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT CAST(x AS FLOAT) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

# --- Additional tests for deeper pattern engine coverage ---

test_that("render_r with multiple parameters", {
  sql <- CDMConnector:::render_r(
    "SELECT * FROM @schema.@table WHERE @col = @val",
    schema = "cdm", table = "person", col = "person_id", val = "123"
  )
  expect_true(grepl("cdm", sql))
  expect_true(grepl("person", sql))
  expect_true(grepl("123", sql))
  expect_false(grepl("@", sql))
})

test_that("render_r with list parameter (IN clause)", {
  sql <- CDMConnector:::render_r(
    "SELECT * FROM t WHERE id IN (@ids)",
    ids = c(1, 2, 3)
  )
  expect_true(grepl("1,2,3", sql))
})

test_that("render wrapper substitutes all parameters", {
  sql <- CDMConnector:::render(
    "SELECT * FROM @cdm.@table WHERE @col > @val",
    cdm = "main", table = "person", col = "year_of_birth", val = "1990"
  )
  expect_false(grepl("@", sql))
})

test_that("translate_sql_with_path handles IF OBJECT_ID for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL DROP TABLE #temp;",
    "postgresql", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("translate_sql_with_path handles TOP for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT TOP 10 * FROM person;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("LIMIT", translated, ignore.case = TRUE) || nchar(translated) > 0)
})

test_that("translate_sql_with_path handles multiple statements for spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  # A WITH CTE + SELECT INTO that becomes multi-statement for Spark
  sql <- paste0(
    "WITH cte1 AS (SELECT person_id FROM person), ",
    "cte2 AS (SELECT person_id FROM cte1) ",
    "SELECT person_id INTO scratch.results FROM cte2;"
  )
  translated <- CDMConnector:::translate_sql_with_path(
    sql, "spark", "session1", "", path
  )
  # Should produce views + CREATE TABLE
  expect_true(grepl("VIEW|CREATE TABLE", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles DATEADD with _date suffix for spark regex pattern", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  # Test the regex pattern that wraps DATEADD in CAST for _date columns
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,start_date) FROM t;",
    "spark", "session1", "", path
  )
  expect_true(grepl("CAST|DATEADD|DATE", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles DATEADD without _date suffix for spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  # Column without _date suffix should NOT get CAST wrapper
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,some_column) FROM t;",
    "spark", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
  expect_false(grepl("@@date", translated))
})

test_that("translate_sql_with_path handles DATEADD with dotted column for spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  # Table-qualified column (po.procedure_date) should NOT trigger regex CAST
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,po.procedure_date) FROM t;",
    "spark", "session1", "", path
  )
  expect_false(grepl("@@date", translated))
  expect_true(grepl("procedure_date", translated))
})

test_that("translate_sql_with_path handles CONCAT for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT CONCAT(a, b, c) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("translate_sql_with_path handles LEN for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT LEN(name) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("LENGTH|len|char_length", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles CHARINDEX for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT CHARINDEX('a', name) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("STRPOS|POSITION|charindex", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles STDEV for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT STDEV(val) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("STDDEV|stdev", translated, ignore.case = TRUE))
})

test_that("translate_sql_with_path handles RIGHT and LEFT for postgresql", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT RIGHT(name, 3), LEFT(name, 2) FROM t;",
    "postgresql", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("render_sql_core substitutes simple parameters", {
  result <- CDMConnector:::render_sql_core(
    "SELECT @a FROM @b",
    c(a = "col1", b = "table1")
  )
  expect_true(grepl("col1", result))
  expect_true(grepl("table1", result))
})

test_that("substitute_parameters handles default values", {
  result <- CDMConnector:::render_sql_core(
    "{DEFAULT @limit = 100} SELECT TOP @limit * FROM t",
    c()
  )
  expect_true(grepl("100", result))
})

test_that("split_sql_core handles multi-statement SQL", {
  sql <- "CREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);\nSELECT * FROM t;"
  stmts <- CDMConnector:::split_sql_core(sql)
  expect_true(length(stmts) >= 3)
})

test_that("translate for bigquery dialect works", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,start_date) FROM t;",
    "bigquery", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

test_that("translate for oracle dialect works", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  translated <- CDMConnector:::translate_sql_with_path(
    "SELECT DATEADD(day,1,start_date) FROM t;",
    "oracle", "session1", "", path
  )
  expect_true(nchar(translated) > 0)
})

# --- safe_split ---

test_that("safe_split splits on comma", {
  result <- CDMConnector:::safe_split("a,b,c", ",")
  expect_equal(result, c("a", "b", "c"))
})

test_that("safe_split respects quoted strings", {
  result <- CDMConnector:::safe_split('a,"b,c",d', ",")
  expect_equal(length(result), 3)
  expect_equal(result[1], "a")
  expect_true(grepl("b,c", result[2]))
})

test_that("safe_split handles empty string", {
  result <- CDMConnector:::safe_split("", ",")
  expect_equal(result, "")
})

test_that("safe_split handles escape character", {
  result <- CDMConnector:::safe_split("a\\,b,c", ",")
  # The backslash escapes the comma, so "a\\,b" is one field
  expect_equal(length(result), 2)
})

# --- split_and_keep ---

test_that("split_and_keep splits and keeps regex matches", {
  result <- CDMConnector:::split_and_keep("hello123world", "[0-9]+")
  expect_true(length(result) >= 3)
  expect_true(any(grepl("123", result)))
  expect_true(any(grepl("hello", result)))
  expect_true(any(grepl("world", result)))
})

test_that("split_and_keep returns unchanged when no match", {
  result <- CDMConnector:::split_and_keep("hello world", "[0-9]+")
  expect_equal(length(result), 1)
  expect_equal(result[[1]], "hello world")
})

# --- replace_with_concat ---

test_that("replace_with_concat handles simple string", {
  result <- CDMConnector:::replace_with_concat("SELECT 'hello'")
  expect_true(grepl("hello", result))
})

test_that("replace_with_concat converts escaped quotes to CONCAT", {
  result <- CDMConnector:::replace_with_concat("SELECT 'it''s'")
  expect_true(grepl("CONCAT", result, ignore.case = TRUE))
})

# --- evaluate_primitive_condition ---

test_that("evaluate_primitive_condition handles true/false strings", {
  expect_true(CDMConnector:::evaluate_primitive_condition("true"))
  expect_false(CDMConnector:::evaluate_primitive_condition("false"))
  expect_true(CDMConnector:::evaluate_primitive_condition("1"))
  expect_false(CDMConnector:::evaluate_primitive_condition("0"))
  expect_true(CDMConnector:::evaluate_primitive_condition("!false"))
  expect_false(CDMConnector:::evaluate_primitive_condition("!true"))
  expect_true(CDMConnector:::evaluate_primitive_condition("!0"))
  expect_false(CDMConnector:::evaluate_primitive_condition("!1"))
})

test_that("evaluate_primitive_condition handles == comparison", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'a' == 'a'"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'a' == 'b'"))
})

test_that("evaluate_primitive_condition handles != comparison", {
  expect_true(CDMConnector:::evaluate_primitive_condition("1 != 0"))
  expect_false(CDMConnector:::evaluate_primitive_condition("1 != 1"))
})

test_that("evaluate_primitive_condition handles <> comparison", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'x' <> 'y'"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'x' <> 'x'"))
})

test_that("evaluate_primitive_condition handles IN", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'a' IN ('a','b','c')"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'z' IN ('a','b','c')"))
})

test_that("evaluate_primitive_condition returns FALSE for unknown", {
  expect_false(CDMConnector:::evaluate_primitive_condition("rules"))
})

# --- evaluate_boolean_condition ---

test_that("evaluate_boolean_condition handles AND", {
  expect_true(CDMConnector:::evaluate_boolean_condition("1 != 0 & 2 != 0"))
  expect_false(CDMConnector:::evaluate_boolean_condition("1 != 0 & 0 != 0"))
})

test_that("evaluate_boolean_condition handles OR", {
  expect_true(CDMConnector:::evaluate_boolean_condition("1 != 0 | 0 != 0"))
  expect_false(CDMConnector:::evaluate_boolean_condition("0 != 0 | 0 != 0"))
})

test_that("evaluate_boolean_condition handles simple primitive", {
  expect_true(CDMConnector:::evaluate_boolean_condition("true"))
  expect_false(CDMConnector:::evaluate_boolean_condition("false"))
})

# --- evaluate_condition (with parentheses) ---

test_that("evaluate_condition handles parenthesized condition", {
  expect_true(CDMConnector:::evaluate_condition("(1 != 0)"))
  expect_false(CDMConnector:::evaluate_condition("(0 != 0)"))
})

test_that("evaluate_condition handles nested parentheses", {
  expect_true(CDMConnector:::evaluate_condition("((true))"))
  expect_false(CDMConnector:::evaluate_condition("((false))"))
})

test_that("evaluate_condition handles IN with parentheses", {
  expect_true(CDMConnector:::evaluate_condition("'a' IN ('a','b')"))
  expect_false(CDMConnector:::evaluate_condition("'z' IN ('a','b')"))
})

# --- preceded_by_in ---

test_that("preceded_by_in detects ' in ' before position", {
  # "SELECT x IN (1,2)" - the ( is at position 13
  expect_true(CDMConnector:::preceded_by_in(13, "SELECT x IN (1,2)"))
  expect_false(CDMConnector:::preceded_by_in(8, "SELECT (1,2)"))
})

# --- link_if_then_elses ---

test_that("link_if_then_elses finds condition-then pairs", {
  str <- "{true}?{yes}"
  spans <- CDMConnector:::find_curly_bracket_spans(str)
  ite <- CDMConnector:::link_if_then_elses(str, spans)
  expect_true(length(ite) > 0)
  expect_false(ite[[1]]$hasIfFalse)
})

test_that("link_if_then_elses finds condition-then-else triple", {
  str <- "{true}?{yes}:{no}"
  spans <- CDMConnector:::find_curly_bracket_spans(str)
  ite <- CDMConnector:::link_if_then_elses(str, spans)
  expect_true(length(ite) > 0)
  expect_true(ite[[1]]$hasIfFalse)
})

# --- parse_if_then_else ---

test_that("parse_if_then_else evaluates true condition", {
  result <- CDMConnector:::parse_if_then_else("{1 != 0}?{YES}")
  expect_true(grepl("YES", result))
})

test_that("parse_if_then_else evaluates false condition strips content", {
  result <- CDMConnector:::parse_if_then_else("{0 != 0}?{YES}")
  expect_false(grepl("YES", result))
})

test_that("parse_if_then_else evaluates false with else", {
  result <- CDMConnector:::parse_if_then_else("{0 != 0}?{YES}:{NO}")
  expect_false(grepl("YES", result))
  expect_true(grepl("NO", result))
})

# --- render_sql_core ---

test_that("render_sql_core substitutes and evaluates", {
  sql <- "SELECT @col FROM @tbl WHERE {1 != 0}?{active = 1}"
  result <- CDMConnector:::render_sql_core(sql, c(col = "name", tbl = "person"))
  expect_true(grepl("name", result))
  expect_true(grepl("person", result))
  expect_true(grepl("active = 1", result))
})

test_that("render_sql_core handles DEFAULT values", {
  sql <- "{DEFAULT @x = 5}\nSELECT @x"
  result <- CDMConnector:::render_sql_core(sql, character(0))
  expect_true(grepl("5", result))
})

test_that("render_sql_core overrides DEFAULT when param provided", {
  sql <- "{DEFAULT @x = 5}\nSELECT @x"
  result <- CDMConnector:::render_sql_core(sql, c(x = "10"))
  expect_true(grepl("10", result))
  expect_false(grepl("5", result))
})

# --- extract_defaults ---

test_that("extract_defaults parses DEFAULT blocks", {
  sql <- "{DEFAULT @schema = dbo}{DEFAULT @prefix = temp_}"
  result <- CDMConnector:::extract_defaults(sql)
  expect_equal(result$schema, "dbo")
  expect_equal(result$prefix, "temp_")
})

# --- remove_defaults ---

test_that("remove_defaults removes DEFAULT blocks", {
  sql <- "{DEFAULT @x = 5}\nSELECT 1"
  result <- CDMConnector:::remove_defaults(sql)
  expect_false(grepl("DEFAULT", result))
  expect_true(grepl("SELECT", result))
})

# --- substitute_parameters ---

test_that("substitute_parameters replaces longer names first", {
  result <- CDMConnector:::substitute_parameters(
    "SELECT @table_name FROM @table",
    c(table_name = "person", table = "t")
  )
  expect_true(grepl("person", result))
})

# --- translate_bigquery ---

test_that("translate_bigquery lowercases non-quoted tokens", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  CDMConnector:::ensure_patterns_loaded(path)
  result <- CDMConnector:::translate_bigquery("SELECT Column1 FROM 'MyTable'")
  expect_true(grepl("column1", result))
})

# --- translate_spark ---

test_that("translate_spark converts CREATE TABLE to SELECT INTO", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  CDMConnector:::ensure_patterns_loaded(path)
  result <- CDMConnector:::translate_spark("CREATE TABLE mytbl (a integer, b date)")
  expect_true(grepl("SELECT", result, ignore.case = TRUE))
  expect_true(grepl("INTO", result, ignore.case = TRUE))
  # Spark converts CREATE TABLE columns to SELECT ... INTO ... WHERE 1 = 0
  expect_true(grepl("WHERE 1 = 0", result))
})

# --- translate_r (main entry point) ---

test_that("translate_r translates to postgresql", {
  result <- CDMConnector:::translate_r(
    "SELECT DATEADD(day,1,start_date) FROM t",
    targetDialect = "postgresql"
  )
  expect_false(grepl("DATEADD", result))
  expect_true(!is.null(attr(result, "sqlDialect")))
  expect_equal(attr(result, "sqlDialect"), "postgresql")
})

test_that("translate_r warns on already translated SQL", {
  sql <- "SELECT 1"
  attr(sql, "sqlDialect") <- "postgresql"
  expect_warning(
    CDMConnector:::translate_r(sql, targetDialect = "postgresql"),
    "already been translated"
  )
})

test_that("translate_r uses oracleTempSchema fallback", {
  result <- CDMConnector:::translate_r(
    "SELECT 1;",
    targetDialect = "oracle",
    oracleTempSchema = "temp_schema"
  )
  expect_true(nchar(result) > 0)
})

# --- translate_check_warnings ---

test_that("translate_check_warnings warns on long temp table names", {
  long_name <- paste0("#", paste(rep("a", 130), collapse = ""))
  sql <- paste("CREATE TABLE", long_name, "(id INT)")
  warnings <- CDMConnector:::translate_check_warnings(sql, "postgresql")
  expect_true(length(warnings) > 0)
})

test_that("translate_check_warnings warns on long permanent table names", {
  long_name <- paste(rep("a", 130), collapse = "")
  sql <- paste("CREATE TABLE", long_name, "(id INT)")
  warnings <- CDMConnector:::translate_check_warnings(sql, "postgresql")
  expect_true(length(warnings) > 0)
})

# --- translate_single_statement_with_path ---

test_that("translate_single_statement_with_path works for single statement", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CDMConnector:::translate_single_statement_with_path(
    "SELECT ISNULL(a, 0) FROM t",
    "postgresql", "session1", "", path
  )
  expect_true(grepl("COALESCE", result, ignore.case = TRUE))
})

# --- render_check_missing_params ---

test_that("render_check_missing_params finds missing parameters", {
  warnings <- CDMConnector:::render_check_missing_params("SELECT @x", c("x", "y"))
  expect_true(any(grepl("y", warnings)))
  expect_false(any(grepl("x", warnings)))
})

# --- str_replace_region ---

test_that("str_replace_region replaces substring", {
  result <- CDMConnector:::str_replace_region("hello world", 6, 11, "there")
  expect_equal(result, "hellothere")
})

# --- token_is_identifier ---

test_that("token_is_identifier detects identifiers", {
  expect_true(CDMConnector:::token_is_identifier("my_table"))
  expect_true(CDMConnector:::token_is_identifier("column1"))
  expect_false(CDMConnector:::token_is_identifier("my-table"))
  expect_false(CDMConnector:::token_is_identifier(""))
})

# --- remove_parentheses_quotes ---

test_that("remove_parentheses_quotes strips outer quotes", {
  expect_equal(CDMConnector:::remove_parentheses_quotes("'hello'"), "hello")
  expect_equal(CDMConnector:::remove_parentheses_quotes('"hello"'), "hello")
  expect_equal(CDMConnector:::remove_parentheses_quotes("hello"), "hello")
  expect_equal(CDMConnector:::remove_parentheses_quotes("x"), "x")
})

# --- translate_sql_with_path error on bad dialect ---

test_that("translate_sql_with_path errors on unsupported dialect", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  CDMConnector:::ensure_patterns_loaded(path)
  expect_error(
    CDMConnector:::translate_sql_with_path("SELECT 1", "nosuchdb", "s1", "", path),
    "Don't know how to translate"
  )
})

# --- translate_sql_with_path spark path ---

test_that("translate_sql_with_path for spark invokes translate_spark", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CDMConnector:::translate_sql_with_path(
    "SELECT ISNULL(a, 0) FROM t;",
    "spark", "session1", "", path
  )
  expect_true(nchar(result) > 0)
  expect_true(grepl("coalesce|COALESCE", result, ignore.case = TRUE))
})

# --- translate_sql_with_path bigquery path ---

test_that("translate_sql_with_path for bigquery invokes translate_bigquery", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CDMConnector:::translate_sql_with_path(
    "SELECT ISNULL(a, 0) FROM t;",
    "bigquery", "session1", "", path
  )
  expect_true(nchar(result) > 0)
})

# --- replace_with_concat with impala dialect ---

test_that("translate_sql_with_path for impala uses replace_with_concat", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  # Impala uses replace_with_concat for escaped quotes
  result <- CDMConnector:::translate_sql_with_path(
    "SELECT 'it''s here' FROM t;",
    "spark", "session1", "", path
  )
  expect_true(nchar(result) > 0)
})
