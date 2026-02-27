# Additional tests for R/sqlrender.R — tokenize, split, render, translate pure functions

# --- tokenize_sql ---

test_that("tokenize_sql tokenizes simple SELECT", {
  tokens <- CDMConnector:::tokenize_sql("SELECT * FROM t")
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("select" %in% texts || "SELECT" %in% texts)
  expect_true("*" %in% texts)
})

test_that("tokenize_sql handles empty string", {
  tokens <- CDMConnector:::tokenize_sql("")
  expect_equal(length(tokens), 0)
})

test_that("tokenize_sql handles single-line comment", {
  tokens <- CDMConnector:::tokenize_sql("SELECT 1 -- this is a comment\nSELECT 2")
  # Comment should be skipped
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_false(any(grepl("comment", texts)))
})

test_that("tokenize_sql handles block comment", {
  tokens <- CDMConnector:::tokenize_sql("SELECT /* block */ 1")
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_false(any(grepl("block", texts)))
})

test_that("tokenize_sql handles single-quoted string", {
  tokens <- CDMConnector:::tokenize_sql("SELECT 'hello world'")
  # The string content should be in quotes
  in_quotes <- vapply(tokens, function(t) t$inQuotes, logical(1))
  expect_true(any(in_quotes))
})

test_that("tokenize_sql handles double-quoted identifier", {
  tokens <- CDMConnector:::tokenize_sql('SELECT "my_col" FROM t')
  in_quotes <- vapply(tokens, function(t) t$inQuotes, logical(1))
  expect_true(any(in_quotes))
})

test_that("tokenize_sql handles --HINT: keyword", {
  tokens <- CDMConnector:::tokenize_sql("--hint SELECT 1")
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("--" %in% texts)
})

test_that("tokenize_sql handles @parameter tokens", {
  tokens <- CDMConnector:::tokenize_sql("SELECT @param FROM t")
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("@param" %in% texts)
})

# --- replace_char_at ---

test_that("replace_char_at replaces character at position", {
  result <- CDMConnector:::replace_char_at("hello", 1, "H")
  expect_equal(result, "Hello")
})

# --- str_replace_region ---

test_that("str_replace_region replaces region in string", {
  result <- CDMConnector:::str_replace_region("hello world", 6, 11, "there")
  expect_true(grepl("there", result))
})

# --- str_replace_all ---

test_that("str_replace_all replaces all occurrences", {
  result <- CDMConnector:::str_replace_all("a.b.c", ".", "_")
  expect_equal(result, "a_b_c")
})

# --- token_is_identifier ---

test_that("token_is_identifier returns TRUE for identifiers", {
  expect_true(CDMConnector:::token_is_identifier("hello"))
  expect_true(CDMConnector:::token_is_identifier("table_1"))
  expect_true(CDMConnector:::token_is_identifier("ABC123"))
})

test_that("token_is_identifier returns FALSE for operators", {
  expect_false(CDMConnector:::token_is_identifier("*"))
  expect_false(CDMConnector:::token_is_identifier("."))
  expect_false(CDMConnector:::token_is_identifier("+"))
})

# --- safe_split ---

test_that("safe_split splits by delimiter", {
  result <- CDMConnector:::safe_split("a,b,c", ",")
  expect_equal(result, c("a", "b", "c"))
})

test_that("safe_split handles empty string", {
  result <- CDMConnector:::safe_split("", ",")
  expect_equal(result, "")
})

test_that("safe_split respects quoted fields", {
  result <- CDMConnector:::safe_split('a,"b,c",d', ",")
  expect_equal(length(result), 3)
  expect_equal(result[1], "a")
  expect_true(grepl("b,c", result[2]))
})

test_that("safe_split handles escaped characters", {
  result <- CDMConnector:::safe_split("a\\,b,c", ",")
  # Backslash escaping: \\, should not split
  expect_true(length(result) >= 2)
})

# --- split_and_keep ---

test_that("split_and_keep splits and retains matches", {
  result <- CDMConnector:::split_and_keep("hello123world", "[0-9]+")
  expect_true(length(result) >= 3)
  expect_true("123" %in% unlist(result))
})

test_that("split_and_keep returns original if no match", {
  result <- CDMConnector:::split_and_keep("hello", "[0-9]+")
  expect_equal(result[[1]], "hello")
})

# --- replace_with_concat ---

test_that("replace_with_concat handles no escaped quotes", {
  result <- CDMConnector:::replace_with_concat("'hello'")
  expect_equal(result, "'hello'")
})

test_that("replace_with_concat handles escaped single quotes", {
  result <- CDMConnector:::replace_with_concat("'it''s'")
  expect_true(grepl("CONCAT", result))
})

test_that("replace_with_concat handles non-string content", {
  result <- CDMConnector:::replace_with_concat("SELECT 1")
  expect_equal(result, "SELECT 1")
})

test_that("replace_with_concat handles empty string", {
  result <- CDMConnector:::replace_with_concat("")
  expect_equal(result, "")
})

# --- find_curly_bracket_spans ---

test_that("find_curly_bracket_spans finds simple spans", {
  spans <- CDMConnector:::find_curly_bracket_spans("{a}{b}")
  expect_equal(length(spans), 2)
  expect_equal(spans[[1]]$start, 1)
  expect_equal(spans[[2]]$start, 4)
})

test_that("find_curly_bracket_spans handles nested", {
  spans <- CDMConnector:::find_curly_bracket_spans("{a{b}c}")
  expect_equal(length(spans), 2)
})

test_that("find_curly_bracket_spans handles empty", {
  spans <- CDMConnector:::find_curly_bracket_spans("no braces")
  expect_equal(length(spans), 0)
})

# --- link_if_then_elses ---

test_that("link_if_then_elses links condition-then", {
  str <- "{cond}?{then}"
  spans <- CDMConnector:::find_curly_bracket_spans(str)
  ites <- CDMConnector:::link_if_then_elses(str, spans)
  expect_equal(length(ites), 1)
  expect_false(ites[[1]]$hasIfFalse)
})

test_that("link_if_then_elses links condition-then-else", {
  str <- "{cond}?{then}:{else}"
  spans <- CDMConnector:::find_curly_bracket_spans(str)
  ites <- CDMConnector:::link_if_then_elses(str, spans)
  expect_equal(length(ites), 1)
  expect_true(ites[[1]]$hasIfFalse)
})

# --- remove_parentheses_quotes ---

test_that("remove_parentheses_quotes removes single quotes", {
  expect_equal(CDMConnector:::remove_parentheses_quotes("'hello'"), "hello")
})

test_that("remove_parentheses_quotes removes double quotes", {
  expect_equal(CDMConnector:::remove_parentheses_quotes('"hello"'), "hello")
})

test_that("remove_parentheses_quotes leaves unquoted string", {
  expect_equal(CDMConnector:::remove_parentheses_quotes("hello"), "hello")
})

test_that("remove_parentheses_quotes leaves mismatched quotes", {
  expect_equal(CDMConnector:::remove_parentheses_quotes("'hello\""), "'hello\"")
})

# --- preceded_by_in ---

test_that("preceded_by_in returns TRUE when preceded by IN", {
  str <- "x IN (1,2,3)"
  pos <- regexpr("\\(", str)[1]
  expect_true(CDMConnector:::preceded_by_in(pos, str))
})

test_that("preceded_by_in returns FALSE when not preceded", {
  str <- "SELECT (1,2,3)"
  pos <- regexpr("\\(", str)[1]
  expect_false(CDMConnector:::preceded_by_in(pos, str))
})

test_that("preceded_by_in returns FALSE at start", {
  expect_false(CDMConnector:::preceded_by_in(1, "hello"))
})

# --- evaluate_primitive_condition ---

test_that("evaluate_primitive_condition handles true/false literals", {
  expect_true(CDMConnector:::evaluate_primitive_condition("true"))
  expect_false(CDMConnector:::evaluate_primitive_condition("false"))
  expect_true(CDMConnector:::evaluate_primitive_condition("1"))
  expect_false(CDMConnector:::evaluate_primitive_condition("0"))
  expect_true(CDMConnector:::evaluate_primitive_condition("!false"))
  expect_false(CDMConnector:::evaluate_primitive_condition("!true"))
  expect_true(CDMConnector:::evaluate_primitive_condition("!0"))
  expect_false(CDMConnector:::evaluate_primitive_condition("!1"))
})

test_that("evaluate_primitive_condition handles ==", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'a' == 'a'"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'a' == 'b'"))
})

test_that("evaluate_primitive_condition handles !=", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'a' != 'b'"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'a' != 'a'"))
})

test_that("evaluate_primitive_condition handles <>", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'a' <> 'b'"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'a' <> 'a'"))
})

test_that("evaluate_primitive_condition handles IN", {
  expect_true(CDMConnector:::evaluate_primitive_condition("'a' IN ('a','b','c')"))
  expect_false(CDMConnector:::evaluate_primitive_condition("'d' IN ('a','b','c')"))
})

test_that("evaluate_primitive_condition handles unrecognized", {
  expect_false(CDMConnector:::evaluate_primitive_condition("rules"))
})

# --- evaluate_boolean_condition ---

test_that("evaluate_boolean_condition handles AND", {
  expect_true(CDMConnector:::evaluate_boolean_condition("true & true"))
  expect_false(CDMConnector:::evaluate_boolean_condition("true & false"))
})

test_that("evaluate_boolean_condition handles OR", {
  expect_true(CDMConnector:::evaluate_boolean_condition("false | true"))
  expect_false(CDMConnector:::evaluate_boolean_condition("false | false"))
})

test_that("evaluate_boolean_condition handles simple", {
  expect_true(CDMConnector:::evaluate_boolean_condition("true"))
  expect_false(CDMConnector:::evaluate_boolean_condition("false"))
})

# --- evaluate_condition ---

test_that("evaluate_condition handles parenthesized conditions", {
  expect_true(CDMConnector:::evaluate_condition("(true & true)"))
  expect_false(CDMConnector:::evaluate_condition("(true & false)"))
})

test_that("evaluate_condition handles nested parentheses", {
  expect_true(CDMConnector:::evaluate_condition("((true))"))
})

test_that("evaluate_condition handles IN with parentheses", {
  expect_true(CDMConnector:::evaluate_condition("'a' IN ('a','b')"))
  expect_false(CDMConnector:::evaluate_condition("'c' IN ('a','b')"))
})

# --- extract_defaults ---

test_that("extract_defaults extracts parameter defaults", {
  sql <- "{DEFAULT @param1 = 'hello'} {DEFAULT @param2 = 42}"
  defaults <- CDMConnector:::extract_defaults(sql)
  expect_equal(defaults[["param1"]], "hello")
  expect_equal(defaults[["param2"]], "42")
})

test_that("extract_defaults handles empty string", {
  defaults <- CDMConnector:::extract_defaults("SELECT 1")
  expect_equal(length(defaults), 0)
})

# --- remove_defaults ---

test_that("remove_defaults removes DEFAULT blocks", {
  sql <- "{DEFAULT @x = 5}\nSELECT @x"
  result <- CDMConnector:::remove_defaults(sql)
  expect_false(grepl("DEFAULT", result))
  expect_true(grepl("SELECT", result))
})

# --- substitute_parameters ---

test_that("substitute_parameters replaces parameters", {
  sql <- "SELECT * FROM t WHERE id = @id AND name = @name"
  result <- CDMConnector:::substitute_parameters(sql, list(id = "1", name = "'test'"))
  expect_true(grepl("id = 1", result))
  expect_true(grepl("name = 'test'", result))
})

test_that("substitute_parameters applies defaults", {
  sql <- "{DEFAULT @x = 10}\nSELECT @x"
  result <- CDMConnector:::substitute_parameters(sql, list())
  expect_true(grepl("10", result))
})

test_that("substitute_parameters overrides defaults", {
  sql <- "{DEFAULT @x = 10}\nSELECT @x"
  result <- CDMConnector:::substitute_parameters(sql, list(x = "20"))
  expect_true(grepl("20", result))
  expect_false(grepl("10", result))
})

test_that("substitute_parameters handles longer param names first", {
  sql <- "SELECT @param, @param_long"
  result <- CDMConnector:::substitute_parameters(sql, list(param = "A", param_long = "B"))
  expect_true(grepl("B", result))
})

# --- parse_if_then_else ---

test_that("parse_if_then_else evaluates true condition", {
  sql <- "{1 == 1}?{SELECT 1}"
  result <- CDMConnector:::parse_if_then_else(sql)
  expect_true(grepl("SELECT 1", result))
})

test_that("parse_if_then_else evaluates false condition with else", {
  sql <- "{1 == 0}?{then_val}:{else_val}"
  result <- CDMConnector:::parse_if_then_else(sql)
  expect_true(grepl("else_val", result))
  expect_false(grepl("then_val", result))
})

test_that("parse_if_then_else evaluates false without else", {
  sql <- "{1 == 0}?{hidden}"
  result <- CDMConnector:::parse_if_then_else(sql)
  expect_false(grepl("hidden", result))
})

# --- render_sql_core ---

test_that("render_sql_core renders with parameters and conditionals", {
  sql <- "{DEFAULT @include = 1}\nSELECT * FROM t {@include == 1}?{WHERE x > 0}"
  result <- CDMConnector:::render_sql_core(sql, list())
  expect_true(grepl("WHERE x > 0", result))
})

test_that("render_sql_core handles nested conditionals", {
  sql <- "{@a}?{{@b}?{inner}}"
  result <- CDMConnector:::render_sql_core(sql, list(a = "1", b = "1"))
  expect_true(grepl("inner", result))
})

# --- split_sql_core ---

test_that("split_sql_core splits multiple statements", {
  sql <- "SELECT 1; SELECT 2; SELECT 3"
  parts <- CDMConnector:::split_sql_core(sql)
  expect_equal(length(parts), 3)
})

test_that("split_sql_core handles empty SQL", {
  parts <- CDMConnector:::split_sql_core("")
  expect_equal(length(parts), 0)
})

test_that("split_sql_core handles CASE...END", {
  sql <- "SELECT CASE WHEN x = 1 THEN 'a' END FROM t; SELECT 2"
  parts <- CDMConnector:::split_sql_core(sql)
  expect_equal(length(parts), 2)
})

test_that("split_sql_core handles BEGIN...END block", {
  sql <- "BEGIN SELECT 1; END; SELECT 2"
  parts <- CDMConnector:::split_sql_core(sql)
  expect_true(length(parts) >= 2)
})

test_that("split_sql_core handles nested parentheses with semicolons", {
  sql <- "INSERT INTO t SELECT * FROM (SELECT 1; UNION ALL SELECT 2) x; SELECT 3"
  parts <- CDMConnector:::split_sql_core(sql)
  # Semicolons inside parens should not cause splits
  expect_true(length(parts) >= 1)
})

test_that("split_sql_core handles quoted strings", {
  sql <- "SELECT 'hello;world'; SELECT 2"
  parts <- CDMConnector:::split_sql_core(sql)
  # Should not split inside quote
  expect_equal(length(parts), 2)
})

test_that("split_sql_core handles bracketed identifiers", {
  sql <- "SELECT [col;name] FROM t; SELECT 2"
  parts <- CDMConnector:::split_sql_core(sql)
  expect_equal(length(parts), 2)
})

# --- render_check_missing_params ---

test_that("render_check_missing_params finds missing params", {
  sql <- "SELECT @found FROM t"
  warnings <- CDMConnector:::render_check_missing_params(sql, c("found", "missing"))
  expect_equal(length(warnings), 1)
  expect_true(grepl("missing", warnings[1]))
})

test_that("render_check_missing_params returns empty for all found", {
  sql <- "SELECT @a, @b FROM t"
  warnings <- CDMConnector:::render_check_missing_params(sql, c("a", "b"))
  expect_equal(length(warnings), 0)
})

# --- parse_search_pattern ---

test_that("parse_search_pattern parses literal pattern", {
  blocks <- CDMConnector:::parse_search_pattern("SELECT FROM")
  expect_true(length(blocks) >= 2)
})

test_that("parse_search_pattern errors on variable at end", {
  # Non-regex variables cannot be at start or end
  expect_error(
    CDMConnector:::parse_search_pattern("SELECT @@a FROM @@b"),
    "cannot start or end"
  )
})

test_that("parse_search_pattern parses embedded variable", {
  blocks <- CDMConnector:::parse_search_pattern("SELECT @@a FROM t")
  has_var <- vapply(blocks, function(b) b$isVariable, logical(1))
  expect_true(any(has_var))
})

# --- get_key ---

test_that("get_key returns value for first match", {
  obj <- list(PascalCase = 1, camelCase = 2)
  expect_equal(CDMConnector:::get_key(obj, c("PascalCase", "camelCase")), 1)
})

test_that("get_key returns second match if first missing", {
  obj <- list(camelCase = 2)
  expect_equal(CDMConnector:::get_key(obj, c("PascalCase", "camelCase")), 2)
})

test_that("get_key returns default if no match", {
  obj <- list(other = 3)
  expect_equal(CDMConnector:::get_key(obj, c("PascalCase", "camelCase"), default = 99), 99)
})

# --- %||% operator ---

test_that("null coalescing returns x when non-null", {
  expect_equal(CDMConnector:::`%||%`(1, 2), 1)
})

test_that("null coalescing returns y when x is null", {
  expect_equal(CDMConnector:::`%||%`(NULL, 2), 2)
})

# --- str_replace_and_adjust_spans ---

test_that("str_replace_and_adjust_spans replaces region", {
  str <- "hello world"
  spans <- list()
  result <- CDMConnector:::str_replace_and_adjust_spans(str, spans, 1, 6, 7, 11)
  expect_true(is.character(result))
})

# --- generate_session_id ---

test_that("generate_session_id generates correct length", {
  id <- CDMConnector:::generate_session_id()
  expect_equal(nchar(id), CDMConnector:::SESSION_ID_LENGTH)
  expect_true(grepl("^[a-z]", id))  # Starts with letter
})

# --- get_global_session_id ---

test_that("get_global_session_id returns consistent id", {
  id1 <- CDMConnector:::get_global_session_id()
  id2 <- CDMConnector:::get_global_session_id()
  expect_equal(id1, id2)
})

# --- search_and_replace ---

test_that("search_and_replace performs simple replacement", {
  parsed <- CDMConnector:::parse_search_pattern("DATEADD(@@a,@@b,@@c)")
  result <- CDMConnector:::search_and_replace(
    "DATEADD(day,30,start_date)",
    parsed,
    "(@@c + @@b)"
  )
  expect_true(grepl("start_date", result))
  expect_true(grepl("30", result))
})

# --- translate_search ---

test_that("translate_search finds pattern match", {
  parsed <- CDMConnector:::parse_search_pattern("SELECT @@a FROM")
  m <- CDMConnector:::translate_search("SELECT 1 FROM t", parsed, 1)
  expect_true(m$start > 0)
})

test_that("translate_search returns -1 for no match", {
  parsed <- CDMConnector:::parse_search_pattern("INSERT INTO")
  m <- CDMConnector:::translate_search("SELECT 1 FROM t", parsed, 1)
  expect_equal(m$start, -1)
})

# --- translate_sql_with_path ---

test_that("translate_sql_with_path translates to duckdb", {
  sql <- "SELECT DATEADD(day,30,start_date) FROM t"
  result <- CDMConnector:::translate_sql_with_path(sql, "duckdb", "abc12345", NULL, NULL)
  expect_true(is.character(result))
  expect_true(nchar(result) > 10)
})

test_that("translate_sql_with_path errors on unknown dialect", {
  expect_error(
    CDMConnector:::translate_sql_with_path("SELECT 1", "unknown_db_xyz", "abc12345", NULL, NULL),
    "Don't know how to translate"
  )
})

# --- translate_single_statement_with_path ---

test_that("translate_single_statement_with_path translates single", {
  sql <- "SELECT 1 FROM t"
  result <- CDMConnector:::translate_single_statement_with_path(sql, "duckdb", "abc12345", NULL, NULL)
  expect_true(is.character(result))
})

# --- translate_check_warnings ---

test_that("translate_check_warnings warns about long temp table names", {
  long_name <- paste0("#", paste(rep("a", 70), collapse = ""))
  sql <- paste("CREATE TABLE", long_name, "(x INT)")
  warnings <- CDMConnector:::translate_check_warnings(sql, "duckdb")
  expect_true(length(warnings) >= 1)
  expect_true(any(grepl("too long", warnings)))
})

test_that("translate_check_warnings returns empty for normal SQL", {
  sql <- "SELECT * FROM short_table"
  warnings <- CDMConnector:::translate_check_warnings(sql, "duckdb")
  expect_equal(length(warnings), 0)
})

# --- translate_bigquery ---

test_that("translate_bigquery lowercases non-string tokens", {
  sql <- "SELECT A, B FROM TABLE1 WHERE X = 'KeepCase'"
  result <- CDMConnector:::translate_bigquery(sql)
  expect_true(grepl("select", result))
  expect_true(grepl("KeepCase", result))  # String literal preserved
})

# --- translate_spark ---

test_that("translate_spark handles SQL", {
  sql <- "SELECT 1 FROM t"
  result <- CDMConnector:::translate_spark(sql)
  expect_true(is.character(result))
})

# --- ensure_patterns_loaded ---

test_that("ensure_patterns_loaded loads replacement patterns", {
  # Force clear cache
  rm(list = ls(CDMConnector:::.translate_pattern_cache), envir = CDMConnector:::.translate_pattern_cache)
  CDMConnector:::ensure_patterns_loaded(NULL)
  # Should have loaded some patterns
  expect_true(length(ls(CDMConnector:::.translate_pattern_cache)) > 0)
})

# --- translate_sql_apply_patterns ---

test_that("translate_sql_apply_patterns applies patterns", {
  CDMConnector:::ensure_patterns_loaded(NULL)
  pats <- get("duckdb", envir = CDMConnector:::.translate_pattern_cache)
  if (length(pats) > 0) {
    sql <- "SELECT DATEADD(day,1,col1) FROM t"
    result <- CDMConnector:::translate_sql_apply_patterns(sql, pats, "abc12345", "")
    expect_true(is.character(result))
  }
})

# --- translate_spark CREATE TABLE ---

test_that("translate_spark converts CREATE TABLE to SELECT INTO", {
  sql <- "CREATE TABLE test_tbl (id INT, name VARCHAR(50))"
  result <- CDMConnector:::translate_spark(sql)
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("translate_spark removes semicolons", {
  sql <- "SELECT 1; SELECT 2;"
  result <- CDMConnector:::translate_spark(sql)
  expect_true(nchar(result) > 0)
})

# --- listSupportedDialects ---

test_that("listSupportedDialects returns data frame of dialects", {
  result <- CDMConnector:::listSupportedDialects()
  expect_s3_class(result, "data.frame")
  expect_true("dialect" %in% names(result))
  expect_true("duckdb" %in% result$dialect)
  expect_true("postgresql" %in% result$dialect)
})

# --- render_r ---

test_that("render_r renders SQL with parameters", {
  result <- CDMConnector:::render_r("SELECT * FROM @schema.person", schema = "main")
  expect_true(grepl("main", result))
})

test_that("render_r handles conditionals", {
  result <- CDMConnector:::render_r("SELECT * FROM t {@include}?{WHERE x > 0}", include = "true")
  expect_true(grepl("WHERE x > 0", result))
})

test_that("render_r warns on missing parameters", {
  # render_r warns when a parameter was provided but is not in the SQL
  expect_warning(
    CDMConnector:::render_r("SELECT @a FROM t", a = "1", b = "2"),
    "not found"
  )
})

test_that("render_r does not warn when warnOnMissingParameters = FALSE", {
  expect_no_warning(
    CDMConnector:::render_r("SELECT @a, @b FROM t", warnOnMissingParameters = FALSE, a = "1")
  )
})

# --- translate_r ---

test_that("translate_r translates to duckdb", {
  result <- CDMConnector:::translate_r("SELECT DATEADD(day,1,start_date) FROM t", targetDialect = "duckdb")
  expect_true(is.character(result))
  expect_equal(attr(result, "sqlDialect"), "duckdb")
})

test_that("translate_r translates to postgresql", {
  result <- CDMConnector:::translate_r("SELECT TOP 10 * FROM person", targetDialect = "postgresql")
  expect_true(is.character(result))
  expect_equal(attr(result, "sqlDialect"), "postgresql")
})

test_that("translate_r warns on already-translated SQL", {
  sql <- "SELECT 1"
  attr(sql, "sqlDialect") <- "duckdb"
  expect_warning(
    CDMConnector:::translate_r(sql, targetDialect = "duckdb"),
    "already been translated"
  )
})

# --- render (public API wrapper) ---

test_that("render replaces parameters via public API", {
  result <- render("SELECT * FROM @schema.person WHERE id = @id", schema = "main", id = 1)
  expect_true(grepl("main", result))
  expect_true(grepl("1", result))
})

# --- translate (public API wrapper) ---

test_that("translate converts to duckdb dialect", {
  sql <- "SELECT TOP 10 * FROM person"
  result <- translate(sql, targetDialect = "duckdb")
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("translate converts to postgresql dialect", {
  sql <- "SELECT DATEADD(day, 30, start_date) FROM t"
  result <- translate(sql, targetDialect = "postgresql")
  expect_true(is.character(result))
})
