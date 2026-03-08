# Copyright 2025 Observational Health Data Sciences and Informatics
# SqlRender - internal string/SQL utilities (pure R port of StringUtils.java)
# Loaded first (00_) so tokenize_sql etc. are available to other modules.

# Token: start, end (1-based in R), text, inQuotes
# Uses stringi (C-backed) for fast vectorized char extraction, regex, and substring.
tokenize_sql <- function(sql) {
  sql <- as.character(sql)
  n <- stringi::stri_length(sql)
  if (n == 0) return(list())
  # Single C-level call to get all characters (avoids strsplit + many small strings)
  chars <- stringi::stri_sub(sql, seq_len(n), length = 1)
  # One vectorized regex pass for alnum/underscore/at and for whitespace (avoids n x grepl)
  is_alnum <- stringi::stri_detect_regex(chars, "[A-Za-z0-9_@]")
  is_whitespace <- stringi::stri_detect_regex(chars, "[ \t\n\r]")
  tokens <- list()
  start <- 1
  cursor <- 1
  comment_type1 <- FALSE  # -- to EOL
  comment_type2 <- FALSE  # /* */
  in_single <- FALSE
  in_double <- FALSE
  hint_keyword <- "hint"

  while (cursor <= n) {
    ch <- chars[cursor]
    if (comment_type1) {
      if (ch == "\n") {
        comment_type1 <- FALSE
        start <- cursor + 1
      }
      cursor <- cursor + 1
      next
    }
    if (comment_type2) {
      if (ch == "/" && cursor > 1 && chars[cursor - 1] == "*") {
        comment_type2 <- FALSE
        start <- cursor + 1
      }
      cursor <- cursor + 1
      next
    }
    if (!is_alnum[cursor]) {
      if (cursor > start) {
        # C-level substring instead of paste(chars[start:(cursor-1)], collapse="")
        text <- stringi::stri_sub(sql, start, cursor - 1)
        tokens[[length(tokens) + 1]] <- list(
          start = start, end = cursor, text = text,
          inQuotes = in_single || in_double
        )
      }
      # Check for -- comment (but not --HINT: 4 chars at cursor+2..cursor+5)
      if (ch == "-" && cursor < n && chars[cursor + 1] == "-" &&
          !in_single && !in_double) {
        if (cursor + 5 > n || stringi::stri_sub(sql, cursor + 2, cursor + 5) != hint_keyword) {
          comment_type1 <- TRUE
        } else {
          # --HINT: emit "--" as one token so statement starts at cursor
          tokens[[length(tokens) + 1]] <- list(
            start = cursor, end = cursor + 2, text = "--",
            inQuotes = in_single || in_double
          )
          start <- cursor + 2
          cursor <- cursor + 1  # skip second '-' so we don't add it again
        }
      } else if (ch == "/" && cursor < n && chars[cursor + 1] == "*" &&
          !in_single && !in_double) {
        comment_type2 <- TRUE
      } else if (!is_whitespace[cursor]) {
        tokens[[length(tokens) + 1]] <- list(
          start = cursor, end = cursor + 1, text = ch,
          inQuotes = in_single || in_double
        )
        if (ch == "'" && !in_double) in_single <- !in_single
        if (ch == '"' && !in_single) in_double <- !in_double
      }
      start <- cursor + 1
    }
    cursor <- cursor + 1
  }
  if (cursor > start && !comment_type1 && !comment_type2) {
    text <- stringi::stri_sub(sql, start, n)
    tokens[[length(tokens) + 1]] <- list(
      start = start, end = n + 1, text = text,
      inQuotes = in_single || in_double
    )
  }
  tokens
}

replace_char_at <- function(string, pos, ch) {
  stringi::stri_sub_replace(string, from = pos, to = pos, value = ch)
}

str_replace_region <- function(string, start, end, replacement) {
  stringi::stri_sub_replace(string, from = start, to = end, value = replacement)
}

# Replace all non-overlapping occurrences of search with replace in result (C-backed)
str_replace_all <- function(result, search, replace) {
  stringi::stri_replace_all_fixed(result, search, replace)
}

token_is_identifier <- function(text) {
  stringi::stri_detect_regex(text, "^[A-Za-z0-9_]+$")
}

# Safe CSV line split respecting quoted fields (uses stringi for substring extraction)
safe_split <- function(string, delimiter) {
  n <- stringi::stri_length(string)
  if (n == 0) return("")
  chars <- stringi::stri_sub(string, seq_len(n), length = 1)
  result <- character(0)
  literal <- FALSE
  escape <- FALSE
  startpos <- 1
  i <- 1
  while (i <= n) {
    currentchar <- chars[i]
    if (currentchar == '"' && !escape) literal <- !literal
    if (!literal && currentchar == delimiter && !escape) {
      result <- c(result, stringi::stri_sub(string, startpos, i - 1))
      startpos <- i + 1
    }
    if (currentchar == "\\") {
      escape <- !escape
    } else {
      escape <- FALSE
    }
    i <- i + 1
  }
  result <- c(result, stringi::stri_sub(string, startpos, n))
  result
}

# Split string by regex but keep the matches (C-backed locate + substring)
split_and_keep <- function(val, regex) {
  m <- stringi::stri_locate_all_regex(val, regex)[[1]]
  if (is.na(m[1L, 1L])) return(list(val))
  n <- nrow(m)
  result <- list()
  pos <- 1
  len_val <- stringi::stri_length(val)
  for (i in seq_len(n)) {
    start <- m[i, 1]
    end <- m[i, 2]
    if (start > pos) {
      result <- c(result, list(stringi::stri_sub(val, pos, start - 1)))
    }
    result <- c(result, list(stringi::stri_sub(val, start, end)))
    pos <- end + 1
  }
  if (pos <= len_val) {
    result <- c(result, list(stringi::stri_sub(val, pos, len_val)))
  }
  result
}

# Replace string literals containing '' with CONCAT('a','\'','b') style (for Impala/BigQuery/Spark)
# Uses stringi for length, char extraction, substring, detect, split, replace, and join.
# Tracks segments when we see escaped quote ('') so we can build CONCAT even after collapsing.
replace_with_concat <- function(val) {
  n <- stringi::stri_length(val)
  if (n == 0) return(val)
  chars <- stringi::stri_sub(val, seq_len(n), length = 1)
  result <- character(0)
  i <- 1
  while (i <= n) {
    if (chars[i] %in% c("'", '"')) {
      quote_char <- chars[i]
      i <- i + 1
      segments <- character(0)
      current_segment <- ""
      while (i <= n) {
        if (chars[i] == quote_char) {
          if (i < n && chars[i + 1] == quote_char) {
            segments <- c(segments, current_segment)
            current_segment <- ""
            i <- i + 2
            next
          } else {
            i <- i + 1
            break
          }
        }
        current_segment <- stringi::stri_join(current_segment, chars[i])
        i <- i + 1
      }
      segments <- c(segments, current_segment)
      if (length(segments) > 1) {
        concat_parts <- character(0)
        for (k in seq_along(segments)) {
          inner <- segments[k]
          inner <- stringi::stri_replace_all_fixed(inner, "\\", "\\\\")
          inner <- stringi::stri_replace_all_fixed(inner, "\"", "\\042")
          inner <- stringi::stri_replace_all_fixed(inner, "/", "\\/")
          concat_parts <- c(concat_parts, stringi::stri_join("'", inner, "'"))
          if (k < length(segments)) concat_parts <- c(concat_parts, "'\\047'")
        }
        result <- c(result, stringi::stri_join("CONCAT(", stringi::stri_join(concat_parts, collapse = ","), ")"))
      } else {
        full_lit <- stringi::stri_join(quote_char, segments[1], quote_char)
        result <- c(result, full_lit)
      }
      next
    }
    result <- c(result, chars[i])
    i <- i + 1
  }
  stringi::stri_join(result, collapse = "")
}
#' Package utilities
#'
#' @name package_utilities_section
#' @keywords internal
#' @noRd
NULL

#' Null coalescing operator
#' @name or-or
#' @param x Left value
#' @param y Right value (used when x is NULL)
#' @return x or y
#' @keywords internal
#' @noRd
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Get value from list with fallback keys (PascalCase/camelCase)
#' @param obj List
#' @param keys Character vector of possible key names
#' @param default Default value if not found
#' @return Value or default
#' @keywords internal
#' @noRd
get_key <- function(obj, keys, default = NULL) {
  nms <- names(obj)
  for (k in keys) {
    if (k %in% nms && !is.null(obj[[k]])) return(obj[[k]])
  }
  default
}
# Copyright 2025 Observational Health Data Sciences and Informatics
# SqlRender - parameterized SQL rendering (pure R port of SqlRender.java)

# Find matching curly bracket spans (start, end) - 1-based, end exclusive in Java so end is past last char
find_curly_bracket_spans <- function(str) {
  chars <- strsplit(str, "")[[1]]
  n <- length(chars)
  starts <- integer(0)
  spans <- list()
  for (i in seq_len(n)) {
    if (chars[i] == "{") {
      starts <- c(starts, i)
    } else if (chars[i] == "}" && length(starts) > 0) {
      start <- starts[length(starts)]
      starts <- starts[-length(starts)]
      spans[[length(spans) + 1]] <- list(start = start, end = i + 1, valid = TRUE)
    }
  }
  spans
}

# Link {cond}?{then}:{else} - find condition, ifTrue, ifFalse spans
link_if_then_elses <- function(str, spans) {
  n <- length(spans)
  ifthen <- list()
  for (i in seq_len(max(0, n - 1))) {
    for (j in (i + 1):n) {
      if (j > n) next
      if (spans[[j]]$start > spans[[i]]$end) {
        in_between <- trimws(substr(str, spans[[i]]$end, spans[[j]]$start - 1))
        if (in_between == "?") {
          ite <- list(condition = spans[[i]], ifTrue = spans[[j]], hasIfFalse = FALSE, ifFalse = NULL)
          for (k in seq(j + 1, n)) {
            if (k > n) break
            if (spans[[k]]$start > spans[[j]]$end) {
              in_between2 <- trimws(substr(str, spans[[j]]$end, spans[[k]]$start - 1))
              if (in_between2 == ":") {
                ite$ifFalse <- spans[[k]]
                ite$hasIfFalse <- TRUE
                break
              }
            }
          }
          # end of block (1-based inclusive)
          ite$block_end <- if (ite$hasIfFalse) ite$ifFalse$end else spans[[j]]$end
          ifthen[[length(ifthen) + 1]] <- ite
        }
        break
      }
    }
  }
  ifthen
}

# Remove outer quotes if present
remove_parentheses_quotes <- function(s) {
  n <- nchar(s)
  if (n > 1) {
    c1 <- substr(s, 1, 1)
    cn <- substr(s, n, n)
    if ((c1 == "'" && cn == "'") || (c1 == '"' && cn == '"')) {
      return(substr(s, 2, n - 1))
    }
  }
  s
}

# Check if position is preceded by " in " (case insensitive) - start is 1-based
preceded_by_in <- function(start, str) {
  str <- tolower(str)
  if (start < 3) return(FALSE)
  matched <- 0
  for (i in (start - 1):1) {
    ch <- substr(str, i, i)
    if (!grepl("[ \t\n\r]", ch)) {
      if (matched == 0 && ch == "n") matched <- 1
      else if (matched == 1 && ch == "i") matched <- 2
      else return(FALSE)
    } else if (matched == 2) return(TRUE)
  }
  FALSE
}

# Evaluate primitive condition (no & or |)
evaluate_primitive_condition <- function(str) {
  str <- trimws(str)
  str_lc <- tolower(str)
  if (str_lc %in% c("false", "0", "!true", "!1")) return(FALSE)
  if (str_lc %in% c("true", "1", "!false", "!0")) return(TRUE)
  if (grepl("==", str, fixed = TRUE)) {
    parts <- strsplit(str, "==", fixed = TRUE)[[1]]
    left <- remove_parentheses_quotes(trimws(parts[1]))
    right <- remove_parentheses_quotes(trimws(parts[2]))
    return(left == right)
  }
  if (grepl("!=", str, fixed = TRUE)) {
    parts <- strsplit(str, "!=", fixed = TRUE)[[1]]
    left <- remove_parentheses_quotes(trimws(parts[1]))
    right <- remove_parentheses_quotes(trimws(parts[2]))
    return(left != right)
  }
  if (grepl("<>", str, fixed = TRUE)) {
    parts <- strsplit(str, "<>", fixed = TRUE)[[1]]
    left <- remove_parentheses_quotes(trimws(parts[1]))
    right <- remove_parentheses_quotes(trimws(parts[2]))
    return(left != right)
  }
  # " in " pattern (case-insensitive: strsplit has no ignore.case, so find position and split)
  if (grepl(" in ", str_lc, fixed = TRUE)) {
    pos <- regexpr(" in ", str_lc, fixed = TRUE)[1]
    if (pos <= 0) stop(sprintf('Error parsing boolean condition: "%s"', str))
    left <- remove_parentheses_quotes(trimws(substr(str, 1, pos - 1)))
    right <- trimws(substr(str, pos + 4, nchar(str)))
    n <- nchar(right)
    if (n >= 2 && substr(right, 1, 1) == "(" && substr(right, n, n) == ")") {
      right <- substr(right, 2, n - 1)
      parts_right <- strsplit(right, ",", fixed = TRUE)[[1]]
      for (part in parts_right) {
        if (left == remove_parentheses_quotes(trimws(part))) return(TRUE)
      }
      return(FALSE)
    }
  }
  # Unrecognized (e.g. stray token from SQL like "rules" from total_rules): treat as FALSE
  FALSE
}

# Evaluate boolean with & and |
evaluate_boolean_condition <- function(str) {
  str <- trimws(str)
  if (grepl("&", str, fixed = TRUE)) {
    parts <- strsplit(str, "&", fixed = TRUE)[[1]]
    for (part in parts) {
      if (!evaluate_primitive_condition(trimws(part))) return(FALSE)
    }
    return(TRUE)
  }
  if (grepl("|", str, fixed = TRUE)) {
    parts <- strsplit(str, "|", fixed = TRUE)[[1]]
    for (part in parts) {
      if (evaluate_primitive_condition(trimws(part))) return(TRUE)
    }
    return(FALSE)
  }
  evaluate_primitive_condition(str)
}

# Evaluate condition with parentheses (innermost first)
evaluate_condition <- function(str) {
  str <- trimws(str)
  # Find parentheses pairs (simple: match ( and ) by scanning)
  repeat {
    open <- regexpr("(", str, fixed = TRUE)[1]
    if (open <= 0) break
    depth <- 1
    close <- open
    chars <- strsplit(str, "")[[1]]
    for (i in (open + 1):length(chars)) {
      if (chars[i] == "(") depth <- depth + 1
      else if (chars[i] == ")") {
        depth <- depth - 1
        if (depth == 0) { close <- i; break }
      }
    }
    inner <- substr(str, open + 1, close - 1)
    if (!preceded_by_in(open, str)) {
      val <- evaluate_condition(inner)
      repl <- if (val) "1" else "0"
      str <- paste0(substr(str, 1, open - 1), repl, substr(str, close + 1, nchar(str)))
    } else {
      # "x IN (...)" - find the left operand (last token before " in "), evaluate only that IN and replace with 1/0
      before_in <- substr(str, 1, open - 5)
      last_token <- trimws(sub(".*[ \t&|]([^ \t&|]+)[ \t]*$", "\\1", paste0(" ", before_in)))
      if (nchar(last_token) == 0) last_token <- trimws(before_in)
      start_replace <- regexpr(last_token, before_in, fixed = TRUE)[1]
      if (start_replace <= 0) start_replace <- 1
      expr <- paste0(last_token, " IN (", inner, ")")
      val <- evaluate_primitive_condition(expr)
      repl <- if (val) "1" else "0"
      str <- paste0(substr(str, 1, start_replace - 1), repl, substr(str, close + 1, nchar(str)))
    }
  }
  evaluate_boolean_condition(str)
}

# Replace region in str and adjust span positions (spans list modified in place where needed)
str_replace_and_adjust_spans <- function(str, spans, to_replace_start, to_replace_end, replace_with_start, replace_with_end) {
  replace_with_string <- substr(str, replace_with_start, replace_with_end)
  new_str <- paste0(
    substr(str, 1, to_replace_start - 1),
    replace_with_string,
    substr(str, to_replace_end, nchar(str))
  )
  delta <- (to_replace_start - to_replace_end) + nchar(replace_with_string)
  for (sp in spans) {
    if (!isTRUE(sp$valid)) next
    if (sp$start > to_replace_start) {
      if (sp$start >= replace_with_start && sp$start < replace_with_end) {
        d <- to_replace_start - replace_with_start
        sp$start <- sp$start + d
        sp$end <- sp$end + d
      } else if (sp$start > to_replace_end) {
        sp$start <- sp$start + delta
        sp$end <- sp$end + delta
      } else {
        sp$valid <- FALSE
      }
    } else if (sp$end > to_replace_end) {
      sp$end <- sp$end + delta
    }
  }
  new_str
}

# Extract {DEFAULT @param = value} into named list
extract_defaults <- function(str) {
  defaults <- list()
  pre <- "{DEFAULT "
  post <- "}"
  default_end <- 0
  repeat {
    default_start <- regexpr(pre, str, fixed = TRUE)[1]
    if (default_start <= 0) break
    default_start <- default_start + nchar(pre)
    default_end <- regexpr(post, substr(str, default_start, nchar(str)), fixed = TRUE)[1]
    if (default_end <= 0) break
    default_end <- default_start + default_end - 2
    span <- substr(str, default_start, default_end)
    eq <- regexpr("=", span, fixed = TRUE)[1]
    if (eq <= 0) { default_end <- default_end + 2; next }
    parameter <- trimws(substr(span, 1, eq - 1))
    if (nchar(parameter) > 0 && substr(parameter, 1, 1) == "@") {
      parameter <- substr(parameter, 2, nchar(parameter))
    }
    default_value <- trimws(substr(span, eq + 1, nchar(span)))
    default_value <- remove_parentheses_quotes(default_value)
    defaults[[parameter]] <- default_value
    default_end <- default_end + 2
    str <- substr(str, default_end, nchar(str))
  }
  defaults
}

remove_defaults <- function(string) {
  gsub("\\{DEFAULT[^}]*\\}\\s*\n?", "", string)
}

# Escape $ for replacement (only needed when replacement is used in regex; we use fixed=TRUE so no-op)
escape_dollar_sign <- function(s) s

# Substitute @param with values; sort params by length descending so longer names win
substitute_parameters <- function(string, parameter_to_value) {
  defaults <- extract_defaults(string)
  string <- remove_defaults(string)
  for (nm in names(defaults)) {
    if (!nm %in% names(parameter_to_value)) {
      parameter_to_value[[nm]] <- defaults[[nm]]
    }
  }
  # Sort by name length descending
  nms <- names(parameter_to_value)
  if (length(nms) > 0) {
    nms <- nms[order(-nchar(nms))]
    for (key in nms) {
      value <- parameter_to_value[[key]]
      string <- gsub(paste0("@", key), escape_dollar_sign(value), string, fixed = TRUE)
    }
  }
  string
}

# Parse if-then-else and evaluate (single pass, spans adjusted in place)
parse_if_then_else <- function(str) {
  spans <- find_curly_bracket_spans(str)
  ifthen_list <- link_if_then_elses(str, spans)
  result <- str
  for (ite in ifthen_list) {
    if (!isTRUE(ite$condition$valid)) next
    cond_str <- substr(result, ite$condition$start, ite$condition$end - 1)
    cond_str <- substr(cond_str, 2, nchar(cond_str) - 1)  # strip { }
    if (evaluate_condition(cond_str)) {
      repl <- substr(result, ite$ifTrue$start + 1, ite$ifTrue$end - 2)
      result <- str_replace_and_adjust_spans(
        result, spans,
        ite$condition$start, ite$block_end,
        ite$ifTrue$start + 1, ite$ifTrue$end - 2
      )
    } else {
      if (ite$hasIfFalse) {
        repl <- substr(result, ite$ifFalse$start + 1, ite$ifFalse$end - 2)
        result <- str_replace_and_adjust_spans(
          result, spans,
          ite$condition$start, ite$block_end,
          ite$ifFalse$start + 1, ite$ifFalse$end - 2
        )
      } else {
        result <- str_replace_and_adjust_spans(
          result, spans,
          ite$condition$start, ite$block_end,
          1, 0
        )
      }
    }
  }
  result
}

# Main render: substitute then parse if-then-else (repeat until no more ? : to handle nesting)
render_sql_core <- function(str, parameter_to_value) {
  result <- substitute_parameters(str, parameter_to_value)
  repeat {
    prev <- result
    result <- parse_if_then_else(result)
    if (identical(result, prev)) break
  }
  result
}

# Check which parameters were not found in SQL
render_check_missing_params <- function(sql, parameters) {
  warnings <- character(0)
  for (param in parameters) {
    if (!grepl(paste0("@", param), sql, fixed = TRUE)) {
      warnings <- c(warnings, sprintf("Parameter '%s' not found in SQL", param))
    }
  }
  warnings
}
# Copyright 2025 Observational Health Data Sciences and Informatics
# SqlRender - split SQL into statements (pure R port of SqlSplit.java)

split_sql_core <- function(sql) {
  sql <- as.character(sql)
  tokens <- tokenize_sql(tolower(sql))
  n <- length(tokens)
  if (n == 0) return(character(0))
  parts <- character(0)
  nest_stack <- character(0)
  last_pop <- ""
  start_idx <- 1
  cursor <- 1
  quote <- FALSE
  bracket <- FALSE
  quote_text <- ""

  while (cursor <= n) {
    token <- tokens[[cursor]]
    text <- token$text
    if (quote) {
      if (text == quote_text) quote <- FALSE
      cursor <- cursor + 1
      next
    }
    if (bracket) {
      if (text == "]") bracket <- FALSE
      cursor <- cursor + 1
      next
    }
    if (text %in% c("'", '"')) {
      quote <- TRUE
      quote_text <- text
      cursor <- cursor + 1
      next
    }
    if (text == "[") {
      bracket <- TRUE
      cursor <- cursor + 1
      next
    }
    # Track parentheses so we do not split on ";" inside ( e.g. INSERT ... FROM ( subquery ) ; )
    if (text == "(") {
      nest_stack <- c(nest_stack, "(")
      cursor <- cursor + 1
      next
    }
    if (text == ")") {
      if (length(nest_stack) > 0 && nest_stack[length(nest_stack)] == "(") {
        nest_stack <- nest_stack[-length(nest_stack)]
      }
      cursor <- cursor + 1
      next
    }
    if (text %in% c("begin", "case")) {
      nest_stack <- c(nest_stack, text)
      cursor <- cursor + 1
      next
    }
    if (text == "end") {
      # end but not "end if"
      if (cursor == n || tokens[[cursor + 1]]$text != "if") {
        if (length(nest_stack) > 0) {
          last_pop <- nest_stack[length(nest_stack)]
          nest_stack <- nest_stack[-length(nest_stack)]
        }
      }
      cursor <- cursor + 1
      next
    }
    if (length(nest_stack) == 0 && text == ";") {
      start_token <- tokens[[start_idx]]
      if (cursor == 1 || (tokens[[cursor - 1]]$text == "end" && last_pop == "begin")) {
        part <- substr(sql, start_token$start, token$end - 1)
      } else {
        part <- substr(sql, start_token$start, token$start - 1)
      }
      parts <- c(parts, part)
      start_idx <- cursor + 1
    }
    cursor <- cursor + 1
  }
  if (start_idx <= n) {
    start_token <- tokens[[start_idx]]
    end_token <- tokens[[n]]
    part <- substr(sql, start_token$start, end_token$end - 1)
    parts <- c(parts, part)
  }
  trimws(parts)
}
# Copyright 2025 Observational Health Data Sciences and Informatics
# SqlRender - SQL dialect translation (pure R port of SqlTranslate.java)

SESSION_ID_LENGTH <- 8
MAX_TABLE_NAME_LENGTH <- 63
# Safety: prevent runaway growth or excessive iterations during pattern replacement
MAX_TRANSLATE_ITERATIONS <- 1000L
MAX_SQL_LENGTH_TRANSLATE <- 500000L
# Max input length for regex matching to avoid catastrophic backtracking
MAX_REGEX_INPUT_LENGTH <- 100000L

# Cached replacement patterns: list of target_dialect -> list of list(parsed, replacement_template, pattern)
.translate_pattern_cache <- new.env()

# Parse search pattern into blocks (literal or variable @@x or @@(regex)var)
parse_search_pattern <- function(pattern) {
  pattern_lower <- tolower(pattern)
  tokens <- tokenize_sql(pattern_lower)
  blocks <- list()
  i <- 1
  n <- length(tokens)
  while (i <= n) {
    tok <- tokens[[i]]
    block <- list(start = tok$start, end = tok$end, text = tok$text, isVariable = FALSE, regEx = NULL)
    if (nchar(tok$text) > 2 && substr(tok$text, 1, 1) == "@") {
      block$isVariable <- TRUE
    }
    if (tok$text == "@@" && i + 2 <= n && tokens[[i + 1]]$text == "(") {
      # Regex variable: @@(regex)varname
      escape <- FALSE
      nesting <- 0
      j <- i + 2
      while (j <= n) {
        tj <- tokens[[j]]$text
        if (escape) {
          escape <- FALSE
        } else if (tj == "\\") {
          escape <- TRUE
        } else if (tj == "(") {
          nesting <- nesting + 1
        } else if (tj == ")") {
          if (nesting == 0) {
            if (j + 1 <= n) {
              block$text <- paste0("@@", tokens[[j + 1]]$text)
              block$regEx <- substr(pattern, tokens[[i + 1]]$end, tokens[[j]]$start - 1)
              block$end <- tokens[[j + 1]]$end
              block$isVariable <- TRUE
              i <- j + 1
            }
            break
          }
          nesting <- nesting - 1
        }
        j <- j + 1
      }
    }
    blocks[[length(blocks) + 1]] <- block
    i <- i + 1
  }
  if (length(blocks) > 0) {
    first <- blocks[[1]]
    last <- blocks[[length(blocks)]]
    if ((first$isVariable && is.null(first$regEx)) ||
        (last$isVariable && is.null(last$regEx))) {
      stop("Error in search pattern: pattern cannot start or end with a non-regex variable: ", pattern)
    }
  }
  blocks
}

# Search for pattern in sql starting at start_token; return list(start, end, startToken, variableToValue) or start=-1
# Optional lowercase_sql and tokens avoid re-tokenizing when caller already has them (e.g. inside search_and_replace loop).
translate_search <- function(sql, parsed_pattern, start_token, lowercase_sql = NULL, tokens = NULL) {
  if (is.null(tokens)) {
    lowercase_sql <- tolower(sql)
    tokens <- tokenize_sql(lowercase_sql)
  }
  n_tok <- length(tokens)
  match_count <- 0
  var_start <- 0
  nest_stack <- character(0)
  in_pattern_quote <- FALSE
  matched <- list(start = -1, end = -1, startToken = 0, variableToValue = list())

  cursor <- start_token
  while (cursor <= n_tok) {
    token <- tokens[[cursor]]
    text <- token$text
    block_idx <- match_count + 1
    block <- parsed_pattern[[block_idx]]

    if (block$isVariable) {
      # Variable: absorb tokens until we find the next literal or hit boundary
      if (!is.null(block$regEx) && (block_idx == length(parsed_pattern) || parsed_pattern[[block_idx + 1]]$isVariable)) {
        # Regex variable at end or followed by variable
        rest_sql <- substr(sql, token$start, nchar(sql))
        rest_sql_cap <- if (nchar(rest_sql) > MAX_REGEX_INPUT_LENGTH) substr(rest_sql, 1L, MAX_REGEX_INPUT_LENGTH) else rest_sql
        m <- regexpr(block$regEx, rest_sql_cap, ignore.case = TRUE, perl = TRUE)
        if (m > 0) {
          len <- attr(m, "match.length")
          if (m == 1) {
            if (match_count == 0) {
              matched$start <- token$start
              matched$startToken <- cursor
            }
            # Use full rest_sql for capture when we only searched capped prefix
            matched$variableToValue[[block$text]] <- substr(rest_sql, 1, len)
            match_count <- block_idx
            if (match_count == length(parsed_pattern)) {
              matched$end <- token$start + len - 1
              return(matched)
            }
            if (parsed_pattern[[match_count + 1]]$isVariable) {
              var_start <- token$start + len
            }
            while (cursor <= n_tok && tokens[[cursor]]$start < token$start + len) cursor <- cursor + 1
            cursor <- cursor - 1
          } else {
            match_count <- 0
          }
        } else {
          match_count <- 0
        }
      } else if (length(nest_stack) == 0 && block_idx < length(parsed_pattern) &&
          text == parsed_pattern[[block_idx + 1]]$text) {
        # Found the token after the variable: variable runs from var_start to just before this token.
        var_value <- substr(sql, var_start, token$start - 1)
        regex_rejected <- FALSE
        if (!is.null(block$regEx) && block_idx == 1) {
          var_value_trim <- gsub("\\s+$", "", var_value)
          var_value_trim_cap <- if (nchar(var_value_trim) > MAX_REGEX_INPUT_LENGTH) substr(var_value_trim, 1L, MAX_REGEX_INPUT_LENGTH) else var_value_trim
          m <- regexpr(block$regEx, var_value_trim_cap, ignore.case = TRUE, perl = TRUE)
          if (m > 0 && m + attr(m, "match.length") - 1 == nchar(var_value_trim)) {
            matched$variableToValue[[block$text]] <- substr(var_value_trim, m, m + attr(m, "match.length") - 1)
            matched$start <- var_start + m - 1
            matched$startToken <- cursor
          } else {
            regex_rejected <- TRUE
          }
        } else if (!is.null(block$regEx)) {
          var_value_cap <- if (nchar(var_value) > MAX_REGEX_INPUT_LENGTH) substr(var_value, 1L, MAX_REGEX_INPUT_LENGTH) else var_value
          if (!grepl(paste0("^", block$regEx, "$"), var_value_cap, ignore.case = TRUE, perl = TRUE)) {
            match_count <- 0
            cursor <- matched$startToken
            regex_rejected <- TRUE
          } else {
            matched$variableToValue[[block$text]] <- var_value
            if (match_count == 0) {
              matched$start <- var_start
              matched$startToken <- cursor
            }
          }
        } else {
          matched$variableToValue[[block$text]] <- var_value
          if (match_count == 0) {
            matched$start <- var_start
            matched$startToken <- cursor
          }
        }
        # Only advance match_count if the variable was captured (regex succeeded or no regex).
        # When a regex constraint rejects the variable, the overall pattern match fails.
        if (!regex_rejected) {
          match_count <- block_idx + 1
          if (match_count == length(parsed_pattern)) {
            matched$end <- token$end - 1
            return(matched)
          }
          if (match_count < length(parsed_pattern) && parsed_pattern[[match_count + 1]]$isVariable && cursor < n_tok) {
            var_start <- tokens[[cursor + 1]]$start
          }
        }
        if (text %in% c("'", "\"")) in_pattern_quote <- !in_pattern_quote
      } else if (match_count != 0 && length(nest_stack) == 0 && !in_pattern_quote &&
          (text == ";" || text == ")")) {
        match_count <- 0
        cursor <- matched$startToken
      } else {
        if (length(nest_stack) > 0 && nest_stack[length(nest_stack)] %in% c("\"", "'")) {
          if (text == nest_stack[length(nest_stack)]) {
            nest_stack <- nest_stack[-length(nest_stack)]
          }
        } else {
          if (text %in% c("\"", "'")) {
            nest_stack <- c(nest_stack, text)
          } else if (!in_pattern_quote && text == "(") {
            nest_stack <- c(nest_stack, text)
          } else if (!in_pattern_quote && length(nest_stack) > 0 && text == ")" && nest_stack[length(nest_stack)] == "(") {
            nest_stack <- nest_stack[-length(nest_stack)]
          }
        }
      }
    } else {
      if (text == block$text && (match_count != 0 || !token$inQuotes)) {
        if (match_count == 0) {
          matched$start <- token$start
          matched$startToken <- cursor
        }
        match_count <- block_idx
        if (match_count == length(parsed_pattern)) {
          matched$end <- token$end - 1
          return(matched)
        }
        # Set var_start when next block is variable (so we know where variable content starts)
        if (parsed_pattern[[match_count + 1]]$isVariable && cursor < n_tok) {
          var_start <- tokens[[cursor + 1]]$start
        }
        if (text %in% c("'", "\"")) in_pattern_quote <- !in_pattern_quote
      } else if (match_count != 0) {
        match_count <- 0
        cursor <- matched$startToken
      }
    }
    if (match_count != 0 && cursor == n_tok) {
      match_count <- 0
      cursor <- matched$startToken
    }
    cursor <- cursor + 1
  }
  matched$start <- -1
  matched
}

search_and_replace <- function(sql, parsed_pattern, replace_pattern) {
  lowercase_sql <- tolower(sql)
  tokens <- tokenize_sql(lowercase_sql)
  m <- translate_search(sql, parsed_pattern, 1, lowercase_sql, tokens)
  iter <- 0
  last_start <- -1L
  same_start_count <- 0L
  while (m$start >= 0 && iter < MAX_TRANSLATE_ITERATIONS) {
    if (nchar(sql) > MAX_SQL_LENGTH_TRANSLATE) break  # runaway growth
    iter <- iter + 1
    if (m$start == last_start) {
      same_start_count <- same_start_count + 1L
      if (same_start_count >= 3L) break  # same match 3 times: avoid infinite loop
    } else {
      same_start_count <- 0L
    }
    last_start <- m$start
    replacement <- replace_pattern
    for (nm in names(m$variableToValue)) {
      replacement <- str_replace_all(replacement, nm, trimws(m$variableToValue[[nm]]))
    }
    new_sql <- paste0(
      substr(sql, 1, m$start - 1),
      replacement,
      substr(sql, m$end + 1, nchar(sql))
    )
    if (new_sql == sql) break  # no change, avoid infinite loop
    sql <- new_sql
    delta <- 1
    repl_tokens <- tokenize_sql(replacement)
    if (length(repl_tokens) == 0) delta <- 0
    if (delta > 0 && grepl("^@@", replace_pattern) &&
        nchar(replacement) >= nchar(parsed_pattern[[1]]$text) &&
        tolower(trimws(substr(replacement, 1, nchar(parsed_pattern[[1]]$text)))) == parsed_pattern[[1]]$text) {
      delta <- 0
    }
    lowercase_sql <- tolower(sql)
    tokens <- tokenize_sql(lowercase_sql)
    m <- translate_search(sql, parsed_pattern, m$startToken + delta, lowercase_sql, tokens)
  }
  gsub("(?m)^[ \t]*\r?\n", "", sql, perl = TRUE)
}

translate_sql_apply_patterns <- function(sql, replacement_patterns, session_id, oracle_temp_prefix) {
  for (i in seq_along(replacement_patterns)) {
    pair <- replacement_patterns[[i]]
    repl <- pair$replacement_template
    repl <- gsub("%session_id%", session_id, repl, fixed = TRUE)
    repl <- gsub("%temp_prefix%", oracle_temp_prefix, repl, fixed = TRUE)
    sql <- search_and_replace(sql, pair$parsed, repl)
  }
  gsub("(?m)^[ \t]*\r?\n", "", sql, perl = TRUE)
}

generate_session_id <- function() {
  chars <- c(letters[1:26], 0:9)
  first <- sample(letters[1:26], 1)
  rest <- sample(chars, SESSION_ID_LENGTH - 1, replace = TRUE)
  paste0(c(first, rest), collapse = "")
}

.session_id_env <- new.env(parent = emptyenv())
.session_id_env$id <- NULL
get_global_session_id <- function() {
  if (is.null(.session_id_env$id)) {
    .session_id_env$id <- generate_session_id()
  }
  .session_id_env$id
}

ensure_patterns_loaded <- function(path_to_replacement_patterns) {
  if (length(ls(.translate_pattern_cache)) > 0) return()
  if (is.null(path_to_replacement_patterns) || path_to_replacement_patterns == "") {
    path_to_replacement_patterns <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  }
  df <- read.csv(path_to_replacement_patterns, stringsAsFactors = FALSE, encoding = "UTF-8", quote = "\"", fill = TRUE, comment.char = "")
  colnames(df) <- tolower(make.names(colnames(df)))
  if (!"to" %in% colnames(df)) colnames(df)[1] <- "to"
  if (!"pattern" %in% colnames(df)) colnames(df)[2] <- "pattern"
  if (!"replacement" %in% colnames(df)) colnames(df)[3] <- "replacement"
  for (i in seq_len(nrow(df))) {
    target <- trimws(df$to[i])
    pattern <- trimws(df$pattern[i])
    replacement <- trimws(df$replacement[i])
    if (is.na(target) || target == "" || is.na(pattern)) next
    if (is.na(replacement)) replacement <- ""
    replacement <- gsub("\\n", "\n", replacement, fixed = TRUE)
    pattern <- gsub("\\n", "\n", pattern, fixed = TRUE)
    pattern <- gsub("@", "@@", pattern, fixed = TRUE)
    replacement <- gsub("@", "@@", replacement, fixed = TRUE)
    parsed <- parse_search_pattern(pattern)
    if (!exists(target, envir = .translate_pattern_cache, inherits = FALSE)) {
      assign(target, list(), envir = .translate_pattern_cache)
    }
    pats <- get(target, envir = .translate_pattern_cache)
    pats[[length(pats) + 1]] <- list(parsed = parsed, replacement_template = replacement, pattern = pattern)
    assign(target, pats, envir = .translate_pattern_cache)
  }
  # Reorder so "USE @schema;" runs first (avoid other patterns altering schema name before USE)
  for (target in ls(.translate_pattern_cache)) {
    pats <- get(target, envir = .translate_pattern_cache)
    use_idx <- which(vapply(pats, function(p) {
      pt <- tolower(trimws(p$pattern))
      pt == "use @@schema;" || (startsWith(pt, "use ") && grepl("@@schema", pt, fixed = TRUE) && endsWith(pt, ";"))
    }, logical(1)))
    if (length(use_idx) > 0) {
      pats <- c(pats[use_idx], pats[-use_idx])
      assign(target, pats, envir = .translate_pattern_cache)
    }
  }
}

translate_sql_with_path <- function(sql, target_dialect, session_id, temp_emulation_schema, path_to_replacement_patterns) {
  ensure_patterns_loaded(path_to_replacement_patterns)
  if (is.null(session_id) || is.na(session_id) || session_id == "") {
    session_id <- get_global_session_id()
  }
  oracle_temp_prefix <- if (is.null(temp_emulation_schema) || temp_emulation_schema == "") "" else paste0(temp_emulation_schema, ".")
  targets <- ls(.translate_pattern_cache)
  if (!target_dialect %in% targets) {
    stop("Don't know how to translate to ", target_dialect, ". Valid target dialects are ", paste(targets, collapse = ", "))
  }
  replacement_patterns <- get(target_dialect, envir = .translate_pattern_cache)
  if (tolower(target_dialect) == "bigquery") {
    sql <- translate_bigquery(sql)
  } else if (tolower(target_dialect) == "spark") {
    sql <- translate_spark(sql)
  }
  sql <- translate_sql_apply_patterns(sql, replacement_patterns, session_id, oracle_temp_prefix)
  if (tolower(target_dialect) %in% c("impala", "bigquery", "spark")) {
    sql <- replace_with_concat(sql)
  }
  sql
}

translate_single_statement_with_path <- function(sql, target_dialect, session_id, temp_emulation_schema, path_to_replacement_patterns) {
  sql <- translate_sql_with_path(sql, target_dialect, session_id, temp_emulation_schema, path_to_replacement_patterns)
  statements <- split_sql_core(sql)
  if (length(statements) > 1) {
    stop("SQL contains more than one statement: ", sql)
  }
  statements[1]
}

translate_check_warnings <- function(sql, target_dialect) {
  warnings <- character(0)
  long_temp <- regmatches(sql, gregexpr("#[0-9a-zA-Z_]+", sql))[[1]]
  long_temp_names <- long_temp[nchar(long_temp) > MAX_TABLE_NAME_LENGTH - SESSION_ID_LENGTH - 1]
  for (nm in unique(long_temp_names)) {
    warnings <- c(warnings, sprintf("Temp table name '%s' is too long. Temp table names should be shorter than %d characters to prevent some DMBSs from throwing an error.", nm, MAX_TABLE_NAME_LENGTH - SESSION_ID_LENGTH))
  }
  m <- gregexpr("(create|drop|truncate)\\s+table +[0-9a-zA-Z_]+", tolower(sql))[[1]]
  if (m[1] > 0) {
    for (i in seq_along(m)) {
      grp <- substr(sql, m[i], m[i] + attr(m, "match.length")[i] - 1)
      name_start <- max(gregexpr(" ", grp)[[1]])
      name <- trimws(substr(grp, name_start, nchar(grp)))
      if (nchar(name) > MAX_TABLE_NAME_LENGTH && !paste0("#", name) %in% long_temp_names) {
        warnings <- c(warnings, sprintf("Table name '%s' is too long. Table names should be shorter than %d characters to prevent some DMBSs from throwing an error.", name, MAX_TABLE_NAME_LENGTH))
      }
    }
  }
  unique(warnings)
}

# BigQuery/Spark pre-translation stubs (full logic in BigQuerySparkTranslate.java - simplified here)
translate_bigquery <- function(sql) {
  # Lowercase except string literals; build result in one pass to avoid O(n^2) string rebuilding
  tokens <- tokenize_sql(sql)
  n <- nchar(sql)
  parts <- list()
  last_end <- 1L
  for (i in seq_along(tokens)) {
    t <- tokens[[i]]
    if (last_end < t$start) {
      parts[[length(parts) + 1L]] <- substr(sql, last_end, t$start - 1L)
    }
    if (!t$inQuotes && !grepl("^@", t$text)) {
      parts[[length(parts) + 1L]] <- tolower(t$text)
    } else {
      parts[[length(parts) + 1L]] <- substr(sql, t$start, t$end - 1L)
    }
    last_end <- t$end
  }
  if (last_end <= n) {
    parts[[length(parts) + 1L]] <- substr(sql, last_end, n)
  }
  paste(parts, collapse = "")
}

translate_spark <- function(sql) {
  statements <- split_sql_core(sql)
  for (i in seq_along(statements)) {
    st <- statements[i]
    st <- gsub("\t", " ", st)
    st <- gsub(" +", " ", st)
    if (grepl("^\\s*CREATE TABLE\\s+(\\S+)\\s*\\((.+)\\)\\s*$", st, ignore.case = TRUE)) {
      # Convert CREATE TABLE x (a int, b int) to SELECT ... INTO ... WHERE 1=0
      mt <- regexec("^\\s*CREATE TABLE\\s+(\\S+)\\s*\\((.+)\\)\\s*$", st, ignore.case = TRUE)[[1]]
      if (mt[1] > 0) {
        table_name <- substr(st, mt[3], mt[3] + attr(mt, "match.length")[3] - 1)
        def <- substr(st, mt[4], mt[4] + attr(mt, "match.length")[4] - 1)
        def <- tolower(gsub("[\r\n]", "", def))
        def <- gsub(" as ", " ", def)
        parts <- strsplit(def, ",")[[1]]
        cols <- vapply(trimws(parts), function(p) {
          sp <- strsplit(trimws(p), "\\s+")[[1]]
          if (length(sp) >= 2) paste0("CAST(NULL AS ", sp[2], ") AS ", sp[1]) else p
        }, character(1))
        st <- paste0("SELECT ", paste(cols, collapse = ",\r\n"), " INTO ", table_name, " WHERE 1 = 0")
      }
    }
    st <- gsub(";", "", st)
    statements[i] <- st
  }
  sql <- paste(trimws(statements), collapse = ";\r\n")
  if (grepl(";\\s*$", sql)) sql else paste0(sql, ";")
}
# Render and translate SQL (from SqlRendereR - in-tree)

#' @importFrom utils read.csv
listSupportedDialects <- function() {
  pathToCsv <- system.file("csv", "supportedDialects.csv", package = "CDMConnector")
  read.csv(pathToCsv, stringsAsFactors = FALSE)
}

#' Render parameterized SQL (Pure R implementation — fallback)
#'
#' Supports @param substitution and conditional \code{{cond}?{then}:{else}} syntax.
#' @param sql SQL string.
#' @param warnOnMissingParameters Whether to warn on missing parameters.
#' @param ... Named parameters for substitution.
#' @keywords internal
#' @noRd
render_r <- function(sql, warnOnMissingParameters = TRUE, ...) {
  checkmate::assertCharacter(sql, len = 1)
  checkmate::assertLogical(warnOnMissingParameters, len = 1)
  parameters <- lapply(list(...), function(x) paste(x, collapse = ","))
  param_values <- as.character(parameters)
  names(param_values) <- names(parameters)
  if (warnOnMissingParameters && length(parameters) > 0) {
    messages <- render_check_missing_params(as.character(sql), names(parameters))
    for (message in messages) warning(message)
  }
  translatedSql <- render_sql_core(as.character(sql), param_values)
  attributes(translatedSql) <- attributes(sql)
  return(translatedSql)
}

#' Translate SQL to target dialect
#' (Pure R implementation — kept as fallback)
#' @param sql SQL string.
#' @param targetDialect Target dialect.
#' @param tempEmulationSchema Temp schema for emulation.
#' @param oracleTempSchema Oracle temp schema.
#' @keywords internal
#' @noRd
translate_r <- function(sql,
                      targetDialect,
                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                      oracleTempSchema = NULL) {
  checkmate::assertCharacter(sql, len = 1)
  checkmate::assertCharacter(targetDialect, len = 1)
  checkmate::assertChoice(targetDialect, listSupportedDialects()$dialect)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE)
  checkmate::assertCharacter(oracleTempSchema, len = 1, null.ok = TRUE)
  if (!is.null(attr(sql, "sqlDialect"))) {
    warning("Input SQL has already been translated")
    return(sql)
  }
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    tempEmulationSchema <- oracleTempSchema
  }
  pathToReplacementPatterns <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  tempEmulationSchema <- if (is.null(tempEmulationSchema) || is.na(tempEmulationSchema)) "" else as.character(tempEmulationSchema)
  messages <- translate_check_warnings(as.character(sql), as.character(targetDialect))
  for (message in messages) warning(message)
  translatedSql <- translate_sql_with_path(as.character(sql), as.character(targetDialect), NULL, tempEmulationSchema, pathToReplacementPatterns)
  attributes(translatedSql) <- attributes(sql)
  attr(translatedSql, "sqlDialect") <- targetDialect
  return(translatedSql)
}

#' Render parameterized SQL — delegates to SqlRender (Java) for speed
#' @noRd
render <- function(sql, warnOnMissingParameters = TRUE, ...) {
  SqlRender::render(sql, warnOnMissingParameters = warnOnMissingParameters, ...)
}

#' Translate SQL to target dialect — delegates to SqlRender (Java) for speed
#' @noRd
translate <- function(sql,
                      targetDialect,
                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                      oracleTempSchema = NULL) {
  # SqlRender::translate has ~O(n^2) scaling with input size.

  # Chunking large SQL strings into smaller pieces before translating
  # avoids this bottleneck (e.g. 15s -> 1.5s for 1MB SQL).
  chunk_threshold <- 100000L  # 100k chars
  if (nchar(sql) > chunk_threshold) {
    stmts <- split_sql_core(sql)
    n <- length(stmts)
    if (n > 50L) {
      chunk_size <- ceiling(n / 50L)
      chunks <- split(stmts, ceiling(seq_len(n) / chunk_size))
      translated <- vapply(chunks, function(ch) {
        # Trailing semicolons required: SqlRender only converts SELECT INTO -> CTAS
        # when statements end with ';'
        SqlRender::translate(paste0(paste(ch, collapse = ";\n"), ";"),
                             targetDialect = targetDialect,
                             tempEmulationSchema = tempEmulationSchema,
                             oracleTempSchema = oracleTempSchema)
      }, character(1))
      return(paste(translated, collapse = "\n"))
    }
  }
  SqlRender::translate(sql, targetDialect = targetDialect,
                       tempEmulationSchema = tempEmulationSchema,
                       oracleTempSchema = oracleTempSchema)
}
