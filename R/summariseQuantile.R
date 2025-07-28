# Copyright 2025 DARWIN EU®
#
# This file is part of CDMConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Quantile calculation using dbplyr
#'
#' @description
#' This function provides DBMS independent syntax for quantiles estimation.
#' Can be used by itself or in combination with `mutate()`
#' when calculating other aggregate metrics (min, max, mean).
#'
#' `summarise_quantile()`, `summarize_quantile()`, `summariseQuantile()` and `summarizeQuantile()` are synonyms.
#'
#' @details
#' Implemented quantiles estimation algorithm returns values analogous to
#' `quantile{stats}` with argument `type = 1`.
#' See discussion in Hyndman and Fan (1996).
#' Results differ from `PERCENTILE_CONT` natively implemented in various DBMS,
#' where returned values are equal to `quantile{stats}` with default argument `type = 7`
#'
#'
#' @param .data lazy data frame backed by a database query.
#' @param x column name whose sample quantiles are wanted.
#' @param probs numeric vector of probabilities with values in \[0,1\].
#' @param nameSuffix character; is appended to numerical quantile value as a column name part.
#' @return
#' An object of the same type as '.data'
#'
#' @importFrom rlang %||%
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#' mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#'
#' df <- mtcars_tbl %>%
#'  dplyr::group_by(cyl) %>%
#'  dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#'  summariseQuantile(mpg, probs = c(0, 0.2, 0.4, 0.6, 0.8, 1),
#'                    nameSuffix = "quant") %>%
#'  dplyr::collect()
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
summariseQuantile <- function(.data, x = NULL, probs, nameSuffix = "value") {
  checkmate::assertClass(.data, "tbl_sql")
  checkmate::assert_double(probs, min.len = 1, lower = 0, upper = 1)
  checkmate::assert_character(nameSuffix, null.ok = TRUE)

  selection_context <- .data$lazy_query$select_operation

  if (!is.null(selection_context) && selection_context == 'summarise') {
    rlang::abort("Cannot estimate quantiles in summarise context.
                 Try using `mutate()` function instead of `summarise()`")
  }

  vars_context <- NULL
  x_context <- NULL
  x_arg <- rlang::enexpr(x)

  if (!is.null(selection_context)) {
    vars_context <- .data$lazy_query$select %>%
      dplyr::filter(unlist(purrr::map(.data$expr, rlang::is_quosure)))
    if (nrow(vars_context) > 0) {
      vars_context <- vars_context %>%
        # dplyr::mutate(x_var = purrr::map(purrr::map(.data$expr, rlang::get_expr), ~ if (length(.x) >= 2) {.x[[2]]} else {NULL}))
        dplyr::mutate(x_var = purrr::map(.data$expr, ~if(length(rlang::get_expr(.x)) >= 2) {rlang::get_expr(.x)[[2]]} else {NULL}))
      x_context <- unique(vars_context$x_var)[[1]]
    }
  }

  if (!is.null(x_context) && !is.null(x_arg) && x_context != x_arg) {
      msg <- paste0("Confilicting quantile variables: `", x_context, "` (from context) and `", x_arg, "` (passed argument)")
      rlang::abort(msg)
  }

  if (is.null(x_context) & is.null(x_arg)) {
    msg = "Quantile variable is not specified"
    rlang::abort(msg)
  }

  x <- x_context %||% x_arg

  x <- x_arg
  group_by_vars <- .data$lazy_query$group_vars
  group_1 <- rlang::syms(c(group_by_vars, x))

  funs = list()
  if (!is.null(selection_context)) {
    funs <- purrr::map(vars_context$name, ~ rlang::expr(max(!!rlang::sym(.x), na.rm = TRUE)))
    names(funs) <- vars_context$name
  }

  group_2 <- rlang::syms(c(group_by_vars, names(funs)))

  probs = sort(unique(probs))
  quant_expr <- purrr::map(probs, ~ rlang::expr(min(ifelse(accumulated >= !!.x * total, !!x, NA), na.rm = TRUE)))
  names(quant_expr) <- paste0('p', as.character(probs * 100), '_', nameSuffix)

  query <- rlang::expr(
    .data %>%
      dplyr::group_by(!!!group_1) %>%
      dplyr::summarise(..n = dplyr::n(), !!!funs, .groups = "drop") %>%
      dplyr::group_by(!!!group_2) %>%
      dbplyr::window_order(!!x) %>%
      dplyr::mutate(accumulated = cumsum(.data$..n),
                    total = sum(.data$..n, na.rm = TRUE)) %>%
      dplyr::summarize(!!!quant_expr, .groups = "drop")
  )
  eval(query)
}

# Internal function to support summariseQuantile2. Accepts a single variable as a string.
summariseQuantile1 <- function(.data, x = NULL, probs, nameSuffix = "{x}") {
  if (!methods::is(.data, "tbl_sql") && !methods::is(.data, "data.frame")) {
    cli::cli_abort(".data must be tbl_sql or a data.frame")
  }
  checkmate::assert_double(probs, min.len = 1, lower = 0, upper = 1)
  checkmate::assert_character(nameSuffix, null.ok = TRUE)
  checkmate::assert_character(x, null.ok = TRUE)

  nameSuffix <- glue::glue(nameSuffix)

  x_sym <- as.symbol(x)
  group_by_vars <- dplyr::group_vars(.data)
  group_1 <- rlang::syms(c(group_by_vars, x_sym))
  group_2 <- rlang::syms(c(group_by_vars))

  probs <- sort(unique(probs))
  quant_expr <- purrr::map(probs, ~ rlang::expr(min(ifelse(accumulated >= !!.x * total, !!x_sym, NA), na.rm = TRUE)))
  names(quant_expr) <- paste0('q', sprintf('%02d', as.integer(probs * 100)), '_', nameSuffix)

  if (methods::is(.data, "data.frame")) {
    query <- rlang::expr(
      .data %>%
        dplyr::group_by(!!!group_1) %>%
        dplyr::summarise(n__ = dplyr::n(), .groups = "drop") %>%
        dplyr::group_by(!!!group_2) %>%
        dplyr::arrange(!!x_sym) %>%
        dplyr::mutate(accumulated = cumsum(.data$n__),
                      total = sum(.data$n__, na.rm = TRUE)) %>%
        dplyr::summarize(!!!quant_expr, .groups = "drop")
    )
  } else if (dbms(.data$src$con) == "duckdb") {
    # tie breaker for min() expression that result in non-deterministic ordering in SQL
    query <- rlang::expr(
      .data %>%
        dplyr::group_by(!!!group_1) %>%
        dplyr::summarise(n__ = dplyr::n(), .groups = "drop") %>%
        dplyr::mutate(row_id__ = dplyr::row_number()) %>%
        dplyr::group_by(!!!group_2) %>%
        dbplyr::window_order(!!x_sym, .data$row_id__) %>%
        dplyr::mutate(accumulated = cumsum(.data$n__),
                      total = sum(.data$n__, na.rm = TRUE)) %>%
        dplyr::summarize(!!!quant_expr, .groups = "drop")
    )
  } else {
    # tie breaker for min() is not needed on most databases
    query <- rlang::expr(
      .data %>%
        dplyr::group_by(!!!group_1) %>%
        dplyr::summarise(n__ = dplyr::n(), .groups = "drop") %>%
        dplyr::group_by(!!!group_2) %>%
        dbplyr::window_order(!!x_sym) %>%
        dplyr::mutate(accumulated = cumsum(.data$n__),
                      total = sum(.data$n__, na.rm = TRUE)) %>%
        dplyr::summarize(!!!quant_expr, .groups = "drop")
    )
  }

  eval(query)
}

#' Quantile calculation using dbplyr
#'
#' @description
#' This function provides DBMS independent syntax for quantile estimation. Some
#' database systems do not have a `quantile` function. The SQL generated by
#' `summarizeQuantile2` should work on all supported database systems. This function
#' can be added to a dplyr pipeline and adds an additional query to the input.
#' No computation is triggered by `summarizeQuantile2` if the input is a `tbl`
#' reference to a database table.
#'
#' @details
#' Implemented quantiles estimation algorithm returns values analogous to
#' `quantile{stats}` with argument `type = 1`.
#' See discussion in Hyndman and Fan (1996).
#' Results differ from `PERCENTILE_CONT` natively implemented in various DBMS,
#' where returned values are equal to `quantile{stats}` with default argument `type = 7`
#'
#' `r lifecycle::badge("experimental")`
#'
#' @param .data lazy data frame backed by a database query created by `dplyr::tbl()`.
#' @param x A string vector of column names whose sample quantiles are wanted.
#' @param probs A numeric vector of probabilities with values in \[0,1\].
#' @param nameSuffix A single character character string, evaluated by `glue::glue()`
#'  that is appended to numerical quantile value as a column name part.
#' @return
#' A lazy query with quantile calculation added. The result (after computation)
#' will have one row per combination of grouping variables and one column for
#' every variable/quantile combination. (see examples)
#'
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#' mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#'
#' # quantiles for a single column
#' mtcars_tbl %>%
#'   dplyr::group_by(cyl) %>%
#'   dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#'   summariseQuantile2("mpg", probs = c(0, 0.2, 0.4, 0.6, 0.8, 1),  nameSuffix = "quant") %>%
#'   dplyr::collect()
#'
#' #> cyl  p0_quant p20_quant p40_quant p60_quant p80_quant p100_quant
#' #>   6      17.8      18.1      19.2      21        21         21.4
#' #>   8      10.4      13.3      15        15.5      17.3       19.2
#' #>   4      21.4      22.8      24.4      27.3      30.4       33.9
#'
#' # multiple columns
#' mtcars_tbl %>%
#'   dplyr::group_by(cyl) %>%
#'   dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#'   summariseQuantile2(c("mpg", "hp", "wt"), probs = c(0.2, 0.8),  nameSuffix = "{x}_quant") %>%
#'   dplyr::collect()
#'
#' #>  cyl p20_mpg_quant p80_mpg_quant p20_hp_quant p80_hp_quant p20_wt_quant p80_wt_quant
#' #>    4          22.8          30.4           65           97         1.84         2.78
#' #>    6          18.1          21            110          123         2.77         3.44
#' #>    8          13.3          17.3          175          245         3.44         5.25
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
summariseQuantile2 <- function(.data, x, probs, nameSuffix = "{x}") {

  if (!methods::is(.data, "tbl_sql") && !methods::is(.data, "data.frame")) {
    cli::cli_abort(".data must be tbl_sql or a data.frame")
  }
  checkmate::assertCharacter(x, any.missing = FALSE, min.len = 1, min.chars = 1, pattern = "^[A-Za-z][A-Za-z0-9_]*$")
  checkmate::assert_double(probs, min.len = 1, lower = 0, upper = 1, any.missing = FALSE)

  if (length(x) > 1 && !grepl("\\{x\\}", nameSuffix)) {
    rlang::abort("`nameSuffix` should contain {x}, the variable name, when multiple quantiles are calculated.")
  }

  group_by_vars <- dplyr::group_vars(.data)

  result <- list()
  for (i in seq_along(x)) {
    result[[i]] <- summariseQuantile1(.data, x = x[i], probs = probs, nameSuffix = nameSuffix)
  }

  if (length(group_by_vars) > 0) {
    result <- purrr::reduce(result, dplyr::full_join, by = group_by_vars)
  } else {
    result <- purrr::reduce(result, dplyr::cross_join)
  }

  return(result)
}
