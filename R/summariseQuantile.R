# Copyright 2024 DARWIN EU®
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
