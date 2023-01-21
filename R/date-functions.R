#' Add days or years to a date in a dplyr query
#'
#' This function must be "unquoted" using the "bang bang" operator (!!). See example.
#'
#' @param date The name of a date column in the database table as a character string
#' @param number The number of units to add. Can be a positive or negative whole number.
#' @param interval The units to add. Must be either "day" (default) or "year"
#'
#' @return Platform specific SQL that can be used in a dplyr query.
#' @export
#' @importFrom rlang !!
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#' date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")),
#'                            name = "tmpdate", overwrite = TRUE, temporary = TRUE)
#'
#' df <- date_tbl %>%
#'   dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
#'   dplyr::collect()
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
dateadd <- function(date, number, interval = "day") {
  rlang::check_installed("SqlRender")
  checkmate::assertCharacter(interval, len = 1)
  checkmate::assertSubset(interval, choices = c("day", "year"))
  checkmate::assertCharacter(date, len = 1)
  if(!(checkmate::testCharacter(number, len = 1) || checkmate::testIntegerish(number, len = 1))) {
    rlang::abort("`number` must a character string with a column name or a number.")
  }

  dot <- get(".", envir = parent.frame())
  targetDialect <- CDMConnector::dbms(dot$src$con)

  if (targetDialect == "oracle") {
    date <- as.character(DBI::dbQuoteIdentifier(dot$src$con, date))
    if (is.character(number)) {
      number <- as.character(DBI::dbQuoteIdentifier(dot$src$con, number))
    }
  }

  sql <- glue::glue("DATEADD({interval}, {number}, {date})")
  sql <- SqlRender::translate(sql = as.character(sql), targetDialect = targetDialect)
  dbplyr::sql(sql)
}


#' Compute the difference between two days
#'
#' This function must be "unquoted" using the "bang bang" operator (!!). See example.
#'
#' @param start The name of the start date column in the database as a string.
#' @param end The name of the end date column in the database as a string.
#' @param interval The units to use for difference calculation. Must be either "day" (default) or "year".
#'
#' @return Platform specific SQL that can be used in a dplyr query.
#' @export
#' @importFrom rlang !!
#'
#' @examples
#' \dontrun{
#' library(SqlUtilities)
#' con <- DBI::dbConnect(duckdb::duckdb())
#' date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")),
#'                            name = "tmpdate", overwrite = TRUE, temporary = TRUE)
#'
#' df <- date_tbl %>%
#'   dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
#'   dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
#'   dplyr::collect()
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
datediff <- function(start, end, interval = "day") {
  rlang::check_installed("SqlRender")
  checkmate::assertCharacter(interval, len = 1)
  checkmate::assertSubset(interval, choices = c("day", "year"))
  checkmate::assertCharacter(start, len = 1)
  checkmate::assertCharacter(end, len = 1)

  dot <- get(".", envir = parent.frame())
  targetDialect <- CDMConnector::dbms(dot$src$con)

  if (targetDialect == "oracle") {
    start <- as.character(DBI::dbQuoteIdentifier(dot$src$con, start))
    end <- as.character(DBI::dbQuoteIdentifier(dot$src$con, end))
  }

  sql <- glue::glue("DATEDIFF({interval}, {start},  {end})")
  sql <- SqlRender::translate(sql = as.character(sql), targetDialect = targetDialect)
  dbplyr::sql(sql)
}

#' as.Date dbplyr translation wrapper
#'
#' This is a workaround for using as.Date inside dplyr verbs against a database
#' backend. This function should only be used inside dplyr verbs where the first
#' argument is a database table reference. `asDate` must be unquoted with !! inside
#' dplyr verbs (see example).
#'
#' @param x an R expression
#'
#' @export
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(odbc::odbc(), "Oracle")
#' date_tbl <- dplyr::copy_to(con,
#'                            data.frame(y = 2000L, m = 10L, d = 10L),
#'                            name = "tmp",
#'                            temporary = TRUE)
#'
#' df <- date_tbl %>%
#'   dplyr::mutate(date_from_parts = !!asDate(paste0(
#'     .data$y, "/",
#'     .data$m, "/",
#'     .data$d
#'   ))) %>%
#'   collect()
#' }
asDate <- function(x) {
  x_quo <- rlang::enquo(x)
  .data <- get(".", envir = parent.frame())
  dialect <- CDMConnector::dbms(.data$src$con)

  if (dialect == "oracle") {
    x <- dbplyr::partial_eval(x_quo, data = .data)
    x <- dbplyr::translate_sql(!!x, con = .data$src$con)
    x <- glue::glue("TO_DATE({x}, 'YYYY-MM-DD')")
    return(dplyr::sql(x))
  } else if (dialect == "spark") {
    x <- dbplyr::partial_eval(x_quo, data = .data)
    x <- dbplyr::translate_sql(!!x, con = .data$src$con)
    x <- glue::glue("TO_DATE({x})")
    return(dplyr::sql(x))
  } else {
    return(rlang::expr(as.Date(!!x_quo)))
  }
}
