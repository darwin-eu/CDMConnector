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
  checkmate::assertCharacter(interval, len = 1)
  checkmate::assertSubset(interval, choices = c("day", "year"))
  checkmate::assertCharacter(date, len = 1)
  if(!(checkmate::testCharacter(number, len = 1) || checkmate::testIntegerish(number, len = 1))) {
    rlang::abort("`number` must a character string with a column name or a number.")
  }

  dot <- get(".", envir = parent.frame())
  db <- CDMConnector::dbms(dot$src$con)

  if (db == "oracle") {
    date <- as.character(DBI::dbQuoteIdentifier(dot$src$con, date))
    if (is.character(number)) {
      number <- as.character(DBI::dbQuoteIdentifier(dot$src$con, number))
    }
  }

  if (db %in% c("spark", "oracle") && interval == "year") {
    # spark and oracle sql requires number of days in dateadd
    number <- floor(number*365.25)
  }

  sql <- switch (db,
                 "redshift" = glue::glue("DATEADD({interval}, {number}, {date})"),
                 "oracle" = glue::glue("({date} + NUMTODSINTERVAL({number}, 'day'))"),
                 "postgresql" = glue::glue("({date} + {number}*INTERVAL'1 {interval}')"),
                 "sql server" = glue::glue("DATEADD({interval}, {number}, {date})"),
                 "spark" = glue::glue("date_add({date}, {number})"),
                 "duckdb" = glue::glue("({date} + {number}*INTERVAL'1 {interval}')"),
                 "sqlite" = glue::glue("CAST(STRFTIME('%s', DATETIME({date}, 'unixepoch', ({number})||' {interval}s')) AS REAL)"),
                 "bigquery" = glue::glue("DATE_ADD({date}, INTERVAL {number} {toupper(interval)})"),
                 glue::glue("DATEADD({interval}, {number}, {date})")
  )

  dbplyr::sql(as.character(sql))
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
  checkmate::assertCharacter(interval, len = 1)
  checkmate::assertSubset(interval, choices = c("day", "month", "year"))
  checkmate::assertCharacter(start, len = 1)
  checkmate::assertCharacter(end, len = 1)

  dot <- get(".", envir = parent.frame())
  db <- CDMConnector::dbms(dot$src$con)

  if (db == "oracle") {
    start <- as.character(DBI::dbQuoteIdentifier(dot$src$con, start))
    end <- as.character(DBI::dbQuoteIdentifier(dot$src$con, end))
  }

  if (interval == "day") {
    sql <- switch (
      db,
      "redshift" = glue::glue("DATEDIFF(day, {start}, {end})"),
      "oracle" = glue::glue("CEIL(CAST({end} AS DATE) - CAST({start} AS DATE))"),
      "postgresql" = glue::glue("(CAST({end} AS DATE) - CAST({start} AS DATE))"),
      "sql server" = glue::glue("DATEDIFF(day, {start}, {end})"),
      "spark" = glue::glue("datediff({end},{start})"),
      "duckdb" = glue::glue("datediff('day', {start}, {end})"),
      "sqlite" = glue::glue("(JULIANDAY(end, 'unixepoch') - JULIANDAY(start, 'unixepoch'))"),
      "bigquery" = glue::glue("DATE_DIFF({start}, {end}, DAY)")
    )
  } else {
    dayStart   <- datepart(start, "day",   db)
    monthStart <- datepart(start, "month", db)
    yearStart  <- datepart(start, "year",  db)
    dayEnd     <- datepart(end,   "day",   db)
    monthEnd   <- datepart(end,   "month", db)
    yearEnd    <- datepart(end,   "year",  db)
    if (interval == "month") {
      sql <- glue::glue(
        "(({yearEnd} * 1200 + {monthEnd} * 100 + {dayEnd} -
         ({yearStart} * 1200 + {monthStart} * 100 + {dayStart})) / 100)"
      )
    } else {
      sql <- glue::glue(
        "(({yearEnd} * 10000 + {monthEnd} * 100 + {dayEnd} -
         ({yearStart} * 10000 + {monthStart} * 100 + {dayStart})) / 10000)"
      )
    }
  }

  dbplyr::sql(as.character(sql))
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


#' @rdname asDate
#' @export
as_date <- asDate

#' Extract the day, month or year of a date in a dplyr pipeline
#'
#' @param date Character string that represents to a date column.
#' @param interval Interval to extract from a date. Valid options are "year", "month", or "day".
#' @param dbms Database system, if NULL it is auto detected.
#'
#' @export
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
#' date_tbl <- dplyr::copy_to(con,
#'                            data.frame(birth_date = as.Date("1993-04-19")),
#'                            name = "tmp",
#'                            temporary = TRUE)
#' df <- date_tbl %>%
#'   dplyr::mutate(year = !!datepart("birth_date", "year")) %>%
#'   dplyr::mutate(month = !!datepart("birth_date", "month")) %>%
#'   dplyr::mutate(day = !!datepart("birth_date", "day")) %>%
#'   dplyr::collect()
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
datepart <- function(date, interval = "year", dbms = NULL) {
  checkmate::assertCharacter(date, len = 1)
  checkmate::assertChoice(interval, c("year", "month", "day"))
  supported <- c("redshift", "oracle", "postgresql", "sql server", "spark", "duckdb", "sqlite", "bigquery")
  checkmate::assertChoice(dbms, choices = supported, null.ok = TRUE)

  if (is.null(dbms)) {
    dot <- get(".", envir = parent.frame())
    dbms <- CDMConnector::dbms(dot$src$con)
  }

  sql <- switch (dbms,
    "redshift" = "DATE_PART({interval}, {date})",
    "oracle" = "EXTRACT({toupper(interval)} FROM {date})",
    "postgresql" = "EXTRACT({toupper(interval)} FROM {date})",
    "sql server" = "{toupper(interval)}({date})",
    "spark" = "{toupper(interval)}({date})",
    "duckdb" = "date_part('{interval}', {date})",
    "sqlite" = ifelse(interval == "year",
      "CAST(STRFTIME('%Y', {date}, 'unixepoch') AS INT)",
      "CAST(STRFTIME('%{substr(interval, 1, 1)}', {date}, 'unixepoch') AS INT)"),
    "bigquery" = "EXTRACT({toupper(interval)} from {date})"
  )
  dbplyr::sql(as.character(glue::glue(sql)))
}
