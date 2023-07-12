


# #' Collapse a character column in a database using concatenation
# #'
# #' This function must be "unquoted" using the "bang bang" operator (!!). See example.
# #'
# #' @param name The name of a character column in the database table as a character string
# #'
# #' @return Platform specific SQL that can be used in a dplyr query.
# #' @export
# #' @importFrom rlang !!
# #'
# #' @examples
# #' \dontrun{
# #' con <- DBI::dbConnect(duckdb::duckdb())
# #' date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")),
# #'                            name = "tmpdate", overwrite = TRUE, temporary = TRUE)
# #'
# #' df <- date_tbl %>%
# #'   dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
# #'   dplyr::collect()
# #'
# #' DBI::dbDisconnect(con, shutdown = TRUE)
# #' }
# stringagg <- function(date, number, interval = "day") {
#   checkmate::assertCharacter(interval, len = 1)
#   checkmate::assertSubset(interval, choices = c("day", "year"))
#   checkmate::assertCharacter(date, len = 1)
#   if(!(checkmate::testCharacter(number, len = 1) || checkmate::testIntegerish(number, len = 1))) {
#     rlang::abort("`number` must a character string with a column name or a number.")
#   }
#
#   dot <- get(".", envir = parent.frame())
#   db <- CDMConnector::dbms(dot$src$con)
#
#   if (db %in% c("oracle", "snowflake")) {
#     date <- as.character(DBI::dbQuoteIdentifier(dot$src$con, date))
#     if (is.character(number)) {
#       number <- as.character(DBI::dbQuoteIdentifier(dot$src$con, number))
#     }
#   }
#
#   if (db %in% c("spark", "oracle") && interval == "year") {
#     # spark and oracle sql requires number of days in dateadd
#     if (is.numeric(number)) {
#       number <- floor(number*365.25)
#     } else {
#       number <- paste(number, "* 365.25")
#     }
#   }
#
#   sql <- switch (db,
#                  "redshift" = glue::glue("DATEADD({interval}, {number}, {date})"),
#                  "oracle" = glue::glue("({date} + NUMTODSINTERVAL({number}, 'day'))"),
#                  "postgresql" = glue::glue("({date} + {number}*INTERVAL'1 {interval}')"),
#                  "sql server" = glue::glue("DATEADD({interval}, {number}, {date})"),
#                  "spark" = glue::glue("date_add({date}, {number})"),
#                  "duckdb" = glue::glue("({date} + {number}*INTERVAL'1 {interval}')"),
#                  "sqlite" = glue::glue("CAST(STRFTIME('%s', DATETIME({date}, 'unixepoch', ({number})||' {interval}s')) AS REAL)"),
#                  "bigquery" = glue::glue("DATE_ADD({date}, INTERVAL {number} {toupper(interval)})"),
#                  "snowflake" = glue::glue('DATEADD({interval}, {number}, {date})'),
#                  rlang::abort(glue::glue("Connection type {paste(class(dot$src$con), collapse = ', ')} is not supported!"))
#   )
#   dbplyr::sql(as.character(sql))
# }
