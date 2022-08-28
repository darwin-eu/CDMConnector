

#' dbConnect arguments stored for later use
#'
#' @keywords internal
#' @export
#' @import DBI
#' @import methods
setClass("dbConnectDetails")

#' Save database connection information in an object
#'
#' `dbConnectDetails` returns an object that can be used to connect to a database
#' with dbConnect at some future point in execution. Functions that need to create
#' a new connection to a database with `dbConnect` can accept a `dbConnectDetails` object
#' as an argument and use it to create a new connection.
#'
#' @param drv A DBI driver (e.g. `RPostgres::Postgres()`)
#' @param ... DBI Driver parameters needed to create a connection using DBI::dbConnect()
#'
#' @return A dbConnectArgs object that can be passed to dbConnect
#' @export
#'
#' @examples
#' \dontrun{
#' library(DBI)
#' connectionDetails <- dbConnectDetails(RPostgres::Postgres(),
#'                                       dbname = "cdm",
#'                                       host = "localhost",
#'                                       user = "postgres",
#'                                       password = Sys.getenv("password"))
#'
#'  selfContainedQuery <- function(dbConnectDetails) {
#'    con <- dbConnect(connectionDetails)
#'    on.exit(dbDisonnect(con))
#'    dbGetQuery(con, "select count(*) as n from synthea1k.person")
#'  }
#'
#' selfContainedQuery(connectionDetails)
#' }
#'
dbConnectDetails <- function(drv, ...) {
  result <- rlang::enquos(drv, ...)
  class(result) <- c("dbConnectDetails")
  result
}

#' Connect to a database using stored connection details
#'
#' Create connection details, save them in an object, and use them to connect
#' to a database using dbConnect later during program execution.
#'
#' @import DBI
#' @importMethodsFrom DBI dbConnect dbDisconnect
#' @param drv An dbConnectDetails object created by `dbConnectDetails()`
#' @return An S4 object that inherits from DBIConnection used to communicate with
#' the database engine.
#' @export
#' @examples
#' \dontrun{
#' library(DBI)
#' connectionDetails <- dbConnectDetails(RPostgres::Postgres(),
#'                                       dbname = "cdm",
#'                                       host = "localhost",
#'                                       user = "postgres",
#'                                       password = Sys.getenv("password"))
#'
#'  selfContainedQuery <- function(dbConnectDetails) {
#'    con <- dbConnect(connectionDetails)
#'    on.exit(dbDisonnect(con))
#'    dbGetQuery(con, "select count(*) as n from synthea1k.person")
#'  }
#'
#' selfContainedQuery(connectionDetails)
#' }
#'
setMethod("dbConnect", "dbConnectDetails", function(drv) {
  l <- purrr::map(drv, rlang::eval_tidy)
  rlang::inject(DBI::dbConnect(!!!l))
})




