##' @importFrom dplyr collect
##' @export
NULL

#' Bring a remote CDM table into R
#'
#' This function calls collect on a lazy query and returns
#' the result as a dataframe, ensuring that the column names are in lower case.
#'
#' @param x A cdm_tbl object.
#' @param ... Not used. Included for compatibility.
#'
#' @return A cdm_tbl object that is a R dataframe.
#' @importFrom dplyr collect
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#'
#' local_concept <- collect(cdm$concept)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
collect.cdm_tbl <- function(x, ...) {
  # the only difference to dbplyr´s collect.tbl_sql is that at the end
  # we ensure column names are lower case after collecting
  dplyr::collect(x, ...) %>%
    dplyr::rename_all(tolower)
}
