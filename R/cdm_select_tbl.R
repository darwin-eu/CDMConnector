
#' Select a subset of tables in a cdm reference object
#'
#' This function uses syntax similar to `dplyr::select` and can be used to
#' subset a cdm reference object to a specific tables
#'
#' @param cdm A cdm reference object created by `cdm_from_con`
#' @param ... One or more table names of the tables of the `cdm` object.
#' `tidyselect` is supported, see `dplyr::select()` for details on the semantics.
#'
#' @return A cdm reference object containing the selected tables
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#'
#' cdm <- cdm_from_con(con, "main")
#'
#' cdm_select_tbl(cdm, person)
#' cdm_select_tbl(cdm, person, observation_period)
#' cdm_select_tbl(cdm, tbl_group("vocab"))
#' cdm_select_tbl(cdm, "person")
#'
#' DBI::dbDisconnect(con)
#' }
cdm_select_tbl <- function(cdm, ...) {
  tables <- names(cdm) %>% rlang::set_names(names(cdm))
  selected <- names(tidyselect::eval_select(rlang::quo(c(...)), data = tables))
  if (length(selected) == 0) {
    rlang::abort("No tables selected!")
  }

  tables_to_drop <- dplyr::setdiff(tables, selected)
  for (i in tables_to_drop) {
    cdm[i] <- NULL
  }
  cdm
}

