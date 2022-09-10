
#' Validation report for a CDM
#'
#' Print a short validation report for a cdm object. The validation includes
#' checking that column names are correct and that no tables are empty. A short
#' report is printed to the console. This function is meant for interactive use.
#'
#'
#' @param cdm A cdm reference object.
#'
#' @return Invisibly returns the cdm input
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con, cdm_tables = c("person", "observation_period"))
#' validate_cdm(cdm)
#' DBI::dbDisconnect(con)
#' }
validate_cdm <- function(cdm) {
  checkmate::assert_class(cdm, "cdm_reference")
  cli::cat_rule(glue::glue("CDM v5.4 validation (checking {length(cdm)} tables)"))
  validate_cdm_colnames(cdm)
  validate_cdm_rowcounts(cdm)
}

validate_cdm_colnames <- function(cdm) {
  any_dif <- FALSE
  for (nm in names(cdm)) {
      # spec_cdm_field is a a global internal package dataframe created in extras/package_maintenece.R
      expected_columns <- spec_cdm_field %>% dplyr::filter(.data$cdmTableName == nm) %>% dplyr::pull(.data$cdmFieldName)
      actual_columns <- cdm[[nm]] %>% head(1) %>% collect() %>% colnames()
      # actual_columns <- actual_columns[-1]
      dif <- waldo::compare(expected_columns,
                            actual_columns,
                            x_arg = glue::glue("{nm} table expected columns"),
                            y_arg = glue::glue("{nm} table actual_colums"),
                            ignore_attr = TRUE)

      if (length(dif) > 0) {
        print(dif)
        any_dif <- TRUE
      }
  }
  if (!any_dif) cli::cat_bullet("cdm table names", bullet = "tick", bullet_col = "green")
}

validate_cdm_rowcounts <- function(cdm) {
  nm <- names(cdm)
  rowcounts <- purrr::map_dbl(nm, function(.) dplyr::tally(cdm[[.]], name = "n") %>% dplyr::pull(.data$n)) %>%
    rlang::set_names(nm)

  empty_tables <- rowcounts[rowcounts == 0]
  if (length(empty_tables) > 0) {
    table_text <- cli::col_grey(paste(names(empty_tables), collapse = ', '))
    cli::cat_bullet(glue::glue("{length(empty_tables)} empty CDM tables: {table_text}"), bullet_col = "red")
  } else {
    cli::cat_bullet("all row counts > 0", bullet = "tick", bullet_col = "green")
  }
  invisible(cdm)
}
