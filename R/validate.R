
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
  if(is.null(attr(cdm, "dbcon"))) rlang::abort("validate_cdm is not implement for local cdms")

  cli::cat_rule(glue::glue("CDM v5.4 validation (checking {length(cdm)} tables)"))
  validate_cdm_colnames(cdm)
  validate_cdm_rowcounts(cdm)
}

#' @export
#' @describeIn validate_cdm camelCase alias
validateCdm <- validate_cdm

validate_cdm_colnames <- function(cdm) {
  withr::local_options(list(arrow.pull_as_vector = TRUE)) # needed for pull with arrow
  any_dif <- FALSE
  ver <- attr(cdm, "cdm_version")
  for (nm in names(cdm)) {
      # spec_cdm_field is a a global internal package dataframe created in extras/package_maintenece.R
      expected_columns <- spec_cdm_field[[ver]] %>% dplyr::filter(.data$cdmTableName == nm) %>% dplyr::pull(.data$cdmFieldName)
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
  withr::local_options(list(arrow.pull_as_vector = TRUE)) # needed for pull with arrow
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

#' Assert that tables exist in a cdm object
#'
#' A cdm object is a list of references to a subset of tables in the OMOP Common Data Model.
#' If you write a function that accepts a cdm object as a parameter `assert_tables`/`assertTables` will help you check
#' that the tables you need are in the cdm object, have the correct columns/fields, and (optionally) are not empty.
#'
#' @param cdm A cdm object
#' @param tables A character vector of table names to check.
#' @param empty.ok Should an empty table (0 rows) be considered an error? TRUE or FALSE (default)
#' @param add An optional AssertCollection created by `checkmate::makeAssertCollection()` that errors should be added to.
#'
#' @return Invisibly returns the cdm object
#' @importFrom rlang .env .data
#' @export
#'
#' @examples
#' \dontrun{
#' # Use assertTables inside an analysis function to check that tables exist and are populated
#' countDrugsByGender <- function(cdm) {
#'   assertTables(cdm, tables = c("person", "drug_era"), empty.ok = FALSE)
#'
#'   cdm$person %>%
#'     dplyr::inner_join(cdm$drug_era, by = "person_id") %>%
#'     dplyr::count(.data$gender_concept_id, .data$drug_concept_id) %>%
#'     dplyr::collect()
#' }
#'
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' cdm <- cdm_from_con(con)
#'
#' countDrugsByGender(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#'
#' }
assert_tables <- function(cdm, tables, empty.ok = FALSE, add = NULL) {
  checkmate::assertClass(add, "AssertCollection", null.ok = TRUE)
  checkmate::assertLogical(empty.ok, len = 1, null.ok = FALSE)
  checkmate::assertCharacter(tables, min.len = 1, min.chars = 1, null.ok = FALSE)
  checkmate::assertClass(cdm, "cdm_reference")
  withr::local_options(list(arrow.pull_as_vector = TRUE))

  ver <- attr(cdm, "cdm_version")

  missingTables <- tables[!(tables %in% names(cdm))]
  existingTables <- tables[tables %in% names(cdm)]

  if (length(missingTables) > 0) {
    s <- ifelse(length(missingTables) > 1, "s", "")
    msg <- glue::glue("- {paste(missingTables, collapse = ', ' )} table{s} not found in cdm object")
    if (is.null(add)) rlang::abort(msg) else add$push(msg)
  }

  # checking of column names will not throw an error if column names exist but are in the wrong order
  for (nm in existingTables) {
    # spec_cdm_field is global internal package data (list of dataframes) created in extras/package_maintenance.R
    expectedColumns <- spec_cdm_field[[ver]] %>% dplyr::filter(.data$cdmTableName == .env$nm) %>% dplyr::pull(.data$cdmFieldName)
    actualColumns <- cdm[[nm]] %>% head(1) %>% collect() %>% colnames()
    missingColumns <- dplyr::setdiff(expectedColumns, actualColumns)

    if (length(missingColumns) > 0) {
      s <- ifelse(length(missingColumns) > 1, "s", "")
      msg <- glue::glue("- {paste(missingColumns, collapse = ', ' )} column{s} not found in cdm table {nm}")
      if (is.null(add)) rlang::abort(msg) else add$push(msg)
    }
  }

  if (!empty.ok) {
    rowcounts <- purrr::map_dbl(existingTables, function(.) dplyr::tally(cdm[[.]], name = "n") %>% dplyr::pull(.data$n)) %>%
      rlang::set_names(existingTables)

    empty_tables <- rowcounts[rowcounts == 0]

    if (length(empty_tables) > 0) {
      s <- ifelse(length(empty_tables) > 1, "s are", " is")
      msg <- glue::glue("- {paste(names(empty_tables), collapse = ', ')} cdm table{s} empty")
      if (is.null(add)) rlang::abort(msg) else add$push(msg)
    }
  }

  invisible(cdm)
}

#' @export
#' @describeIn assert_tables camelCase alias
assertTables <- assert_tables

#' Assert that cdm has a writable schema
#'
#' A cdm object can optionally contain a single schema in a database with write access.
#' assert_write_schema checks that the cdm contains the "write_schema" attribute and
#' tests that local dataframes can be written to tables in this schema.
#'
#' @param cdm A cdm object
#' @param add An optional AssertCollection created by `checkmate::makeAssertCollection()` that errors should be added to.
#'
#' @return Invisibly returns the cdm object
#' @export
assert_write_schema <- function(cdm, add = NULL) {
  checkmate::assert_class(cdm, "cdm_reference")
  if (is.null(attr(cdm, "dbcon"))) rlang::abort("Local cdm objects do not have a write schema.")
  write_schema <- attr(cdm, "write_schema")
  checkmate::assert_character(write_schema, len = 1, min.chars = 1, add = add)
  verify_write_access(attr(cdm, "dbcon"), write_schema = write_schema, add = add)
  invisible(cdm)
}

#' @export
#' @describeIn assert_write_schema camelCase alias
assertWriteSchema <- assert_write_schema


