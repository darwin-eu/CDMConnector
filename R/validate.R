
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
#' cdm <- cdm_from_con(con, cdm_schema = "main")
#' validate_cdm(cdm)
#' DBI::dbDisconnect(con)
#' }
validate_cdm <- function(cdm) {
  checkmate::assert_class(cdm, "cdm_reference")
  if (is.null(cdmCon(cdm))) {
    rlang::abort("validate_cdm is not implement for local cdms")
  }

  cli::cat_rule(
    glue::glue("CDM v{version(cdm)} validation (checking {length(cdm)} tables)")
  )
  validate_cdm_colnames(cdm)
  validate_cdm_rowcounts(cdm)
}

#' @export
#' @rdname validate_cdm
validateCdm <- validate_cdm

validate_cdm_colnames <- function(cdm) {
  # local option needed for pull with arrow
  withr::local_options(list(arrow.pull_as_vector = TRUE))
  any_dif <- FALSE
  ver <- attr(cdm, "cdm_version")
  for (nm in names(cdm)) {
      # spec_cdm_field is a a global internal package dataframe created in
      # the file extras/package_maintenance.R
      expected_columns <- spec_cdm_field[[ver]] %>%
        dplyr::filter(.data$cdmTableName == nm) %>%
        dplyr::pull(.data$cdmFieldName)
      actual_columns <- cdm[[nm]] %>% head(1) %>% dplyr::collect() %>% colnames()

      dif <- waldo::compare(expected_columns,
                            actual_columns,
                            x_arg = glue::glue("{nm} table expected columns"),
                            y_arg = glue::glue("{nm} table actual_colums"),
                            ignore_attr = TRUE)

      if (length(dif) > 0) {
        print(dif, n = 100)
        any_dif <- TRUE
      }
  }
  if (!any_dif) {
    cli::cat_bullet("cdm field names are correct",
                    bullet = "tick",
                    bullet_col = "green")
  }
}

validate_cdm_rowcounts <- function(cdm) {
  # arrow.pull_as_vector option needed for dplyr::pull with arrow
  withr::local_options(list(arrow.pull_as_vector = TRUE))
  nm <- names(cdm)
  rowcounts <- purrr::map_dbl(nm, function(.) {
    dplyr::tally(cdm[[.]], name = "n") %>%
      dplyr::pull(.data$n)
    }) %>%
    rlang::set_names(nm)

  empty_tables <- rowcounts[rowcounts == 0]
  if (length(empty_tables) > 0) {
    table_text <- cli::col_grey(paste(names(empty_tables), collapse = ", "))
    s <- ifelse(length(empty_tables) > 1, "s", "")
    cli::cat_bullet(
      glue::glue("{length(empty_tables)} empty CDM table{s}: {table_text}"),
      bullet_col = "red")
  } else {
    cli::cat_bullet("all row counts > 0", bullet = "tick", bullet_col = "green")
  }
  invisible(cdm)
}

#' Assert that tables exist in a cdm object
#'
#' A cdm object is a list of references to a subset of tables in the
#' OMOP Common Data Model.
#' If you write a function that accepts a cdm object as a parameter
#' `assert_tables`/`assertTables` will help you check that the tables you need
#' are in the cdm object, have the correct columns/fields,
#' and (optionally) are not empty.
#'
#' @param cdm A cdm object
#' @param tables A character vector of table names to check.
#' @param empty.ok Should an empty table (0 rows) be considered an error?
#' TRUE or FALSE (default)
#' @param add An optional AssertCollection created by
#' `checkmate::makeAssertCollection()` that errors should be added to.
#'
#' @return Invisibly returns the cdm object
#' @importFrom rlang .env .data
#' @export
#'
#' @examples
#' \dontrun{
#' # Use assertTables inside a function to check that tables exist
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
  checkmate::assertCharacter(tables,
                             min.len = 1,
                             min.chars = 1,
                             null.ok = FALSE)
  checkmate::assertClass(cdm, "cdm_reference")
  withr::local_options(list(arrow.pull_as_vector = TRUE))

  ver <- attr(cdm, "cdm_version")

  missingTables <- tables[!(tables %in% names(cdm))]
  existingTables <- tables[tables %in% names(cdm)]

  if (length(missingTables) > 0) {
    s <- ifelse(length(missingTables) > 1, "s", "")
    misstbls <- paste(missingTables, collapse = ', ' )
    msg <- glue::glue("- {misstbls} table{s} not found in cdm object")
    if (is.null(add)) rlang::abort(msg) else add$push(msg)
  }

  # checking of column names will not throw an error if column names exist but
  # are in the wrong order
  for (nm in existingTables) {
    # spec_cdm_field is global internal package data (list of dataframes)
    # created in extras/package_maintenance.R
    expectedColumns <- spec_cdm_field[[ver]] %>%
      dplyr::filter(.data$cdmTableName == .env$nm) %>%
      dplyr::pull(.data$cdmFieldName)
    actualColumns <- cdm[[nm]] %>% head(1) %>% dplyr::collect() %>% colnames()
    missingColumns <- dplyr::setdiff(expectedColumns, actualColumns)

    if (length(missingColumns) > 0) {
      s <- ifelse(length(missingColumns) > 1, "s", "")
      misscols <- paste(missingColumns, collapse = ", ")
      msg <- glue::glue("- {misscols} column{s} not found in cdm table {nm}")
      if (is.null(add)) rlang::abort(msg) else add$push(msg)
    }
  }

  if (!empty.ok) {
    rowcounts <- purrr::map_dbl(existingTables, function(.) {
      dplyr::tally(cdm[[.]], name = "n") %>%
        dplyr::pull(.data$n)
      }) %>%
      rlang::set_names(existingTables)

    empty_tables <- rowcounts[rowcounts == 0]

    if (length(empty_tables) > 0) {
      s <- ifelse(length(empty_tables) > 1, "s are", " is")
      emptytbls <- paste(names(empty_tables), collapse = ", ")
      msg <- glue::glue("- {emptytbls} cdm table{s} empty")
      if (is.null(add)) rlang::abort(msg) else add$push(msg)
    }
  }

  invisible(cdm)
}


#' @export
#' @rdname assert_tables
assertTables <- assert_tables


#' Assert that cdm has a writable schema
#'
#' A cdm object can optionally contain a single schema in a database with
#' write access. assert_write_schema checks that the cdm contains the
#' "write_schema" attribute and tests that local dataframes can be written
#' to tables in this schema.
#'
#' @param cdm A cdm object
#' @param add An optional AssertCollection created by
#' `checkmate::makeAssertCollection()` that errors should be added to.
#'
#' @return Invisibly returns the cdm object
#' @export
assert_write_schema <- function(cdm, add = NULL) {
  checkmate::assert_class(cdm, "cdm_reference")
  if (is.null(cdmCon(cdm))) {
    rlang::abort("Local cdm objects do not have a write schema.")
  }
  write_schema <- cdmWriteSchema(cdm)
  checkmate::assert_character(write_schema, min.len = 1, max.len = 3, min.chars = 1, add = add)
  verify_write_access(cdmCon(cdm),
                      write_schema = write_schema,
                      add = add)
  invisible(cdm)
}


#' @rdname assert_write_schema
#' @export
assertWriteSchema <- assert_write_schema
