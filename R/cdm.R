
#' Create a CDM reference reference to a database connection
#'
#' @param con A DBI database connection to a database where an OMOP CDM v5.4 instance is located.
#' @param schema The schema where the vocab tables are located. Defaults to NULL.
#' @param select Which tables should be included?
#' "all" for all CDM tables
#' "vocab" for just the vocabulary tables
#' "clinical" for just the clinical tables
#'.
#'
#' @return A list of dplyr database table references pointing to CDM tables
#' @export
cdm_from_con <- function(con, cdm_schema = NULL, select = tbl_group("default"), write_schema = NULL) {

  checkmate::assert_class(con, "DBIConnection")
  checkmate::assert_true(DBI::dbIsValid(con))
  checkmate::assert_character(cdm_schema, null.ok = TRUE)
  checkmate::assert_character(write_schema, null.ok = TRUE)

  cdm_tables <- select_cdm_tables(select)

  # TODO should this throw an error if the table does not exist?
  if (!is.null(cdm_schema)) {
    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, dbplyr::in_schema(cdm_schema, .)))
  } else {
    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, .))
  }

  cdm <- cdm %>%
    magrittr::set_names(cdm_tables) %>%
    magrittr::set_class("cdm_reference") %>%
    assert_column_names()

  if(!is.null(write_schema)) verify_write_access(con, write_schema)

  attr(cdm, "cdm_schema") <- cdm_schema
  attr(cdm, "write_schema") <- write_schema
  attr(cdm, "con") <- con
  cdm
}

#' Print a CDM reference object
#'
#' @param x A cdm_reference object
#' @param ... Included for compatiblity with generic. Not used.
#'
#' @return Invisibly returns the input
#' @export
print.cdm_reference <- function(x, ...) {
  cli::cat_line(pillar::style_subtle("# OMOP CDM reference"))
  cli::cat_line("")
  cli::cat_line(paste("Tables:", paste(names(x), collapse = ", ")))
  invisible(x)
}

verify_write_access <- function(con, write_schema) {
  tablename <- paste(write_schema, paste(c(sample(letters, 10, replace = TRUE), "_cdm_tables"), collapse = ""), sep = ".")
  DBI::dbWriteTable(con, DBI::SQL(tablename), cdm_tables)
  to_compare <- DBI::dbReadTable(con, DBI::SQL(tablename))
  DBI::dbRemoveTable(con, DBI::SQL(tablename))
  if(!dplyr::all_equal(cdm_tables, to_compare)) rlang::abort(paste("Write access to schema", write_schema, "could not be verified."))
  invisible(NULL)
}

assert_column_names <- function(cdm) {
  # TODO how strict do we want to be here? Need to be clear on versioning.
  # for (nm in names(cdm)) {
  #   expected_columns <- cdm_fields %>% dplyr::filter(.data$cdmTableName == nm) %>% dplyr::pull(.data$cdmFieldName)
  #   # require all expected columns are present but allow for additional columns
  #   checkmate::assert_subset(expected_columns, colnames(cdm[[nm]]))
  # }
  return(cdm)
}

select_cdm_tables <- function(select) {
  data <- rlang::set_names(cdm_tables$cdmTableName, cdm_tables$cdmTableName)
  expr <- rlang::enquo(select)
  i <- tidyselect::eval_select(expr, data = data)
  names(i)
}

#' CDM table selection helper
#'
#' The OMOP CDM tables are grouped together and the `tbl_group` function allows users to easily create a
#' CDM reference including one or more table groups.
#'
#' \figure{cdm54.png}{CDM 5.4}
#'
#' The "default" table group is meant to capture the most commonly used set of CDM tables.
#' Currently the "default" group is: person, observation_period, visit_occurrence,
#' visit_detail, condition_occurrence, drug_exposure, procedure_occurrence,
#' device_exposure, measurement, observation, death, note, note_nlp, specimen,
#' fact_relationship, location, care_site, provider, payer_plan_period,
#' cost, drug_era, dose_era, condition_era, concept, vocabulary, concept_relationship,
#' concept_ancestor, drug_strength
#'
#' @param group A character vector of CDM table groups: "vocab", "clinical", "all", "default", "derived".
#'
#' @return A character vector of CDM tables names in the groups
#' @export
#'
#' @examples
#' \dontrun{
# con <- DBI::dbConnect(RPostgres::Postgres(),
#                       dbname = "cdm",
#                       host = "localhost",
#                       user = "postgres",
#                       password = Sys.getenv("PASSWORD"))
#'
#' cdm <- cdm_from_con(con, select = tbl_group("vocab"))
#' }
tbl_group <- function(group) {
  # groups are defined in the internal package dataframe called cdm tables created by a script in the extras folder
  checkmate::assert_subset(group, c("vocab", "clinical", "all", "default", "derived"))
  purrr::map(group, ~cdm_tables[cdm_tables[[paste0("group_", .)]], ]$cdmTableName) %>% unlist() %>% unique()
}

#' Create a new Eunomia CDM
#'
#' Create a copy of the duckdb Eunomia CDM and return the file path
#'
#' @param exdir Enclosing directory where the Eunomia CDM should be created
#'
#' @return The full path to the new Eunomia CDM that can be passed to `dbConnect()`
#' @export
#'
#' @examples
#' \dontrun{
#' library(DBI)
#' library(CDMConnector)
#' con <- dbConnect(duckdb::duckdb(), dbdir = getEunomiaPath())
#' dbListTables(con)
#' dbDisconnect(con)
#' }
eunomia_dir <- function(exdir = tempdir()) {
  file <- xzfile(system.file("duckdb", "cdm.duckdb.tar.xz", package = "CDMConnector"), open = "rb")
  untar(file, exdir = exdir)
  close(file)
  path <- file.path(exdir, "cdm.duckdb")
  if(!file.exists(path)) abort("Error creating Eunomia CDM")
  path
}
