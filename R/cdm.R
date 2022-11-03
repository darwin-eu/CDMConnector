
#' Create a CDM reference object from a database connection
#'
#' @param con A DBI database connection to a database where an OMOP CDM v5.4 instance is located.
#' @param cdm_schema The schema where the OMOP CDM tables are located. Defaults to NULL.
#' @param write_schema An optional schema in the CDM database that the user has write access to.
#' @param cdm_tables Which tables should be included? Supports a character vector, tidyselect selection helpers, or table groups.
#' \itemize{
#'   \item{tbl_group("all")}{all CDM tables}
#'   \item{tbl_group("vocab")}{the CDM vocabulary tables}
#'   \item{tbl_group("clinical")}{the clinical CDM tables}
#' }
#' @param cohort_tables A character vector listing the cohort table names to be included in the CDM object.
#' Cohort tables must be in the write_schema.
#' @return A list of dplyr database table references pointing to CDM tables
#' @importFrom dplyr all_of matches starts_with ends_with contains
#' @export
cdm_from_con <- function(con, cdm_schema = NULL, cdm_tables = tbl_group("default"), write_schema = NULL, cohort_tables = NULL) {
  checkmate::assert_class(con, "DBIConnection")
  checkmate::assert_true(DBI::dbIsValid(con))
  checkmate::assert_character(cdm_schema, null.ok = TRUE, min.len = 1, max.len = 2)
  checkmate::assert_character(write_schema, null.ok = TRUE, min.len = 1, max.len = 2)
  checkmate::assert_character(cohort_tables, null.ok = TRUE, min.len = 1)

  # tidyselect: https://tidyselect.r-lib.org/articles/tidyselect.html
  all_cdm_tables <- rlang::set_names(spec_cdm_table$cdmTableName, spec_cdm_table$cdmTableName)
  cdm_tables <- names(tidyselect::eval_select(rlang::enquo(cdm_tables), data = all_cdm_tables))

  if (dbms(con) == "duckdb") {
    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, paste(c(cdm_schema, .), collapse = ".")))
  } else if (is.null(cdm_schema)) {
    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, .))
  } else if (length(cdm_schema) == 1) {
    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, dbplyr::in_schema(cdm_schema, .)))
  } else if (length(cdm_schema) == 2) {
    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, dbplyr::in_catalog(cdm_schema[1], cdm_schema[2], .)))
  }
  names(cdm) <- cdm_tables

  if (!is.null(write_schema)) verify_write_access(con, write_schema = write_schema)

  if (!is.null(cohort_tables)) {
    if (dbms(con) == "duckdb") {
      ch <- purrr::map(cohort_tables, ~dplyr::tbl(con, paste(c(write_schema, .), collapse = ".")))
    } else if (is.null(write_schema)) {
      rlang::abort("write_schema not specified. Cohort tables must be in write_schema.")
    } else if (length(write_schema) == 1) {
      ch <- purrr::map(cohort_tables, ~dplyr::tbl(con, dbplyr::in_schema(write_schema, .)))
    } else if (length(write_schema) == 2) {
      ch <- purrr::map(cohort_tables, ~dplyr::tbl(con, dbplyr::in_catalog(write_schema[1], write_schema[2], .)))
    }
    names(ch) <- cohort_tables
    cdm <- c(cdm, ch)
  }

  class(cdm) <- "cdm_reference"
  attr(cdm, "cdm_schema") <- cdm_schema
  attr(cdm, "write_schema") <- write_schema
  attr(cdm, "dbcon") <- con
  cdm
}

#' Print a CDM reference object
#'
#' @param x A cdm_reference object
#' @param ... Included for compatibility with generic. Not used.
#'
#' @return Invisibly returns the input
#' @export
print.cdm_reference <- function(x, ...) {

  type <- class(x[[1]])[[1]]
  cli::cat_line(pillar::style_subtle(glue::glue("# OMOP CDM reference ({type})")))
  cli::cat_line("")
  cli::cat_line(paste("Tables:", paste(names(x), collapse = ", ")))
  invisible(x)
}

verify_write_access <- function(con, write_schema) {
  write_schema <- paste(write_schema, collapse = ".")
  tablename <- paste(c(sample(letters, 12, replace = TRUE), "_test_table"), collapse = "")
  tablename <- paste(write_schema, tablename, sep = ".")
  # spec_cdm_table is a global internal package dataframe
  DBI::dbWriteTable(con, DBI::SQL(tablename), spec_cdm_table[1:4,])
  to_compare <- DBI::dbReadTable(con, DBI::SQL(tablename))
  DBI::dbRemoveTable(con, DBI::SQL(tablename))
  if(!dplyr::all_equal(spec_cdm_table[1:4,], to_compare)) rlang::abort(paste("Write access to schema", write_schema, "could not be verified."))
  invisible(NULL)
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
#' concept_ancestor, concept_synonym, drug_strength
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
#' cdm <- cdm_from_con(con, cdm_tables = tbl_group("vocab"))
#' }
tbl_group <- function(group) {
  # groups are defined in the internal package dataframe called spec_cdm_table created by a script in the extras folder
  checkmate::assert_subset(group, c("vocab", "clinical", "all", "default", "derived"))
  purrr::map(group, ~spec_cdm_table[spec_cdm_table[[paste0("group_", .)]], ]$cdmTableName) %>% unlist() %>% unique()
}

#' Create a new Eunomia CDM
#'
#' Create a copy of the duckdb Eunomia CDM and return the file path
#'
#' @param exdir Enclosing directory where the Eunomia CDM should be created.
#' If NULL (default) then a temp folder is created.
#'
#' @return The full path to the new Eunomia CDM that can be passed to `dbConnect()`
#' @export
#' @importFrom utils untar
#' @examples
#' \dontrun{
#' library(DBI)
#' library(CDMConnector)
#' con <- dbConnect(duckdb::duckdb(), dbdir = getEunomiaPath())
#' dbListTables(con)
#' dbDisconnect(con)
#' }
eunomia_dir <- function(exdir = NULL) {
  if (utils::packageVersion("duckdb") != "0.5.1")
    rlang::abort("duckdb version 0.5.1 is required to use eunomia_dir(). \nPlease install the latest version of duckdb (0.5.1).")

  if (is.null(exdir)) exdir <- file.path(tempdir(TRUE), paste(sample(letters, 8, replace = TRUE), collapse = ""))
  file <- xzfile(system.file("duckdb", "cdm.duckdb.tar.xz", package = "CDMConnector"), open = "rb")
  untar(file, exdir = exdir)
  close(file)
  path <- file.path(exdir, "cdm.duckdb")
  if(!file.exists(path)) rlang::abort("Error creating Eunomia CDM")
  path
}


#' Get the database management system (dbms) from a cdm_reference or DBI connection
#'
#' @param con A DBI connection or cdm_reference
#'
#' @return A character string representing the dbms that can be used with SqlRender
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' cdm <- cdm_from_con(con)
#' dbms(cdm)
#' dbms(con)
#' }
dbms <- function(con) { UseMethod("dbms") }

#' @export
dbms.cdm_reference <- function(con) {
  dbms(attr(con, "dbcon"))
}

#' @export
dbms.DBIConnection <- function(con) {
  if(!is.null(attr(con, "dbms"))) return(attr(con, "dbms"))

  switch (class(con),
          'Microsoft SQL Server' = 'sql server',
          'PqConnection' = 'postgresql',
          'RedshiftConnection' = 'redshift',
          'BigQueryConnection' = 'bigquery',
          'SQLiteConnection' = 'sqlite',
          'duckdb_connection' = 'duckdb'
          # add mappings from various connection classes to dbms here
  )
}

#' Collect a list of lazy queries and save the results as files
#'
#' @param cdm A cdm object
#' @param path A folder to save the cdm object to
#' @param format The file format to use: "parquet", "csv", "feather".
#'
#' @return Invisibly returns the cdm input
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' vocab <- cdm_from_con(con, cdm_tables = c("concept", "concept_ancestor"))
#' stow(vocab, here::here("vocab_tables"))
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
stow <- function(cdm, path, format = "parquet") {
  checkmate::assert_class(cdm, "cdm_reference")
  checkmate::assert_choice(format, c("parquet", "csv", "feather"))
  path <- path.expand(path)
  checkmate::assert_true(file.exists(path))

  switch (format,
    parquet = purrr::walk2(cdm, names(cdm), ~arrow::write_parquet(dplyr::collect(.x), file.path(path, paste0(.y, ".parquet")))),
    csv = purrr::walk2(cdm, names(cdm), ~readr::write_csv(dplyr::collect(.x), file.path(path, paste0(.y, ".csv")))),
    feather = purrr::walk2(cdm, names(cdm), ~arrow::write_feather(dplyr::collect(.x), file.path(path, paste0(.y, ".feather")))),
  )
  invisible(cdm)
}

#' Create a CDM reference from a folder containing parquet, csv, or feather files
#'
#' @param path A folder where an OMOP CDM v5.4 instance is located.
#' @param cdm_tables Which tables should be included? Supports tidyselect and custom selection groups.
#' @param format What is the file format to be read in? Must be "auto" (default), "parquet", "csv", "feather".
#' @param as_data_frame TRUE (default) will read files into R as dataframes. FALSE will read files into R as Arrow Datasets.
#' @return A list of dplyr database table references pointing to CDM tables
#' @export
cdm_from_files <- function(path, cdm_tables = tbl_group("default"), format = "auto", as_data_frame = TRUE) {
  checkmate::assert_choice(format, c("auto", "parquet", "csv", "feather"))
  checkmate::assert_true(file.exists(path))

  path <- path.expand(path)

  files <- list.files(path, full.names = TRUE)

  if (format == "auto") {
    format <- unique(tools::file_ext(files))
    if (length(format) > 1) rlang::abort(paste("Multiple file formats detected:", paste(format, collapse = ", ")))
    checkmate::assert_choice(format, c("parquet", "csv", "feather"))
  }

  # tidyselect: https://tidyselect.r-lib.org/articles/tidyselect.html
  all_cdm_tables <- rlang::set_names(spec_cdm_table$cdmTableName, spec_cdm_table$cdmTableName)
  cdm_tables <- names(tidyselect::eval_select(rlang::enquo(cdm_tables), data = all_cdm_tables))

  cdm_table_files <- file.path(path, paste0(cdm_tables, ".", format))
  purrr::walk(cdm_table_files, function(.) checkmate::assert_file_exists(., "r"))

  cdm <- switch (format,
    parquet = purrr::map(cdm_table_files, function(.) arrow::read_parquet(., as_data_frame = as_data_frame)),
    csv = purrr::map(cdm_table_files, function(.) arrow::read_csv_arrow(., as_data_frame = as_data_frame)),
    feather = purrr::map(cdm_table_files, function(.) arrow::read_feather(., as_data_frame = as_data_frame))
  ) %>%
  magrittr::set_names(cdm_tables) %>%
  magrittr::set_class("cdm_reference")

  attr(cdm, "cdm_schema") <- NULL
  attr(cdm, "write_schema") <- NULL
  attr(cdm, "dbcon") <- NULL
  cdm
}

#' Bring a remote CDM reference into R
#'
#' This function calls collect on a list of lazy queries and returns
#' the result as a list of dataframes.
#'
#' @param x A cdm_reference object.
#' @param ... Not used. Included for compatibility.
#'
#' @return A cdm_reference object that is a list of R dataframes.
#' @importFrom dplyr collect
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' vocab <- cdm_from_con(con, cdm_tables = c("concept", "concept_ancestor"))
#' local_vocab <- collect(vocab)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
collect.cdm_reference <- function(x, ...) {
  for (nm in names(x)) {
    x[[nm]] <- dplyr::collect(x[[nm]])
  }
  x
}
