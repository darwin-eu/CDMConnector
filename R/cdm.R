#' Create a CDM reference object from a database connection
#'
#' @param con A DBI database connection to a database where an OMOP CDM v5.4 or
#'   v5.3 instance is located.
#' @param cdm_schema,cdmSchema The schema where the OMOP CDM tables are located. Defaults
#'   to NULL.
#' @param write_schema,writeSchema An optional schema in the CDM database that the user has
#'   write access to.
#' @param cohort_tables,cohortTables A character vector listing the cohort table names to be
#'   included in the CDM object.
#' @param cdm_version,cdmVersion The version of the OMOP CDM: "5.3" (default), "5.4",
#'   "auto". "auto" attempts to automatically determine the cdm version using
#'   heuristics. Cohort tables must be in the write_schema.
#' @param cdm_name,cdmName The name of the CDM. If NULL (default) the cdm_source_name
#'.  field in the CDM_SOURCE table will be used.
#'
#' @return A list of dplyr database table references pointing to CDM tables
#' @importFrom dplyr all_of matches starts_with ends_with contains
#' @export
cdm_from_con <- function(con,
                         cdm_schema = NULL,
                         write_schema = NULL,
                         cohort_tables = NULL,
                         cdm_version = "5.3",
                         cdm_name = NULL) {

  cdm_tables <- tbl_group("all")

  checkmate::assert_true(methods::is(con, "DBIConnection") || methods::is(con, "Pool"))

  if (methods::is(con, "Pool")) {
    con <- pool::localCheckout(con)
  }

  checkmate::assert_true(.dbIsValid(con))

  if (dbms(con) %in% c("duckdb", "sqlite") && is.null(cdm_schema)) {
    cdm_schema = "main"
  }

  checkmate::assert_character(cdm_schema, min.len = 1, max.len = 3)
  checkmate::assert_character(write_schema, min.len = 1, max.len = 3, null.ok = TRUE)

  checkmate::assert_character(cohort_tables, null.ok = TRUE, min.len = 1)
  checkmate::assert_choice(cdm_version, choices = c("5.3", "5.4", "auto"))
  checkmate::assert_character(cdm_name, null.ok = TRUE)

  if (cdm_version == "auto") {
    cdm_version <- detect_cdm_version(con, cdm_schema = cdm_schema)
  }

  # Try to get the cdm name if not supplied
  dbTables <- listTables(con, schema = cdm_schema)
  if (is.null(cdm_name) && ("cdm_source" %in% tolower(dbTables))) {
    if ("cdm_source" %in% dbTables) {
      cdm_source <- dplyr::tbl(con, inSchema(cdm_schema, "cdm_source", dbms(con)))
    } else if ("CDM_SOURCE" %in% dbTables) {
      cdm_source <- dplyr::tbl(con, inSchema(cdm_schema, "CDM_SOURCE", dbms(con)))
    }

    cdm_source <- cdm_source %>%
      head() %>%
      dplyr::collect() %>%
      dplyr::rename_all(tolower)

    cdm_name <- dplyr::coalesce(cdm_source$cdm_source_name[1],
                                cdm_source$cdm_source_abbreviation[1])
  }

  if (is.null(cdm_name)) {
    rlang::abort("cdm_name must be supplied!")
  }

  # only get the cdm tables that exist in the database
  cdm_tables <- cdm_tables[which(cdm_tables %in% tolower(dbTables))]
  if (length(cdm_tables) == 0) {
    rlang::abort("There were no cdm tables found in the cdm_schema!")
  }

  # Handle uppercase table names in the database
  if (all(dbTables == toupper(dbTables))) {
    cdm_tables <- toupper(cdm_tables)
  } else if (!all(dbTables == tolower(dbTables))) {
    rlang::abort("CDM database tables should be either all upppercase or all lowercase!")
  }

  cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, inSchema(cdm_schema, ., dbms(con)), check_from = FALSE) %>%
                    dplyr::rename_all(tolower)) %>%
    rlang::set_names(tolower(cdm_tables))

  if (!is.null(write_schema)) {
    verify_write_access(con, write_schema = write_schema)
  }

  if (dbms(con) == "snowflake") {

    s <- write_schema %||% cdm_schema

    # Assign temp table schema
    if ("prefix" %in% names(s)) {
      s <- s[names(s) != "prefix"]
    }

    if ("catalog" %in% names(s)) {
      stopifnot("schema" %in% names(s))
      s <- c(unname(s["catalog"]), unname(s["schema"]))
    }

    if (length(s) == 2) {
      s2 <- glue::glue_sql("{DBI::dbQuoteIdentifier(con, s[1])}.{DBI::dbQuoteIdentifier(con, s[2])}")
    } else {
      s2 <- DBI::dbQuoteIdentifier(con, s[1])
    }

    DBI::dbExecute(con, glue::glue_sql("USE SCHEMA {s2}"))
  }

  # add class info -----

  # TODO use a cdm_reference constructor function
  class(cdm) <- "cdm_reference"
  attr(cdm, "cdm_schema") <- cdm_schema
  attr(cdm, "write_schema") <- write_schema
  attr(cdm, "dbcon") <- con
  attr(cdm, "cdm_version") <- cdm_version
  attr(cdm, "cdm_name") <- cdm_name

  # Add existing GeneratedCohortSet objects to cdm object
  if (!is.null(cohort_tables)) {
    if (is.null(write_schema)) {
      rlang::abort("write_schema is required when using cohort_tables")
    }

    write_schema_tables <- listTables(con, schema = write_schema)

    for (cohort_table in cohort_tables) {

      # A generated cohort set object has tables: {cohort}
      # and attribute tables {cohort}_set, {cohort}_attrition, {cohort}_count
      # Only {cohort}_attrition is optional. The others will be created if not supplied.
      nms <- paste0(cohort_table, c("", "_set", "_count", "_attrition"))
      x <- purrr::map(nms, function(nm) {
        if (nm %in% write_schema_tables) {
          dplyr::tbl(con, inSchema(write_schema, nm, dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else if (nm %in% toupper(write_schema_tables)) {
          dplyr::tbl(con, inSchema(write_schema, toupper(nm), dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else {
          NULL
        }
      })

      cohort_ref <- x[[1]]
      cohort_set_ref <- x[[2]]
      cohort_count_ref <- x[[3]]
      cohort_attrition_ref <- x[[4]]

      if (is.null(cohort_ref)) {
        rlang::abort(glue::glue("cohort table `{cohort_table}` not found!"))
      }

      if (is.null(cohort_set_ref)) {
        # create the required cohort_set table
        cohort_set_ref <- cohort_ref %>%
          dplyr::distinct(.data$cohort_definition_id) %>%
          dplyr::mutate(cohort_name = paste("cohort", as.integer(.data$cohort_definition_id))) %>%
          computeQuery(name = paste0(cohort_table, "_set"),
                       schema = write_schema,
                       temporary = FALSE,
                       overwrite = TRUE)
      }

      if (is.null(cohort_count_ref)) {
        # create the required cohort_count table
        cohort_count_ref <- cohort_ref %>%
          dplyr::ungroup() %>%
          dplyr::group_by(.data$cohort_definition_id) %>%
          dplyr::summarise(number_records = dplyr::n(),
                           number_subjects = dplyr::n_distinct(.data$subject_id)) %>%
          computeQuery(name = paste0(cohort_table, "_count"),
                       schema = write_schema,
                       temporary = FALSE,
                       overwrite = TRUE)
      }

      # Note: use name without prefix (i.e. `cohort_tables[i]`) in the cdm object
      cdm[[cohort_table]] <- cohort_ref
      class(cdm) <- "cdm_reference"

      cdm[[cohort_table]] <- new_generated_cohort_set(
        cohort_ref = cdm[[cohort_table]],
        cohort_set_ref = cohort_set_ref,
        cohort_count_ref = cohort_count_ref,
        cohort_attrition_ref = cohort_attrition_ref)
    }
  }

  # add "cdm_tbl" as a class of every table in our cdm reference
  # cdm <- lapply(cdm, function(x) {
  #   class(x) <- c("cdm_tbl", class(x))
  #   return(x)
  # })

  # TODO use a cdm_reference constructor function
  # class(cdm) <- "cdm_reference"
  attr(cdm, "cdm_schema") <- cdm_schema
  attr(cdm, "write_schema") <- write_schema
  attr(cdm, "dbcon") <- con
  attr(cdm, "cdm_version") <- cdm_version
  attr(cdm, "cdm_name") <- cdm_name

  return(cdm)
}

#' @rdname cdm_from_con
#' @export
cdmFromCon <- function(con,
                       cdmSchema = NULL,
                       writeSchema = NULL,
                       cohortTables = NULL,
                       cdmVersion = "5.3",
                       cdmName = NULL) {
  cdm_from_con(
    con = con,
    cdm_schema = cdmSchema,
    write_schema = writeSchema,
    cohort_tables = cohortTables,
    cdm_version = cdmVersion,
    cdm_name = cdmName
  )
}

detect_cdm_version <- function(con, cdm_schema = NULL) {
  cdm_tables <- c("visit_occurrence", "cdm_source", "procedure_occurrence")

  if (!all(cdm_tables %in% listTables(con, schema = cdm_schema))) {
    rlang::abort(paste0(
      "The ",
      paste(cdm_tables, collapse = ", "),
      " tables are required for auto-detection of cdm version."
    ))
  }

  cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, inSchema(cdm_schema, ., dbms(con))) %>%
                      dplyr::rename_all(tolower)) %>%
    rlang::set_names(tolower(cdm_tables))

  # Try a few different things to figure out what the cdm version is
  visit_occurrence_names <- cdm$visit_occurrence %>%
    head() %>%
    collect() %>%
    colnames() %>%
    tolower()

  if ("admitting_source_concept_id" %in% visit_occurrence_names) {
    return("5.3")
  }

  if ("admitted_from_concept_id" %in% visit_occurrence_names) {
    return("5.4")
  }

  procedure_occurrence_names <- cdm$procedure_occurrence %>%
    head() %>%
    collect() %>%
    colnames() %>%
    tolower()

  if ("procedure_end_date" %in% procedure_occurrence_names) {
    return("5.4")
  }

  cdm_version <- cdm$cdm_source %>% dplyr::pull(.data$cdm_version)
  if (isTRUE(grepl("5\\.4", cdm_version))) return("5.4")

  if (isTRUE(grepl("5\\.3", cdm_version))) return("5.3")

  if ("episode" %in% listTables(con, schema = cdm_schema)) {
    return("5.4")
  } else {
    return("5.3")
  }
}

#' Get the CDM version
#'
#' Extract the CDM version attribute from a cdm_reference object
#'
#' @param cdm A cdm object
#'
#' @return "5.3" or "5.4"
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#' version(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
version <- function(cdm) {
  checkmate::assert_class(cdm, "cdm_reference")
  versionNumber <- attr(cdm, "cdm_version")
  if (!(versionNumber %in% c("5.3", "5.4"))) {
    rlang::abort("cdm object version attribute is not 5.3 or 5.4.
                 Contact the maintainer.")
  }
  return(versionNumber)
}

#' Get the CDM name
#'
#' Extract the CDM name attribute from a cdm_reference object
#'
#' @param cdm A cdm object
#'
#' @return The name of the CDM as a character string
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#' cdmName(cdm)
#' #> [1] "Synthea synthetic health database"
#'
#' cdm <- cdm_from_con(con, "main", cdm_name = "Example CDM")
#' cdmName(cdm)
#' #> [1] "Example CDM"
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmName <- function(cdm) {
  checkmate::assert_class(cdm, "cdm_reference")
  return(attr(cdm, "cdm_name"))
}


#' @rdname cdmName
#' @export
cdm_name <- cdmName

#' Print a CDM reference object
#'
#' @param x A cdm_reference object
#' @param ... Included for compatibility with generic. Not used.
#'
#' @return Invisibly returns the input
#' @export
print.cdm_reference <- function(x, ...) {
  type <- class(x[[1]])[[1]]
  cli::cat_line(glue::glue("# OMOP CDM reference ({type})"))
  cli::cat_line("")
  cli::cat_line(paste("Tables:", paste(names(x), collapse = ", ")))
  invisible(x)
}

# con = database connection
# write_schema = schema with write access
# add = checkmate collection
verify_write_access <- function(con, write_schema, add = NULL) {

  checkmate::assert_character(
    write_schema,
    min.len = 1,
    max.len = 3,
    min.chars = 1,
    any.missing = FALSE
  )

  checkmate::assert_class(add, "AssertCollection", null.ok = TRUE)
  checkmate::assert_true(.dbIsValid(con))

  tablename <- paste(c(sample(letters, 5, replace = TRUE), "_test_table"), collapse = "")
  df1 <- data.frame(chr_col = "a", numeric_col = 1, stringsAsFactors = FALSE)

  # Note: ROracle does not support integer round trip
  suppressMessages(
    DBI::dbWriteTable(con,
                      name = inSchema(schema = write_schema, table = tablename, dbms = dbms(con)),
                      value = df1,
                      overwrite = TRUE)
  )

  withr::with_options(list(databaseConnectorIntegerAsNumeric = FALSE), {
    df2 <- dplyr::tbl(con, inSchema(write_schema, tablename, dbms = dbms(con))) %>%
      dplyr::collect() %>%
      as.data.frame() %>%
      dplyr::rename_all(tolower) %>% # dbWriteTable can create uppercase column names on snowflake
      dplyr::select("chr_col", "numeric_col") # bigquery can reorder columns
  })

  DBI::dbRemoveTable(con, inSchema(write_schema, tablename, dbms = dbms(con)))

  if (!isTRUE(all.equal(df1, df2))) {
    msg <- paste("Write access to schema", write_schema, "could not be verified.")

    if (is.null(add)) {
      rlang::abort(msg)
    } else {
      add$push(msg)
    }
  }
  invisible(NULL)
}

#' CDM table selection helper
#'
#' The OMOP CDM tables are grouped together and the `tbl_group` function allows
#' users to easily create a CDM reference including one or more table groups.
#'
#' {\figure{cdm54.png}{options: width="100\%" alt="CDM 5.4"}}
#'
#' The "default" table group is meant to capture the most commonly used set
#' of CDM tables. Currently the "default" group is: person,
#' observation_period, visit_occurrence,
#' visit_detail, condition_occurrence, drug_exposure, procedure_occurrence,
#' device_exposure, measurement, observation, death, note, note_nlp, specimen,
#' fact_relationship, location, care_site, provider, payer_plan_period,
#' cost, drug_era, dose_era, condition_era, concept, vocabulary,
#' concept_relationship, concept_ancestor, concept_synonym, drug_strength
#'
#' @param group A character vector of CDM table groups: "vocab", "clinical",
#' "all", "default", "derived".
#'
#' @return A character vector of CDM tables names in the groups
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(RPostgres::Postgres(),
#'                       dbname = "cdm",
#'                       host = "localhost",
#'                       user = "postgres",
#'                       password = Sys.getenv("PASSWORD"))
#'
#' cdm <- cdm_from_con(con) %>%
#'   cdm_select_tbl(tbl_group("vocab"))
#' }
tbl_group <- function(group) {
  # groups are defined in the internal package dataframe called spec_cdm_table
  # created by a script in the extras folder
  checkmate::assert_subset(group, c("vocab", "clinical", "all", "default", "derived"))
  # use v5.3 here. The set of table groups between 5.3 and 5.4 are the same.
  spec <- spec_cdm_table[["5.3"]]
  purrr::map(group, ~ spec[spec[[paste0("group_", .)]], ]$cdmTableName) %>%
    unlist() %>%
    unique()
}

#' @export
#' @rdname tbl_group
tblGroup <- tbl_group

#' Get the database management system (dbms) from a cdm_reference or DBI
#' connection
#'
#' @param con A DBI connection or cdm_reference
#'
#' @return A character string representing the dbms that can be used with
#'   SqlRender
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' cdm <- cdm_from_con(con)
#' dbms(cdm)
#' dbms(con)
#' }
dbms <- function(con) {

  if (methods::is(con, "cdm_reference")) {
    con <- attr(con, "dbcon")
  } else if (methods::is(con, "Pool")) {
    con <- pool::localCheckout(con)
  }

  checkmate::assertClass(con, "DBIConnection")

  if (!is.null(attr(con, "dbms"))) {
    return(attr(con, "dbms"))
  }

  result <- switch(
    class(con),
    "Microsoft SQL Server" = "sql server",
    "PqConnection" = "postgresql",
    "RedshiftConnection" = "redshift",
    "BigQueryConnection" = "bigquery",
    "SQLiteConnection" = "sqlite",
    "duckdb_connection" = "duckdb",
    "Spark SQL" = "spark",
    "OraConnection" = "oracle",
    "Oracle" = "oracle",
    "Snowflake" = "snowflake"
    # add mappings from various connection classes to dbms here
  )

  if (is.null(result)) {
    rlang::abort(glue::glue("{class(con)} is not a supported connection type."))
  }
  return(result)
}

#' Collect a list of lazy queries and save the results as files
#'
#' @param cdm A cdm object
#' @param path A folder to save the cdm object to
#' @param format The file format to use: "parquet" (default), "csv", "feather" or "duckdb".
#'
#' @return Invisibly returns the cdm input
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' vocab <- cdm_from_con(con, "main") %>%
#'   cdm_select_tbl("concept", "concept_ancestor")
#' stow(vocab, here::here("vocab_tables"))
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
stow <- function(cdm, path, format = "parquet") {
  checkmate::assert_class(cdm, "cdm_reference")
  checkmate::assert_choice(format, c("parquet", "csv", "feather", "duckdb"))
  path <- path.expand(path)
  checkmate::assert_true(file.exists(path))

  switch(
    format,
    parquet = purrr::walk2(
      cdm,
      names(cdm),
      ~ arrow::write_parquet(dplyr::collect(.x), file.path(path, paste0(.y, ".parquet")))
    ),
    csv = purrr::walk2(
      cdm,
      names(cdm),
      ~ readr::write_csv(dplyr::collect(.x), file.path(path, paste0(.y, ".csv")))
    ),
    feather = purrr::walk2(
      cdm,
      names(cdm),
      ~ arrow::write_feather(dplyr::collect(.x), file.path(path, paste0(.y, ".feather")))
    ),
    duckdb = {
      rlang::check_installed("duckdb")
      con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file.path(path, "cdm.duckdb"))
      purrr::walk(names(cdm), ~DBI::dbWriteTable(con, name = ., value = dplyr::collect(cdm[[.]])))
      DBI::dbDisconnect(con, shutdown = TRUE)
    }
  )
  invisible(cdm)
}

#' Create a CDM reference from a folder containing parquet, csv, or feather
#' files
#'
#' @param path A folder where an OMOP CDM v5.4 instance is located.
#' @param format What is the file format to be read in? Must be "auto"
#'   (default), "parquet", "csv", "feather".
#' @param cdm_version,cdmVersion The version of the cdm (5.3 or 5.4)
#' @param cdm_name,cdmName A name to use for the cdm.
#' @param as_data_frame,asDataFrame TRUE (default) will read files into R as dataframes.
#'   FALSE will read files into R as Arrow Datasets.
#' @return A list of dplyr database table references pointing to CDM tables
#' @export
cdm_from_files <- function(path,
                           format = "auto",
                           cdm_version = "5.3",
                           cdm_name = NULL,
                           as_data_frame = TRUE) {
  checkmate::assert_choice(format, c("auto", "parquet", "csv", "feather"))
  checkmate::assert_logical(as_data_frame, len = 1, null.ok = FALSE)
  checkmate::assert_true(file.exists(path))

  checkmate::assert_choice(cdm_version, choices = c("5.3", "5.4"))
  checkmate::assert_character(cdm_name, null.ok = TRUE)

  path <- path.expand(path)

  files <- list.files(path, full.names = TRUE)

  if (format == "auto") {
    format <- unique(tools::file_ext(files))
    if (length(format) > 1) {
      rlang::abort(paste("Multiple file formats detected:", paste(format, collapse = ", ")))
    }
    checkmate::assert_choice(format, c("parquet", "csv", "feather"))
  }

  cdm_tables <- tools::file_path_sans_ext(basename(list.files(path)))
  cdm_table_files <- file.path(path, paste0(cdm_tables, ".", format))
  purrr::walk(cdm_table_files, ~checkmate::assert_file_exists(., "r"))

  cdm <- switch(
    format,
    parquet = purrr::map(cdm_table_files, function(.) {
      arrow::read_parquet(., as_data_frame = as_data_frame)
    }),
    csv = purrr::map(cdm_table_files, function(.) {
      arrow::read_csv_arrow(., as_data_frame = as_data_frame)
    }),
    feather = purrr::map(cdm_table_files, function(.) {
      arrow::read_feather(., as_data_frame = as_data_frame)
    })
  )

  # Try to get the cdm name if not supplied
  if (is.null(cdm_name) && ("cdm_source" %in% names(cdm))) {

    cdm_source <- cdm$cdm_source %>%
      head() %>%
      dplyr::collect() %>%
      dplyr::rename_all(tolower)

    cdm_name <- dplyr::coalesce(cdm_source$cdm_source_name[1],
                                cdm_source$cdm_source_abbreviation[1])
  }

  if (is.null(cdm_name)) {
    rlang::abort("cdm_name must be supplied!")
  }

  names(cdm) <- tolower(cdm_tables)

  # Try to get the cdm name if not supplied
  if (is.null(cdm_name) &&
      !is.null(names(cdm)) &&
      ("cdm_source" %in% names(cdm))) {

    cdm_source <- cdm[["cdm_source"]] %>%
      head() %>%
      dplyr::collect() %>%
      dplyr::rename_all(tolower)

    cdm_name <- dplyr::coalesce(cdm_source$cdm_source_name[1],
                                cdm_source$cdm_source_abbreviation[1])
  }

  if (is.null(cdm_name)) {
    rlang::abort("cdm_name must be supplied!")
  }


  class(cdm) <- "cdm_reference"

  attr(cdm, "cdm_schema") <- NULL
  attr(cdm, "write_schema") <- NULL
  attr(cdm, "dbcon") <- NULL
  attr(cdm, "cdm_version") <- cdm_version
  attr(cdm, "cdm_name") <- cdm_name
  return(cdm)
}


#' @rdname cdm_from_files
#' @export
cdmFromFiles <- function(path,
                         format = "auto",
                         cdmVersion = "5.3",
                         cdmName = NULL,
                         asDataFrame = TRUE) {
  cdm_from_files(path = path,
                 format = format,
                 cdm_version = cdmVersion,
                 cdm_name = cdmName,
                 as_data_frame = asDataFrame)
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
#' vocab <- cdm_from_con(con, "main") %>%
#'   cdm_select_tbl("concept", "concept_ancestor")
#'
#' local_vocab <- collect(vocab)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
collect.cdm_reference <- function(x, ...) {
  for (nm in names(x)) {
    x[[nm]] <- dplyr::collect(x[[nm]])
  }
  x
}

#' Extract CDM metadata
#'
#' Extract the name, version, and selected record counts from a cdm.
#'
#' @param cdm A cdm object
#'
#' @return A named list of attributes about the cdm including selected fields
#' from the cdm_source table and record counts from the person and
#' observation_period tables
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#' cdm <- cdm_from_con(con, "main")
#' snapshot(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
snapshot <- function(cdm) {
  assert_tables(cdm, tables = c("cdm_source", "vocabulary"), empty.ok = TRUE)
  assert_tables(cdm, tables = c("person", "observation_period"))

  person_count <- dplyr::tally(cdm$person, name = "n") %>% dplyr::pull(.data$n)

  observation_period_count <- dplyr::tally(cdm$observation_period, name = "n") %>%
    dplyr::pull(.data$n)

  observation_period_range <- cdm$observation_period %>%
    dplyr::summarise(
      max = max(.data$observation_period_end_date, na.rm = TRUE),
      min = min(.data$observation_period_start_date, na.rm = TRUE)
    ) %>%
    dplyr::collect()

  snapshot_date <- as.character(format(Sys.Date(), "%Y-%m-%d"))

  vocab_version <-
    cdm$vocabulary %>%
    dplyr::filter(.data$vocabulary_id == "None") %>%
    dplyr::pull(.data$vocabulary_version)

  if (length(vocab_version) == 0) {
    vocab_version <- NA_character_
  }

  cdm_source_name <- cdm$cdm_source %>% dplyr::pull(.data$cdm_source_name)

  cdm_source <- dplyr::collect(cdm$cdm_source)
  if (nrow(cdm_source) == 0) {
    cdm_source <- dplyr::tibble(
      vocabulary_version = vocab_version,
      cdm_source_name = "",
      cdm_holder = "",
      cdm_release_date = "",
      cdm_version = attr(cdm, "cdm_version"),
      source_description = "",
      source_documentation_reference = ""
    )
  }

  cdm_source %>%
    dplyr::mutate(
      cdm_name = dplyr::coalesce(attr(cdm, "cdm_name"), as.character(NA)),
      vocabulary_version = dplyr::coalesce(
        .env$vocab_version, .data$vocabulary_version
      ),
      person_count = .env$person_count,
      observation_period_count = .env$observation_period_count,
      earliest_observation_period_start_date =
        .env$observation_period_range$min,
      latest_observation_period_end_date = .env$observation_period_range$max,
      snapshot_date = .env$snapshot_date
    ) %>%
    dplyr::select(
      "cdm_name",
      "cdm_source_name",
      "cdm_description" = "source_description",
      "cdm_documentation_reference" = "source_documentation_reference",
      "cdm_version",
      "cdm_holder",
      "cdm_release_date",
      "vocabulary_version",
      "person_count",
      "observation_period_count",
      "earliest_observation_period_start_date",
      "latest_observation_period_end_date",
      "snapshot_date"
    ) %>%
    dplyr::mutate_all(as.character)
}

#' Disconnect the connection of the cdm object
#'
#' @param cdm cdm reference
#'
#' @export
cdmDisconnect <- function(cdm) {
  if (!("cdm_reference" %in% class(cdm))) {
    cli::cli_abort("cdm should be a cdm_reference")
  }
  DBI::dbDisconnect(attr(cdm, "dbcon"), shutdown = TRUE)
}

#' @rdname cdmDisconnect
#' @export
cdm_disconnect <- cdmDisconnect



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

#' Subset a cdm reference object
#'
#' @param x A cdm reference
#' @param name The name of the table to extract from the cdm object
#'
#' @return A single cdm table reference
#' @export
`$.cdm_reference` <- function(x, name) {
 x[[name]]
}

#' Subset a cdm reference object
#'
#' @param x A cdm reference
#' @param i The name or index of the table to extract from the cdm object
#'
#' @return A single cdm table reference
#' @export
`[.cdm_reference` <- function(x, i) {
  cdm_select_tbl(x, dplyr::all_of(i))
}

#' Subset a cdm reference object
#'
#' @param x A cdm reference
#' @param i The name or index of the table to extract from the cdm object
#'
#' @return A single cdm table reference
#' @export
`[[.cdm_reference` <- function(x, i) {
 x_raw <- unclass(x)
 tbl <- x_raw[[i]]

 if(is.null(tbl)) return(NULL)

 attr(tbl, "cdm_reference") <- x
 return(tbl)
}

#' @export
`[[<-.cdm_reference` <- function(obj, name, value) {
  x <- class(obj)
  attr(value, "cdm_reference") <- NULL
  obj <- unclass(obj)
  obj[[name]] <- value
  class(obj) <- x
  return(obj)
}

#' @export
`$<-.cdm_reference` <- function(obj, name, value) {
  x <- class(obj)
  attr(value, "cdm_reference") <- NULL
  obj <- unclass(obj)
  obj[[name]] <- value
  class(obj) <- x
  return(obj)
}


