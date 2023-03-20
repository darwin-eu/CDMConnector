#' Create a CDM reference object from a database connection
#'
#' @param con A DBI database connection to a database where an OMOP CDM v5.4 or
#'   v5.3 instance is located.
#' @param cdm_schema The schema where the OMOP CDM tables are located. Defaults
#'   to NULL.
#' @param write_schema An optional schema in the CDM database that the user has
#'   write access to.
#' @param cdm_tables Which tables should be included? Supports a character
#'   vector, tidyselect selection helpers, or table groups.
#' \itemize{
#'   \item{tbl_group("all")}{all CDM tables}
#'   \item{tbl_group("vocab")}{the CDM vocabulary tables}
#'   \item{tbl_group("clinical")}{the clinical CDM tables}
#' }
#' @param cohort_tables A character vector listing the cohort table names to be
#'   included in the CDM object.
#' @param cdm_version The version of the OMOP CDM: "5.3" (default), "5.4",
#'   "auto". "auto" attempts to automatically determine the cdm version using
#'   heuristics. Cohort tables must be in the write_schema.
#' @param cdm_name The name of the CDM. If NULL (default) the cdm_source_name
#'.  field in the CDM_SOURCE table will be used.
#' @return A list of dplyr database table references pointing to CDM tables
#' @importFrom dplyr all_of matches starts_with ends_with contains
#' @export
cdm_from_con <- function(con,
                         cdm_schema = NULL,
                         cdm_tables = tbl_group("default"),
                         write_schema = NULL,
                         cohort_tables = NULL,
                         cdm_version = "5.3",
                         cdm_name = NULL) {

  checkmate::assert_class(con, "DBIConnection")
  checkmate::assert_true(.dbIsValid(con))

  if (dbms(con) %in% c("duckdb", "sqlite") && is.null(cdm_schema)) {
    cdm_schema = "main"
  }

  checkmate::assert_character(
    cdm_schema,
    null.ok = FALSE,
    min.len = 1,
    max.len = 2
  )
  checkmate::assert_character(
    write_schema,
    null.ok = TRUE,
    min.len = 1,
    max.len = 2
  )
  checkmate::assert_character(cohort_tables, null.ok = TRUE, min.len = 1)
  checkmate::assert_choice(cdm_version, choices = c("5.3", "5.4", "auto"))
  checkmate::assert_character(cdm_name, null.ok = TRUE)

  # handle schema names like 'schema.dbo'
  if (!is.null(cdm_schema) && length(cdm_schema) == 1) {
    cdm_schema <- strsplit(cdm_schema, "\\.")[[1]]
      checkmate::assert_character(
        cdm_schema,
        null.ok = TRUE,
        min.len = 1,
        max.len = 2
      )
    }

  if (!is.null(write_schema) && length(write_schema) == 1) {
    write_schema <- strsplit(write_schema, "\\.")[[1]]
    checkmate::assert_character(
      write_schema,
      null.ok = TRUE,
      min.len = 1,
      max.len = 2
    )
  }

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

    # tidyselect: https://tidyselect.r-lib.org/articles/tidyselect.html
    all_cdm_tables <-
      rlang::set_names(spec_cdm_table[[cdm_version]]$cdmTableName,
                       spec_cdm_table[[cdm_version]]$cdmTableName)

    cdm_tables <- names(tidyselect::eval_select(rlang::enquo(cdm_tables),
                                                data = all_cdm_tables))

    # Handle uppercase table names in the database
    if (all(dbTables == toupper(dbTables))) {
      cdm_tables <- toupper(cdm_tables)
    }

    cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, inSchema(cdm_schema, ., dbms(con))) %>%
                      dplyr::rename_all(tolower)) %>%
      rlang::set_names(tolower(cdm_tables))

    if (!is.null(write_schema)) {
      verify_write_access(con, write_schema = write_schema)
    }

    dbTablesWrite <- listTables(con, schema = write_schema)
    # Add existing GeneratedCohortSet objects to cdm object
    if (!is.null(cohort_tables)) {
      if (is.null(write_schema)) {
        rlang::abort("write_schema is required when using cohort_tables")
      }

      for (i in seq_along(cohort_tables)) {

        cohort_ref <- dplyr::tbl(con, inSchema(write_schema, cohort_tables[i], dbms(con))) %>%
          dplyr::rename_all(tolower)

        # Optional attribute tables {cohort}_set, {chohort}_attrition, {cohort}_count
        nm <- paste0(cohort_tables[i], "_set")
        if (nm %in% dbTablesWrite) {
          cohort_set_ref <- dplyr::tbl(con, inSchema(write_schema, nm, dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else if (nm %in% toupper(dbTablesWrite)) {
          cohort_set_ref <- dplyr::tbl(con, inSchema(write_schema, toupper(nm), dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else {
          cohort_set_ref <- NULL
        }

        nm <- paste0(cohort_tables[i], "_attrition")
        if (nm %in% dbTablesWrite) {
          cohort_attrition_ref <- dplyr::tbl(con, inSchema(write_schema, nm, dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else if (nm %in% toupper(dbTablesWrite)) {
          cohort_attrition_ref <- dplyr::tbl(con, inSchema(write_schema, toupper(nm), dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else {
          cohort_attrition_ref <- NULL
        }

        nm <- paste0(cohort_tables[i], "_count")
        if (nm %in% dbTablesWrite) {
          cohort_count_ref <- dplyr::tbl(con, inSchema(write_schema, nm, dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else if (nm %in% toupper(dbTablesWrite)) {
          cohort_count_ref <- dplyr::tbl(con, inSchema(write_schema, toupper(nm), dbms(con))) %>%
            dplyr::rename_all(tolower)
        } else {
          cohort_count_ref <- NULL
        }


        cdm[[cohort_tables[i]]] <- newGeneratedCohortSet(cohort_ref = cohort_ref,
                                                         cohort_attrition_ref = cohort_attrition_ref,
                                                         cohort_set_ref = cohort_set_ref,
                                                         cohort_count_ref = cohort_count_ref)
      }
    }



    # TODO cdm_reference constructor
    class(cdm) <- "cdm_reference"
    attr(cdm, "cdm_schema") <- cdm_schema
    attr(cdm, "write_schema") <- write_schema
    attr(cdm, "dbcon") <- con
    attr(cdm, "cdm_version") <- cdm_version
    attr(cdm, "cdm_name") <- cdm_name
    return(cdm)
  }

detect_cdm_version <- function(con, cdm_schema = NULL) {
  cdm_tables <- c("visit_occurrence", "cdm_source", "procedure_occurrence")

  if (!all(cdm_tables %in% listTables(con, schema = cdm_schema))) {
    rlang::abort(paste0(
      "The ",
      paste(cdm_tables, collapse = ", "),
      " tables are require for auto-detection of cdm version."
    ))
  }

  cdm <- purrr::map(cdm_tables, ~dplyr::tbl(con, inSchema(cdm_schema, ., dbms(con))) %>%
                    dplyr::rename_all(tolower)) %>%
    rlang::set_names(tolower(cdm_tables))

  # Try a few different things to figure out what the cdm version is
  visit_occurrence_names <- cdm$visit_occurrence %>%
    head() %>%
    collect() %>%
    names() %>%
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
    names() %>%
    tolower()

  if ("procedure_end_date" %in% procedure_occurrence_names) {
    return("5.4")
  }

  cdm_version <- cdm$cdm_source %>% dplyr::pull(.data$cdm_version)
  if (isTRUE(grepl("5\\.4", cdm_version)))
    return("5.4")
  if (isTRUE(grepl("5\\.3", cdm_version)))
    return("5.3")

  if ("episode" %in% listTables(con, schema = cdm_schema))
    return("5.4")
  else
    return("5.3")
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
#' cdm <- cdm_from_con(con, cdm_tables = c(tbl_group("default"), "cdm_source"))
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

#' Create a CDM reference object from a database connection
#'
#' @param con A DBI database connection to a database where an OMOP CDM v5.4
#'   instance is located.
#' @param cdmSchema The schema where the OMOP CDM tables are located. Defaults
#'   to NULL.
#' @param writeSchema An optional schema in the CDM database that the user has
#'   write access to.
#' @param cdmTables Which tables should be included? Supports a character
#'   vector, tidyselect selection helpers, or table groups.
#' \itemize{
#'   \item{tbl_group("all")}{all CDM tables}
#'   \item{tbl_group("vocab")}{the CDM vocabulary tables}
#'   \item{tbl_group("clinical")}{the clinical CDM tables}
#' }
#' @param cohortTables A character vector listing the cohort table names to be
#'   included in the CDM object. Cohort tables must be in the write_schema.
#' @param cdmName The name of the CDM. If NULL (default) the cdm_source_name
#'.  field in the CDM_SOURCE table will be used.
#' @return A list of dplyr database table references pointing to CDM tables
#' @importFrom dplyr all_of matches starts_with ends_with contains
#' @rdname cdm_from_con
#' @export
cdmFromCon <- function(con,
                       cdmSchema = NULL,
                       cdmTables = tbl_group("default"),
                       writeSchema = NULL,
                       cohortTables = NULL,
                       cdmName = NULL) {
  cdm_from_con(
    con = con,
    cdm_schema = cdmSchema,
    cdm_tables = {{cdmTables}},
    write_schema = writeSchema,
    cohort_tables = cohortTables,
    cdm_name = cdmName
  )
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

# con = database connection
# write_schema = schema with write access
# add = checkmate collection
verify_write_access <- function(con, write_schema, add = NULL) {
  checkmate::assert_character(
    write_schema,
    min.len = 1,
    max.len = 2,
    min.chars = 1
  )
  checkmate::assert_class(add, "AssertCollection", null.ok = TRUE)
  checkmate::assert_true(.dbIsValid(con))

  # TODO quote SQL names
  write_schema <- paste(write_schema, collapse = ".")
  tablename <-
    paste(c(sample(letters, 12, replace = TRUE), "_test_table"), collapse = "")
  tablename <- paste(write_schema, tablename, sep = ".")

  df1 <- data.frame(chr_col = "a", numeric_col = 1)
  # ROracle does not support integer round trip
  DBI::dbWriteTable(con, DBI::SQL(tablename), df1)

  withr::with_options(list(databaseConnectorIntegerAsNumeric = FALSE), {
    df2 <- DBI::dbReadTable(con, DBI::SQL(tablename))
    names(df2) <- tolower(names(df2))
  })

  DBI::dbRemoveTable(con, DBI::SQL(tablename))

  if (!isTRUE(all.equal(df1, df2))) {
    msg <- paste("Write access to schema",
                 write_schema,
                 "could not be verified.")

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
#' \figure{cdm54.png}{CDM 5.4}
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
#' cdm <- cdm_from_con(con, cdm_tables = tbl_group("vocab"))
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
  UseMethod("dbms")
}

#' @export
dbms.cdm_reference <- function(con) {
  dbms(attr(con, "dbcon"))
}

#' @export
dbms.DBIConnection <- function(con) {
  if (!is.null(attr(con, "dbms")))
    return(attr(con, "dbms"))

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
    "Oracle" = "oracle"
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
  )
  invisible(cdm)
}

#' Create a CDM reference from a folder containing parquet, csv, or feather
#' files
#'
#' @param path A folder where an OMOP CDM v5.4 instance is located.
#' @param format What is the file format to be read in? Must be "auto"
#'   (default), "parquet", "csv", "feather".
#' @param as_data_frame TRUE (default) will read files into R as dataframes.
#'   FALSE will read files into R as Arrow Datasets.
#' @return A list of dplyr database table references pointing to CDM tables
#' @export
cdm_from_files <-
  function(path,
           format = "auto",
           as_data_frame = TRUE) {
    checkmate::assert_choice(format, c("auto", "parquet", "csv", "feather"))
    checkmate::assert_logical(as_data_frame, len = 1, null.ok = FALSE)
    checkmate::assert_true(file.exists(path))

    path <- path.expand(path)

    files <- list.files(path, full.names = TRUE)

    if (format == "auto") {
      format <- unique(tools::file_ext(files))
      if (length(format) > 1)
        rlang::abort(paste(
          "Multiple file formats detected:",
          paste(format, collapse = ", ")
        ))
      checkmate::assert_choice(format, c("parquet", "csv", "feather"))
    }

    cdm_tables <-
      tools::file_path_sans_ext(basename(list.files(path)))

    cdm_table_files <-
      file.path(path, paste0(cdm_tables, ".", format))

    purrr::walk(cdm_table_files, function(.) {
      checkmate::assert_file_exists(., "r")
    })

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
    ) %>%
      magrittr::set_names(cdm_tables) %>%
      magrittr::set_class("cdm_reference")

    attr(cdm, "cdm_schema") <- NULL
    attr(cdm, "write_schema") <- NULL
    attr(cdm, "dbcon") <- NULL
    attr(cdm, "cdm_version") <- NULL
    cdm
  }

#' Create a CDM reference from a folder containing parquet, csv, or feather
#' files
#'
#' @param path A folder where an OMOP CDM v5.4 instance is located.
#' @param format What is the file format to be read in? Must be "auto"
#'   (default), "parquet", "csv", "feather".
#' @param asDataFrame TRUE (default) will read files into R as dataframes.
#'   FALSE will read files into R as Arrow Datasets.
#' @return A list of dplyr database table references pointing to CDM tables
#' @rdname cdm_from_files
#' @export
cdmFromFiles <-
  function(path,
           format = "auto",
           asDataFrame = TRUE) {
    cdm_from_files(
      path = path,
      format = format,
      as_data_frame = asDataFrame
    )
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

##' @importFrom dplyr collect
##' @export
NULL

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
#' cdm <- cdm_from_con(con, cdm_tables = c(tbl_group("default"), "cdm_source"))
#' snapshot(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
snapshot <- function(cdm) {
  assert_tables(cdm,
                tables = c("cdm_source",
                           "person",
                           "observation_period",
                           "vocabulary"))

  person_cnt <- dplyr::tally(cdm$person, name = "n") %>% dplyr::pull(.data$n)

  observation_period_cnt <- dplyr::tally(cdm$observation_period, name = "n") %>%
    dplyr::pull(.data$n)

  vocab_version <-
    cdm$vocabulary %>%
    dplyr::filter(.data$vocabulary_id == "None") %>%
    dplyr::pull(.data$vocabulary_version)

  cdm_source_name <- cdm$cdm_source %>% dplyr::pull(.data$cdm_source_name)

  dplyr::collect(cdm$cdm_source) %>%
    dplyr::mutate(vocabulary_version = dplyr::coalesce(.env$vocab_version,
                                                       .data$vocabulary_version)) %>%
    dplyr::mutate(
      person_cnt = .env$person_cnt,
      observation_period_cnt = .env$observation_period_cnt
    ) %>%
    dplyr::select(
      "cdm_source_name",
      "cdm_version",
      "cdm_holder",
      "cdm_release_date",
      "vocabulary_version",
      "person_cnt",
      "observation_period_cnt"
    ) %>%
    as.list() %>%
    c(list(cdm_schema = attr(cdm, "cdm_schema"),
           write_schema = attr(cdm, "write_schema"),
           cdm_name = attr(cdm, "cdm_name"))) %>%
    magrittr::set_class("cdm_snapshot")

}

#' @export
print.cdm_snapshot <- function(x, ...) {
  cli::cat_rule(x$cdm_source_name)
  purrr::walk2(names(x[-1]), x[-1], ~ cli::cat_bullet(.x, ": ", .y))
}
