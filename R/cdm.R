# Copyright 2024 DARWIN EU®
#
# This file is part of CDMConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Create a CDM reference object from a database connection
#'
#' @param con A DBI database connection to a database where an OMOP CDM v5.4 or
#'   v5.3 instance is located.
#' @param cdmSchema The schema where the OMOP CDM tables are located. Defaults
#'   to NULL.
#' @param writeSchema An optional schema in the CDM database that the user has
#'   write access to.
#' @param cohortTables A character vector listing the cohort table names to be
#'   included in the CDM object.
#' @param cdmVersion The version of the OMOP CDM: "5.3" (default), "5.4",
#'   "auto". "auto" attempts to automatically determine the cdm version using
#'   heuristics. Cohort tables must be in the write_schema.
#' @param cdmName The name of the CDM. If NULL (default) the cdm_source_name
#'.  field in the CDM_SOURCE table will be used.
#' @param achillesSchema An optional schema in the CDM database
#' that contains achilles tables.
#' @param .softValidation Normally the observation period table should not
#' have overlapping observation periods for a single person. If `.softValidation` is `TRUE` the
#' validation check that looks for overlapping observation periods will be skipped.
#' Other analytic packages may break or produce incorrect results if `softValidation` is `TRUE` and
#' the observation period table contains overlapping observation periods.
#'
#' @param writePrefix A prefix that will be added to all tables created in the write_schema. This
#' can be used to create namespace in your database write_schema for your tables.
#'
#' @return A list of dplyr database table references pointing to CDM tables
#'
#' @details
#' cdm_from_con / cdmFromCon creates a new cdm reference object from a DBI database connection.
#' In addition to the connection the user needs to pass in the schema in the database where the cdm data can
#' be found as well as another schema where the user has write access to create tables. Nearly all
#' downstream analytic packages need the ability to create temporary data in the database so the
#' write_schema is required.
#'
#' Some database systems have the idea of a catalog or a compound schema with two components.
#' See examples below for how to pass in catalogs and schemas.
#'
#' You can also specify a `writePrefix`. This is a short character string that will be added
#' to any tables created in the `writeSchema` effectively a namespace in the schema just for your
#' analysis. If the write_schema is a shared between multiple users setting a unique write_prefix
#' ensures you do not overwrite existing tables and allows you to easily clean up tables by
#' dropping all tables that start with the prefix.
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'
#' # minimal example
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = "main",
#'                   writeSchema = "scratch")
#'
#' # write prefix is optional but recommended if write_schema is shared
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = "main",
#'                   writeSchema = "scratch",
#'                   writePrefix = "tmp_")
#'
#' # Some database systems use catalogs or compound schemas.
#' # These can be specified as follows:
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = "catalog.main",
#'                   writeSchema = "catalog.scratch",
#'                   writePrefix = "tmp_")
#'
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = c("my_catalog", "main"),
#'                   writeSchema = c("my_catalog", "scratch"),
#'                   writePrefix = "tmp_")
#'
#' cdm <- cdmFromCon(con,
#'                   cdmSchema = c(catalog = "my_catalog", schema = "main"),
#'                   writeSchema = c(catalog = "my_catalog", schema = "scratch"),
#'                   writePrefix = "tmp_")
#'
#'  DBI::dbDisconnect(con)
#' }
#'
#'
#' @importFrom dplyr all_of matches starts_with ends_with contains
#' @importFrom stats rpois
#' @export
cdmFromCon <- function(con,
                       cdmSchema,
                       writeSchema,
                       cohortTables = NULL,
                       cdmVersion = "5.3",
                       cdmName = NULL,
                       achillesSchema = NULL,
                       .softValidation = FALSE,
                       writePrefix = NULL) {

  if (!DBI::dbIsValid(con)) {
    cli::cli_abort("The connection is not valid. Is the database connection open?")
  }

  if (dbms(con) == "sqlite") {
    cli::cli_abort("SQLite is not supported by CDMConnector. Please use duckdb instead.")
  }

  if (missing(writeSchema)) {
    cli::cli_abort("{.arg write_schema} is now required to create a cdm object with a database backend.
                   Please make sure you have a schema in your database where you can create new tables and provide it in the `write_schema` argument.
                   If your schema has multiple parts please provide a length 2 character vector: `write_schema = c('my_db', 'my_schema')`")
  }

  checkmate::assert_character(cdmName, any.missing = FALSE, len = 1, null.ok = TRUE)
  checkmate::assert_character(cdmSchema, min.len = 1, max.len = 3, any.missing = F)
  checkmate::assert_character(writeSchema, min.len = 1, max.len = 3, any.missing = F)
  checkmate::assert_character(cohortTables, null.ok = TRUE, min.len = 1)
  checkmate::assert_character(achillesSchema, min.len = 1, max.len = 3, any.missing = F, null.ok = TRUE)
  checkmate::assert_choice(cdmVersion, choices = c("5.3", "5.4", "auto"), null.ok = TRUE)
  checkmate::assert_character(writePrefix, min.chars = 1, any.missing = FALSE, len = 1, null.ok = TRUE)

  # users can give writeSchema = "catalog.schema"
  if (length(writeSchema) == 1 && stringr::str_detect(writeSchema, "\\.")) {
    if (stringr::str_count(writeSchema, "\\.") != 1) cli::cli_abort("`writeSchema` can only have one .")
    writeSchema <- stringr::str_split(writeSchema, "\\.")[[1]] %>% purrr::set_names(c("catalog", "schema"))
  }

  if (length(cdmSchema) == 1 && stringr::str_detect(cdmSchema, "\\.")) {
    if (stringr::str_count(cdmSchema, "\\.") != 1) cli::cli_abort("`cdmSchema` can only have one .")
    cdmSchema <- stringr::str_split(cdmSchema, "\\.")[[1]] %>% purrr::set_names(c("catalog", "schema"))
  }

  # make sure writeSchema is named
  if (!rlang::is_named(writeSchema)) {
    if (length(writeSchema) == 1) {
      writeSchema <- c("schema" = writeSchema)
    } else if (length(writeSchema) == 2) {
      writeSchema <- c("catalog" = writeSchema[1], "schema" = writeSchema[2])
    } else {
      rlang::abort("If `writeSchema` is unnamed then it should be length 1 `c(schema)` or 2 `c(catalog, schema)`")
    }
  } else {
    checkmate::assertTRUE(all(names(writeSchema) %in% c("catalog", "schema", "prefix")))
    if ("prefix" %in% names(writeSchema)) {
      rlang::inform("Support for 'prefix' in writeSchema is deprecated and will be removed in a future release. Please use the `writePrefix` argument in `cdmFromCon()` instead.",
                    .frequency = "once", .frequency_id = "write_prefix_deprecation")
    }
  }

  # if writePrefix argument is pass it will be override the prefix in writeSchema
  if (!is.null(writePrefix)) {
    checkmate::assert_character(writePrefix, min.chars = 1, len = 1, pattern = "^[a-z0-9_]+$")
    writeSchema["prefix"] <- writePrefix
  }


  # create source object and validate connection
  src <- dbSource(con = con, writeSchema = writeSchema)
  con <- attr(src, "dbcon")

  # read omop tables
  dbTables <- listTables(con, schema = cdmSchema)
  omop_tables <- omopgenerics::omopTables()
  omop_tables <- omop_tables[which(omop_tables %in% tolower(dbTables))]
  if (length(omop_tables) == 0) {
    rlang::abort("There were no cdm tables found in the cdm_schema!")
  }
  cdm_tables_in_db <- dbTables[which(tolower(dbTables) %in% omop_tables)]
  if (all(cdm_tables_in_db == toupper(cdm_tables_in_db))) {
    omop_tables <- toupper(omop_tables)
  } else if (!all(cdm_tables_in_db == tolower(cdm_tables_in_db))) {
    rlang::abort("CDM database tables should be either all upppercase or all lowercase!")
  }

  cdmTables <- purrr::map(
    omop_tables, ~ dplyr::tbl(src = src, schema = cdmSchema, name = .)
  ) %>%
    rlang::set_names(tolower(omop_tables))

  if (is.null(cdmName) && ("cdm_source" %in% names(cdmTables))) {
    cdm_name <- cdmTables$cdm_source %>%
    utils::head(1) %>%
    dplyr::pull("cdm_source_name")
  }

  if (is.null(cdmName) || length(cdmName) != 1 || is.na(cdmName)) {
    cli::cli_alert_warning("cdm name not specified and could not be inferred from the cdm source table")
    cdmName <- "An OMOP CDM database"
  }

  if (!is.null(achillesSchema)) {
    achillesReqTables <- omopgenerics::achillesTables()
    acTables <- listTables(con, schema = achillesSchema)
    achilles_tables <- acTables[which(tolower(acTables) %in% achillesReqTables)]

    if (length(achilles_tables) != 3) {
      cli::cli_abort("Achilles tables not found in {achilles_schema}!")
    }

    achillesTables <- purrr::map(achilles_tables, ~dplyr::tbl(src = src, schema = achillesSchema, .)) %>%
      rlang::set_names(tolower(achilles_tables))

  } else {
    achillesTables <- list()
  }

  cdm <- omopgenerics::newCdmReference(
    tables = c(cdmTables, achillesTables),
    cdmName = cdmName,
    cdmVersion = cdmVersion,
    .softValidation = .softValidation
  )

  # on spark we use permanent tables prefixed with this whenever the user asks for temp tables
  attr(cdm, "temp_emulation_prefix") <- paste0(
    "temp", Sys.getpid() + stats::rpois(1, as.integer(Sys.time())) %% 1e6, "_")

  write_schema_tables <- listTables(con, schema = writeSchema)

  for (cohort_table in cohortTables) {
    nms <- paste0(cohort_table, c("", "_set", "_attrition", "_codelist"))
    x <- purrr::map(nms, function(nm) {
      if (nm %in% write_schema_tables) {
        dplyr::tbl(src = src, schema = writeSchema, name = nm)
      } else if (nm %in% toupper(write_schema_tables)) {
        dplyr::tbl(src = src, schema = writeSchema, name = toupper(nm))
      } else {
        NULL
      }
    })
    cdm[[cohort_table]] <- x[[1]]
    if(is.null(cdm[[cohort_table]])) {
      rlang::abort(glue::glue("cohort table `{cohort_table}` not found!"))
    }

    cdm[[cohort_table]] <- cdm[[cohort_table]] |>
      omopgenerics::newCohortTable(
        cohortSetRef = x[[2]],
        cohortAttritionRef = x[[3]],
        cohortCodelistRef = x[[4]],
        .softValidation = .softValidation
      )

  }

  if (dbms(con) == "snowflake") {

    s <- writeSchema %||% cdmSchema

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

  # TO BE REMOVED WHEN CIRCER WORKS WITH CDM OBJECT
  attr(cdm, "cdm_schema") <- cdmSchema
  # TO BE REMOVED WHEN DOWNSTREAM PACKAGES NO LONGER USE THESE ATTRIBUTES
  attr(cdm, "write_schema") <- writeSchema
  attr(cdm, "dbcon") <- attr(attr(cdm, "cdm_source"), "dbcon")

  return(cdm)
}

#' @export
#' @importFrom dplyr tbl
tbl.db_cdm <- function(src, schema, name, ...) {
  con <- attr(src, "dbcon")
  fullName <- .inSchema(schema = schema, table = name, dbms = dbms(con))
  x <- dplyr::tbl(src = con, fullName) |>
    dplyr::rename_all(tolower) |>
    omopgenerics::newCdmTable(src = src, name = tolower(name))
  return(x)
}


#' @rdname cdmFromCon
#' @export
cdm_from_con <- function(con,
                         cdm_schema,
                         write_schema,
                         cohort_tables = NULL,
                         cdm_version = "5.3",
                         cdm_name = NULL,
                         achilles_schema = NULL,
                         .soft_validation = FALSE,
                         write_prefix = NULL) {
  lifecycle::deprecate_soft("1.7.0", "cdm_from_con()", "cdmFromCon()")

  cdmFromCon(
    con = con,
    cdmSchema = cdm_schema,
    writeSchema = write_schema,
    cohortTables = cohort_tables,
    cdmVersion = cdm_version,
    cdmName = cdm_name,
    achillesSchema = achilles_schema,
    .softValidation = .soft_validation,
    writePrefix = write_prefix
  )
}

detect_cdm_version <- function(con, cdm_schema = NULL) {
  lifecycle::deprecate_soft("1.7.0", "detect_cdm_version()", "detectCdmVersion()")
  cdm_tables <- c("visit_occurrence", "cdm_source", "procedure_occurrence")

  if (!all(cdm_tables %in% listTables(con, schema = cdm_schema))) {
    rlang::abort(paste0(
      "The ",
      paste(cdm_tables, collapse = ", "),
      " tables are required for auto-detection of cdm version."
    ))
  }

  cdm <- purrr::map(
    cdm_tables, ~dplyr::tbl(con, .inSchema(cdm_schema, ., dbms(con))) %>%
                      dplyr::rename_all(tolower)) %>%
    rlang::set_names(tolower(cdm_tables))

  # Try a few different things to figure out what the cdm version is
  visit_occurrence_names <- cdm$visit_occurrence %>%
    head() %>%
    dplyr::collect() %>%
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
    dplyr::collect() %>%
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
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
#' version(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
version <- function(cdm) {
  lifecycle::deprecate_warn("1.3.2", "version()",
                            with = "cdmVersion()")
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
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
#' cdmName(cdm)
#' #> [1] "eunomia"
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
cdmName <- function(cdm) {
  omopgenerics::cdmName(cdm)
}

#' `r lifecycle::badge("deprecated")`
#' @rdname cdmName
#' @export
cdm_name <- function(cdm) {
  lifecycle::deprecate_soft("1.7.0", "cdm_name()", "cdmName()")
  omopgenerics::cdmName(cdm)
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
                      name = .inSchema(schema = write_schema, table = tablename, dbms = dbms(con)),
                      value = df1,
                      overwrite = TRUE)
  )

  withr::with_options(list(databaseConnectorIntegerAsNumeric = FALSE), {
    df2 <- dplyr::tbl(con, .inSchema(write_schema, tablename, dbms = dbms(con))) %>%
      dplyr::collect() %>%
      as.data.frame() %>%
      dplyr::rename_all(tolower) %>% # dbWriteTable can create uppercase column names on snowflake
      dplyr::select("chr_col", "numeric_col") # bigquery can reorder columns
  })

  DBI::dbRemoveTable(con, .inSchema(write_schema, tablename, dbms = dbms(con)))

  if (tablename %in% listTables(con, write_schema)) {
    cli::cli_inform("Write access verified but temp table `{name}` was not properly dropped!")
  }

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
#' The OMOP CDM tables are grouped together and the `tblGroup` function allows
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
#' cdm <- cdmFromCon(con, cdmName = "test", cdmSchema = "public") %>%
#'   cdmSelectTbl(tblGroup("vocab"))
#' }
tblGroup <- function(group) {
  # groups are defined in the internal package dataframe called spec_cdm_table
  # created by a script in the extras folder
  checkmate::assert_subset(group, c("vocab", "clinical", "all", "default", "derived"))
  # use v5.3 here. The set of table groups between 5.3 and 5.4 are the same.
  spec <- spec_cdm_table[["5.3"]]
  purrr::map(group, ~ spec[spec[[paste0("group_", .)]], ]$cdmTableName) %>%
    unlist() %>%
    unique()
}

#' `r lifecycle::badge("deprecated")`
#' @export
#' @rdname tblGroup
tbl_group <- function(group) {
  lifecycle::deprecate_soft("1.7.0", "tbl_group()", "tblGroup()")
  tblGroup(group)
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
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#' cdm <- cdmFromCon(con)
#' dbms(cdm)
#' dbms(con)
#' }
dbms <- function(con) {

  if (methods::is(con, "cdm_reference")) {
    con <- attr(con, "dbcon")
  } else if (methods::is(con, "Pool")) {
    if (!rlang::is_installed("pool")) {
      rlang::abort("Please install the pool package.")
    }
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
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#' vocab <- cdmFromCon(con, "main") %>%
#'   cdmSelectTbl("concept", "concept_ancestor")
#' stow(vocab, here::here("vocab_tables"))
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
stow <- function(cdm, path, format = "parquet") {
  ensureInstalled("readr")
  ensureInstalled("arrow")
  checkmate::assert_class(cdm, "cdm_reference")
  checkmate::assert_choice(format, c("parquet", "csv", "feather", "duckdb"))
  path <- path.expand(path)
  checkmate::assert_true(file.exists(path))

  if (format %in% c("parquet", "feather")) {
    rlang::check_installed("arrow")
  }

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
      con <- DBI::dbConnect(duckdb::duckdb(file.path(path, "cdm.duckdb")))
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
cdmFromFiles <- function(path,
                         format = "auto",
                         cdmVersion = "5.3",
                         cdmName = NULL,
                         asDataFrame = TRUE) {
  checkmate::assert_choice(format, c("auto", "parquet", "csv", "feather"))
  checkmate::assert_logical(asDataFrame, len = 1, null.ok = FALSE)
  checkmate::assert_true(file.exists(path))

  checkmate::assert_choice(cdmVersion, choices = c("5.3", "5.4"))
  checkmate::assert_character(cdmName, null.ok = TRUE)
  rlang::check_installed("arrow")

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
      arrow::read_parquet(., as_data_frame = asDataFrame)
    }),
    csv = purrr::map(cdm_table_files, function(.) {
      arrow::read_csv_arrow(., as_data_frame = asDataFrame)
    }),
    feather = purrr::map(cdm_table_files, function(.) {
      arrow::read_feather(., as_data_frame = asDataFrame)
    })
  )

  # Try to get the cdm name if not supplied
  if (is.null(cdmName) && ("cdm_source" %in% names(cdm))) {

    cdm_source <- cdm$cdm_source %>%
      head() %>%
      dplyr::collect() %>%
      dplyr::rename_all(tolower)

    cdmName <- dplyr::coalesce(cdm_source$cdm_source_name[1],
                                cdm_source$cdm_source_abbreviation[1])
  }

  if (is.null(cdmName)) {
    rlang::abort("cdmName must be supplied!")
  }

  names(cdm) <- tolower(cdm_tables)

  # Try to get the cdm name if not supplied
  if (is.null(cdmName) &&
      !is.null(names(cdm)) &&
      ("cdm_source" %in% names(cdm))) {

    cdm_source <- cdm[["cdm_source"]] %>%
      head() %>%
      dplyr::collect() %>%
      dplyr::rename_all(tolower)

    cdmName <- dplyr::coalesce(cdm_source$cdm_source_name[1],
                                cdm_source$cdm_source_abbreviation[1])
  }

  if (is.null(cdmName)) {
    rlang::abort("cdmName must be supplied!")
  }


  class(cdm) <- "cdm_reference"

  attr(cdm, "cdm_schema") <- NULL
  attr(cdm, "write_schema") <- NULL
  attr(cdm, "dbcon") <- NULL
  attr(cdm, "cdm_version") <- cdmVersion
  attr(cdm, "cdm_name") <- cdmName
  return(cdm)
}

#' `r lifecycle::badge("deprecated")`
#' @rdname cdmFromFiles
#' @export
cdm_from_files <- function(path,
                         format = "auto",
                         cdm_version = "5.3",
                         cdm_name = NULL,
                         as_data_frame = TRUE) {

  lifecycle::deprecate_soft("1.7.0", "cdm_from_files()", "cdmFromFiles()")
  cdmFromFiles(path = path,
               format = format,
               cdmVersion = cdm_version,
               cdmName = cdm_name,
               asDataFrame = as_data_frame)
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
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con, "main")
#' snapshot(cdm)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
snapshot <- function(cdm) {
  .assertTables(cdm, tables = c("cdm_source", "vocabulary"), empty.ok = TRUE)
  .assertTables(cdm, tables = c("person", "observation_period"))

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
#' This function will disconnect from the database as well as drop
#' "temporary" tables that were created on database systems that do not support
#' actual temporary tables. Currently temp tables are emulated on
#' Spark/Databricks systems.
#'
#' @param cdm cdm reference
#'
#' @export
cdmDisconnect <- function(cdm) {
  if (!("cdm_reference" %in% class(cdm))) {
    cli::cli_abort("cdm should be a cdm_reference")
  }

  con <- cdmCon(cdm)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  if (dbms(con) == "spark") {
    schema <- attr(cdm, "write_schema")
    tbls <- listTables(con, schema = schema)
    tempEmulationTablesToDrop <- stringr::str_subset(tbls, attr(cdm, "temp_emulation_prefix"))
    # try to drop the temp emulation tables
    purrr::walk(tempEmulationTablesToDrop,
                ~tryCatch(DBI::dbRemoveTable(con, .inSchema(schema, ., dbms = dbms(con))),
                          error = function(e) invisible(NULL)))
  }
}

#' `r lifecycle::badge("deprecated")`
#' @rdname cdmDisconnect
#' @export
cdm_disconnect <- function(cdm) {
  lifecycle::deprecate_soft("1.7.0", "cdm_disconnect()", "cdmDisconnect()")
  cdmDisconnect(cdm)
}



#' Select a subset of tables in a cdm reference object
#'
#' This function uses syntax similar to `dplyr::select` and can be used to
#' subset a cdm reference object to a specific tables
#'
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
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#'
#' cdm <- cdmFromCon(con, "main")
#'
#' cdmSelectTbl(cdm, person)
#' cdmSelectTbl(cdm, person, observation_period)
#' cdmSelectTbl(cdm, tblGroup("vocab"))
#' cdmSelectTbl(cdm, "person")
#'
#' DBI::dbDisconnect(con)
#' }
cdmSelectTbl <- function(cdm, ...) {
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

#' `r lifecycle::badge("deprecated")
#' @rdname cdmSelectTbl
#' @export
cdm_select_tbl <- function(cdm, ...){
  lifecycle::deprecate_soft("1.7.0", "cdm_select_tbl()", "cdmSelectTbl()")
  cdmSelectTbl(cdm, ...)
}

#' Get cdm write schema
#'
#' @param cdm A cdm reference object created by `cdmFromCon`
#'
#' @return The database write schema
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#'
#' cdm <- cdmFromCon(con = con, cdmName = "Eunomia",
#'                   cdmSchema =  "main", writeSchema = "main")
#'
#' cdmWriteSchema(cdm)
#'
#' DBI::dbDisconnect(con)
#' }
cdmWriteSchema <- function(cdm) {
  attr(attr(cdm, "cdm_source"), "write_schema")
}

#' Get underlying database connection
#'
#' @param cdm A cdm reference object created by `cdmFromCon`
#'
#' @return A reference to the database containing tables in the cdm reference
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#'
#' cdm <- cdmFromCon(con = con, cdmName = "Eunomia",
#'                   cdmSchema =  "main", writeSchema = "main")
#'
#' cdmCon(cdm)
#'
#' DBI::dbDisconnect(con)
#' }
cdmCon <- function(cdm) {
  attr(attr(cdm, "cdm_source"), "dbcon")
}

