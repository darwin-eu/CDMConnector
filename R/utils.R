# Copyright 2025 DARWIN EU®
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

#' Pipe operator
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom dplyr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL

# Workaround for Oracle since ROracle does not define dbIsValid
.dbIsValid <- function(dbObj, ...) {
  if (methods::is(dbObj, "OraConnection")) {
    is.character(DBI::dbListTables(dbObj))
  } else {
    DBI::dbIsValid(dbObj, ...)
  }
}

#' Helper for working with compound schemas
#'
#' This is similar to dbplyr::in_schema but has been tested across multiple
#' database platforms. It only exists to work around some of the limitations
#' of dbplyr::in_schema.
#'
#' @param schema A schema name as a character string
#' @param table A table name as character string
#' @param dbms The name of the database management system as returned
#' by `dbms(connection)`
#'
#' @return A DBI::Id that represents a qualified table and schema
#' @export
inSchema <- function(schema, table, dbms = NULL) {
  # TODO deprecate this function after removing it from all tests
  # lifecycle::deprecate_soft("1.4.1", "CDMConnector::inSchema()", "dbplyr::in_schema()")
  .inSchema(schema, table, dbms)
}

# internal function
.inSchema <- function(schema, table, dbms = NULL) {
  # lifecycle::deprecate_soft("1.4.1", "CDMConnector::inSchema()", "dbplyr::in_schema()")
  checkmate::assertCharacter(schema, min.len = 1, max.len = 3, null.ok = TRUE)
  checkmate::assertCharacter(table, len = 1, min.chars = 1)
  checkmate::assertCharacter(dbms, len = 1, null.ok = TRUE)

  if (is.null(schema)) {
    # return temp table name
    if (dbms == "sql server") {
      return(DBI::Id(table = paste0("#", table)))
    }
    return(DBI::Id(table = table))
  }

  if ("prefix" %in% names(schema)) {
    checkmate::assertCharacter(schema['prefix'], len = 1, min.chars = 1, pattern = "[a-zA-Z1-9_]+")

    # match the case of table name
    if (toupper(table) == table) {
      table <- paste0(toupper(schema['prefix']), table)
    } else {
      table <- paste0(schema['prefix'], table)
    }

    schema <- schema[!names(schema) %in% "prefix"]
    checkmate::assertCharacter(schema, min.len = 1, max.len = 2)
  }

  if (isFALSE(dbms %in% c("snowflake", "sql server", "spark", "bigquery", "duckdb"))) {
    # only a few dbms support three part names
    checkmate::assertCharacter(schema, len = 1)
  }

  schema <- unname(schema)

  if (!is.null(dbms) && dbms == "duckdb" && identical(schema, "main")) {
    out <- table
  } else if (!is.null(dbms) && dbms == "bigquery" && length(schema) == 2) {
    # https://github.com/darwin-eu/CDMConnector/issues/37
    out <- paste(c(schema, table), collapse = ".")
  } else {
    out <- switch(length(schema),
      DBI::Id(schema = schema, table = table),
      DBI::Id(catalog = schema[1], schema = schema[2], table = table))
  }
  return(out)
}

# Build a quoted qualified table name for use in raw SQL (e.g. INSERT INTO ...)
# fullName: character table name or DBI::Id(schema=, table=) or DBI::Id(catalog=, schema=, table=)
.qualifiedNameForSql <- function(con, fullName) {
  if (is.character(fullName) && length(fullName) == 1) {
    return(DBI::dbQuoteIdentifier(con, fullName))
  }
  if (inherits(fullName, "Id")) {
    parts <- unlist(fullName@name)
    return(paste(vapply(parts, function(p) DBI::dbQuoteIdentifier(con, p), character(1)), collapse = "."))
  }
  fullName
}

#' List tables in a schema
#'
#' DBI::dbListTables can be used to get all tables in a database but not always in a
#' specific schema. `listTables` will list tables in a schema.
#'
#'
#' @param con A DBI connection to a database
#' @param schema The name of a schema in a database. If NULL, returns DBI::dbListTables(con).
#'
#' @return A character vector of table names
#' @export
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
#' listTables(con, schema = "main")
#' }
listTables <- function(con, schema = NULL) {

  if (methods::is(con, "Pool")) {
    if (!rlang::is_installed("pool")) {
      rlang::abort("Please install the pool package.")
    }
    con <- pool::localCheckout(con)
  }

  checkmate::assertTRUE(DBI::dbIsValid(con))

  if (methods::is(schema, "Id")) {
    schema <- schema@name
  }

  # Allow "catalog.schema" form (e.g. "cdm.main") for systems with multiple catalogs
  if (length(schema) == 1 && is.character(schema) && stringr::str_detect(schema, "\\.")) {
    if (stringr::str_count(schema, "\\.") != 1) {
      rlang::abort("`schema` can only have one dot when using catalog.schema form.")
    }
    schema <- stringr::str_split(schema, "\\.")[[1]] %>%
      purrr::set_names(c("catalog", "schema"))
  }

  if ("prefix" %in% names(schema)) {
    prefix <- schema["prefix"]
    checkmate::assert_character(prefix, min.chars = 1, len = 1)
    schema2 <- schema[names(schema) != "prefix"]

    process_prefix <- function(x) {
      np <- nchar(prefix)
      x <- x[stringr::str_starts(string = x, pattern = prefix) & nchar(x) > np]
      substr(x, start = np+1, stop = nchar(x))
    }
  } else {
    prefix <- ""
    schema2 <- schema
    process_prefix <- function(x) {x}
  }

  checkmate::assert_character(schema2, null.ok = TRUE, min.len = 1, max.len = 2, min.chars = 1)

  if (is.null(schema)) {
    if (dbms(con) == "sql server") {
      # return temp tables
      # tempdb.sys.objects
      temp_tables <- DBI::dbGetQuery(con, "select * from tempdb..sysobjects")[[1]] %>%
        stringr::str_remove("_________________________.*$") %>%
        stringr::str_remove("^#+")

      return(temp_tables)

    } else if (dbms(con) == "snowflake") {
      # SHOW TERSE TABLES is limited to 10,000 rows; use information_schema to avoid that cap
      sql <- "SELECT table_name AS name FROM information_schema.tables WHERE table_catalog = CURRENT_DATABASE() AND table_schema = CURRENT_SCHEMA()"
      return(DBI::dbGetQuery(con, sql)$name)
    } else {
      return(DBI::dbListTables(con))
    }
  }

  withr::local_options(list(arrow.pull_as_vector = TRUE))

  if (methods::is(con, "DatabaseConnectorJdbcConnection")) {
    out <- DBI::dbListTables(con, databaseSchema = paste0(schema2, collapse = "."))
    return(process_prefix(out))
  }

  if (methods::is(con, "PqConnection") || methods::is(con, "RedshiftConnection")) {

    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {unname(schema2[1])};", .con = con)
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(.data$table_name)
    return(process_prefix(out))
  }

  if (methods::is(con, "duckdb_connection")) {
    if (length(schema2) == 2) {
      # Multiple catalogs (e.g. ATTACH): filter by table_catalog and table_schema
      sql <- glue::glue_sql(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = {schema2[[1]]} AND table_schema = {schema2[[2]]};",
        .con = con
      )
    } else {
      sql <- glue::glue_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = {schema2[[1]]};", .con = con)
    }
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(.data$table_name)
    return(process_prefix(out))
  }

  if (methods::is(con, "Snowflake")) {
    if (length(schema2) == 2) {
      sql <- glue::glue("select table_name from {schema2[1]}.information_schema.tables where table_schema = '{schema2[2]}';")
    } else {
      sql <- glue::glue("select table_name from information_schema.tables where table_schema = '{schema2[1]}';")
    }
    out <- DBI::dbGetQuery(con, sql) %>% dplyr::pull(1)
    return(process_prefix(out))
  }

  if (methods::is(con, "Spark SQL")) {
    # spark odbc connection
    sql <- paste("SHOW TABLES", if (!is.null(schema2)) paste("IN", paste(schema2, collapse = ".")))
    out <- DBI::dbGetQuery(con, sql) %>%
      dplyr::filter(.data$isTemporary == FALSE) %>%
      dplyr::pull(.data$tableName)

    return(process_prefix(out))
  }

  if (methods::is(con, "OdbcConnection")) {
    if (length(schema2) == 1) {
      out <- DBI::dbListTables(con, schema_name = schema2)
    } else if (length(schema2) == 2) {
      out <- DBI::dbListTables(con, catalog_name = schema2[[1]], schema_name = schema2[[2]])
    } else rlang::abort("schema missing!")

    return(process_prefix(out))
  }

  if (methods::is(con, "OraConnection")) {
    checkmate::assert_character(schema2, null.ok = TRUE, len = 1, min.chars = 1)
    out <- DBI::dbListTables(con, schema = schema2)
    return(process_prefix(out))
  }

  if (methods::is(con, "BigQueryConnection")) {
    if (length(schema2) == 1) {
      out <- DBI::dbGetQuery(con,
                             glue::glue("SELECT table_name
                         FROM `{schema2}`.INFORMATION_SCHEMA.TABLES
                         WHERE table_schema = '{schema2}'"))[[1]]
    } else if (length(schema2) == 2) {
      out <- DBI::dbGetQuery(con,
                             glue::glue("SELECT table_name
                         FROM `{schema2[[1]]}`.`{schema2[[2]]}`.INFORMATION_SCHEMA.TABLES
                         WHERE table_schema = '{schema2[[2]]}'"))[[1]]
    } else {
      rlang::abort("schema must be length 1 or 2")
    }

    return(process_prefix(out))
  }

  if (methods::is(con, "DatabaseConnectorDbiConnection")) {
    return(listTables(con@dbiConnection, schema = schema))
  }

  rlang::abort(paste(paste(class(con), collapse = ", "), "connection not supported"))
}

# To silence warning <BigQueryConnection> uses an old dbplyr interface
# https://github.com/r-dbi/bigrquery/issues/508

#' @importFrom dbplyr dbplyr_edition
#' @method dbplyr_edition BigQueryConnection
#' @export
dbplyr_edition.BigQueryConnection <- function(con) 2L

# Create the cdm tables in a database
execute_ddl <- function(con, cdm_schema, cdm_version = "5.3", dbms = "duckdb", tables = tblGroup("all"), prefix = "") {

  specs <- spec_cdm_field[[cdm_version]] %>%
    dplyr::mutate(cdmDatatype = dplyr::if_else(.data$cdmDatatype == "varchar(max)", "varchar(2000)", .data$cdmDatatype)) %>%
    dplyr::mutate(cdmFieldName = dplyr::if_else(.data$cdmFieldName == '"offset"', "offset", .data$cdmFieldName)) %>%
    dplyr::mutate(cdmDatatype = dplyr::case_when(
      dbms(con) == "postgresql" & .data$cdmDatatype == "datetime" ~ "timestamp",
      dbms(con) == "redshift" & .data$cdmDatatype == "datetime" ~ "timestamp",
      TRUE ~ cdmDatatype)) %>%
    tidyr::nest(col = -"cdmTableName") %>%
    dplyr::mutate(col = purrr::map(col, ~stats::setNames(as.character(.$cdmDatatype), .$cdmFieldName)))

  for (i in cli::cli_progress_along(tables)) {
    fields <- specs %>%
      dplyr::filter(.data$cdmTableName == tables[i]) %>%
      dplyr::pull(.data$col) %>%
      unlist()

    DBI::dbCreateTable(con, .inSchema(cdm_schema, paste0(prefix, tables[i]), dbms = dbms(con)), fields = fields)
  }
}

# get a unique prefix based on current time. internal function.
unique_prefix <- function() {
  as.integer((as.numeric(Sys.time())*10) %% 1e6)
}


# Borrowed from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
isInstalled <- function(pkg, version = "0") {
  installedVersion <- tryCatch(utils::packageVersion(pkg),
                                error = function(e) NA
  )
  !is.na(installedVersion) && installedVersion >= version
}

# Borrowed and adapted from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensureInstalled <- function(pkg, version = "0") {
  if (!isInstalled(pkg, version)) {
    versionText <- ifelse(version == "0", "", paste(">=", version))
    msg <- paste(sQuote(pkg), versionText, "must be installed for this functionality.")
    if (interactive()) {
      rlang::inform(paste(msg, "Would you like to install it?", sep = "\n"))
      if (utils::menu(c("Yes", "No")) == 1) {
        utils::install.packages(pkg)
      } else {
        stop(msg, call. = FALSE)
      }
    } else {
      stop(msg, call. = FALSE)
    }
  }
}

mapTypes <- function(conn, type) {
  # mapping types only used for some cases with bigquery (DBI) - e.g. generateCohortSet tests
  if(!(dbms(conn) %in% c("bigquery"))){
    return(type)
  }

  if (type %in% c("integer", "integer64")) {
    return("INT")
  } else if (type == "character") {
    return("STRING")
  }

  return(type)
}

# create table function adjusted to work with DatabaseConnector and bigquery
dcCreateTable <- function(conn, name, fields) {

  if (tibble::is_tibble(fields)) {
    fieldsSql <- paste(names(fields),
      sapply(fields, function(x) mapTypes(conn, class(x)[1])),
      collapse = ", "
    )
  } else {
    fields <- sapply(names(fields), function(field) {
      paste(field, fields[[field]], sep = " ")
    })
    fieldsSql <- paste(fields, collapse = ", ")
  }

  if (is.character(name)) {
    tableName <- paste(name, collapse = ".")
  } else {
    tableName <- paste(name@name, collapse = ".")
  }

  if (!(dbms(conn) %in% c("bigquery"))){
    createTableSQL <- SqlRender::render("CREATE TABLE @a ( @b );", a = tableName, b = fieldsSql)

    createTableSQLTranslated <- SqlRender::translate(createTableSQL, dbms(conn))
  } else {
    createTableSQLTranslated <- glue::glue("CREATE TABLE `{tableName}` ({fieldsSql});")
  }

  return(createTableSQLTranslated)
}

# branching logic: which createTable function to use based on the connection type
.dbCreateTable <- function(conn, name, fields) {
  if (methods::is(conn, "DatabaseConnectorConnection") || dbms(conn)  %in% c("bigquery")) {

    createTableSQLTranslated <- dcCreateTable(conn, name, fields)
    DBI::dbExecute(conn, createTableSQLTranslated)

  } else {
  DBI::dbCreateTable(conn, name, fields)
  }
}

# build and execute the SQL query to insert data into the table
# .dbInsertData <- function(conn, name, table) {
#   columns <- colnames(table)
#   values <- apply(table, 1, function(row) paste0("(", paste(shQuote(row), collapse = ", "), ")"))
#
#   tableName <- paste(name@name, collapse = ".")
#
#   query <- paste(
#     "INSERT INTO", tableName, "(", paste(columns, collapse = ", "), ")",
#     "VALUES", paste(values, collapse = ", ")
#   )
#
#   queryTranslated <- SqlRender::translate(query, dbms(conn))
#
#   tryCatch({
#     DBI::dbExecute(conn, queryTranslated)
#     cat("Data inserted successfully.\n")
#   }, error = function(e) {
#     cat("Error inserting data:", e$message, "\n")
#   })
# }

#' Compute a hash for each CDM table
#'
#' @details
#' This function is used to track changes in CDM databases. It returns a
#' dataframe with one hash for each table. The hash is based on the overall row count
#' and the number of unique values of one column of the table. For clinical tables
#' we count the number of unique concept IDs. For some tables we do not calculate
#' any unique value count (e.g. the location table) and simply use the total
#' row count.
#'
#' `r lifecycle::badge("experimental")
#'
#' @param cdm A cdm_reference object created by `cdmFromCon`
#'
#' @return A dataframe with one row per table, row counts, unique value counts for one column, and a hash
#' @export
#'
#' @examples
#' \dontrun{
#'  library(CDMConnector)
#'  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'  cdm <- cdmFromCon(con, "main", "main")
#'  computeDataHashByTable(cdm)
#'  cdmDisconnect(cdm)
#' }
computeDataHashByTable <- function(cdm) {
  overallStartTime <- Sys.time()
  ensureInstalled("digest")

  con <- cdmCon(cdm)

  cdmSchema <- attr(cdm, "cdm_schema")

  cdmTables <- c(
    "person" = "person_id",
    "observation_period" = "person_id",
    "visit_occurrence" = "visit_concept_id",
    "condition_occurrence" = "condition_concept_id",
    "drug_exposure" = "drug_concept_id",
    "procedure_occurrence" = "procedure_concept_id",
    "measurement" = "measurement_concept_id",
    "observation" = "observation_concept_id",
    "death" = "person_id",
    "location" = "NA",
    "care_site" = "NA",
    "provider" = "NA",
    "drug_era" = "drug_concept_id",
    "dose_era" = "NA",
    "condition_era" = "condition_concept_id",
    "concept" = "concept_id",
    "vocabulary" = "vocabulary_id",
    "concept_relationship" = "concept_id_1",
    "concept_ancestor" = "ancestor_concept_id",
    "concept_synonym" = "concept_id",
    "drug_strength" = "drug_concept_id",
    "cdm_source" = "NA")

  tablesInCdmSchema <- tolower(listTables(con, schema = cdmSchema))

  out <- dplyr::tibble(
    cdm_name = character(),
    table_name = character(),
    table_row_count = integer(),
    unique_column = character(),
    n_unique_values = integer(),
    table_hash = character()
  )

  for (i in seq_along(cdmTables)) {
    pct <- ""
    cli::cli_progress_step("Computing hash for {names(cdmTables)[i]} ({i}/{length(cdmTables)}{pct})", spinner = interactive())

    startTime <- Sys.time()
    tableName <- names(cdmTables)[i]
    uniqueColumn <- cdmTables[i]

    if (tableName %in% tablesInCdmSchema) {
      table_ref <- dplyr::tbl(con, dplyr::in_schema(cdmSchema, tableName))
      if (uniqueColumn != "NA") {
        result <- table_ref |>
          dplyr::summarise(
            n = dplyr::n(),
            n_unique = dplyr::n_distinct(.data[[uniqueColumn]])
          ) |>
          dplyr::collect()
      } else {
        result <- table_ref |>
          dplyr::summarise(
            n = dplyr::n(),
            n_unique = -1L
          ) |>
          dplyr::collect()
      }

      colnames(result) <- tolower(colnames(result))

      tableHash <- digest::digest(
        paste0(tableName, as.character(result$n), uniqueColumn, as.character(result$n_unique)),
        "md5")

      delta <- as.numeric(difftime(Sys.time(), startTime, units = "mins"))

      df <- dplyr::tibble(
        cdm_name = cdmName(cdm),
        table_name = tableName,
        table_row_count = as.integer(result$n),
        unique_column = uniqueColumn,
        n_unique_values = as.integer(result$n_unique),
        table_hash = tableHash,
        compute_time_minutes = delta
      )
    } else {
      df <- dplyr::tibble(
        cdm_name = cdmName(cdm),
        table_name = tableName,
        table_row_count = -1L,
        unique_column = uniqueColumn,
        n_unique_values = -1L,
        table_hash = "Table not found in CDM schema"
      )
    }

    out <- dplyr::bind_rows(out, df)

    if (interactive()) {
      pct <- ifelse(i == length(cdmTables), "", glue::glue(" ~ {floor(100*i/length(cdmTables))}%"))
      cli::cli_progress_update()
    }
  }
  cli::cli_inform("")
  delta2 <- Sys.time() - overallStartTime
  cli::cli_inform(c("*" = "Computing CDM table hashes took {round(delta2, 2)} {attr(delta2, 'units')}"))
  return(out)
}

#' Insert flattened CDM records as an aligned R comment in the active editor
#'
#' Flattens a CDM using [CDMConnector::cdmFlatten()], collects the data into R,
#' optionally filters by one or more `person_id` values, and inserts an aligned,
#' copy-pasteable comment block directly below the current cursor line in the
#' active RStudio document.
#'
#' This is intended as a lightweight debugging and documentation helper when
#' inspecting patient-level timelines (e.g. cohort inclusion, outcome validation,
#' or study review notes).
#'
#' @param cdm A CDM reference object created with CDMConnector.
#' @param personIds Optional numeric vector of `person_id` values to filter on.
#'   If `NULL`, all persons in the flattened CDM are used (use with care).
#'
#' @return Invisibly returns the collected flattened CDM as a data.frame.
#'   The primary side effect is insertion of commented text into the active
#'   RStudio source editor.
#'
#' @details
#' The inserted output is formatted as aligned R comments:
#'
#' \preformatted{
#' # person_id | observation_concept_id | start_date | end_date   | domain
#' # 12        | 2211751                 | 2021-01-13 | 2021-01-13 | procedure_occurrence
#' }
#'
#' The function requires an interactive RStudio session and will error if
#' `rstudioapi` is not available.
#'
#' @seealso
#' [CDMConnector::cdmFlatten()]
#'
#' @examples
#' \dontrun{
#' # Insert patient timeline directly into your script
#' cdmCommentContents(cdm, 12)
#'
#' # Insert multiple patients
#' cdmCommentContents(cdm, c(12, 22))
#' }
#'
#' @keywords internal
#' @noRd
.cdm_comment_interactive <- function() base::interactive()

#' @keywords internal
#' @noRd
.cdm_comment_require_rstudioapi <- function() requireNamespace("rstudioapi", quietly = TRUE)

#' @keywords internal
#' @noRd
.cdm_comment_rstudio_available <- function() rstudioapi::isAvailable()

#' @keywords internal
#' @noRd
.cdm_comment_get_context <- function() rstudioapi::getActiveDocumentContext()

#' @keywords internal
#' @noRd
.cdm_comment_doc_position <- function(row, column) rstudioapi::document_position(row, column)

#' @keywords internal
#' @noRd
.cdm_comment_insert_text <- function(location, text) rstudioapi::insertText(location, text)

#' @keywords internal
#' @noRd
.cdm_comment_flatten <- function(cdm) CDMConnector::cdmFlatten(cdm)

#' @keywords internal
#' @noRd
.cdm_comment_collect <- function(x) dplyr::collect(x)

#' Insert Patient CDM Contents as Aligned Comments in RStudio
#'
#' This function retrieves the longitudinal event table for one or more persons
#' in a CDM object and inserts it as a nicely formatted, R-style comment block
#' directly into your active RStudio document. This is particularly useful for
#' documenting reproducible test cases or examples by showing relevant CDM
#' contents inline in test scripts or analysis code.
#'
#' Each row of patient data will be aligned in columns as a commented table, making
#' it easy to copy, review, and maintain sample data expectations in documentation
#' or test suites.
#'
#' @param cdm A CDMConnector cdm_reference object.
#' @param personIds Optional numeric vector of person IDs to filter the rows to include.
#'   If NULL (default), includes all persons in the cdm.
#'
#' @details
#' Requires an interactive RStudio session with the \code{rstudioapi} package available.
#' The function utilizes \code{CDMConnector::cdmFlatten()} to extract a longitudinal view,
#' and writes the commented results directly below the cursor in the active RStudio document.
#'
#' This workflow is especially helpful when documenting expected patient timelines for use in
#' testthat or other test scripts, or when sharing reproducible CDM content for instructional
#' examples.
#'
#' @seealso \code{\link[CDMConnector]{cdmFlatten}}
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#' cdm <- cdmFromCon(con, "main", "main")
#' cdmCommentContents(cdm, personIds = 6)
#' # person_id | observation_concept_id | start_date | end_date   | type_concept_id...
#' # 6         | 40213296               | 2006-01-10 | 2006-01-10 | 581452         ...
#' # 6         | 40213227               | 2006-01-10 | 2006-01-10 | 581452         ...
#' # 6         | 1118084                | 2005-07-13 | 2005-07-13 | 38000177       ...
#' # 6         | 80180                  | 2005-07-13 | NA         | 32020          ...
#' cdmDisconnect(cdm)
#' }
#'
#' @export
cdmCommentContents <- function(cdm, personIds = NULL) {
  # This function only makes sense in interactive use.
  if (!.cdm_comment_interactive()) return(invisible(NULL))

  if (!.cdm_comment_require_rstudioapi()) {
    stop("Package 'rstudioapi' is required.", call. = FALSE)
  }
  if (!.cdm_comment_rstudio_available()) {
    stop("RStudio is required (rstudioapi is not available in this session).", call. = FALSE)
  }

  if (!is.null(personIds) && !is.numeric(personIds)) {
    stop("`personIds` must be a numeric vector (or NULL).", call. = FALSE)
  }

  # 1) Run the pipeline (cdmFlatten -> collect -> optional filter -> arrange)
  flat <- .cdm_comment_flatten(cdm) |>
    .cdm_comment_collect()

  if (!is.null(personIds)) {
    flat <- dplyr::filter(flat, .data$person_id %in% personIds)
  }

  flat <- dplyr::arrange(
    flat,
    .data$person_id,
    dplyr::desc(.data$start_date),
    dplyr::desc(.data$end_date)
  )

  # 2) Convert to aligned comment text (like your first format)
  df_chr <- as.data.frame(lapply(flat, as.character), stringsAsFactors = FALSE)

  widths <- vapply(
    names(df_chr),
    function(col) max(nchar(c(col, df_chr[[col]])), na.rm = TRUE),
    integer(1)
  )

  pad <- function(x, w) sprintf(paste0("%-", w, "s"), x)

  lines <- character()
  header <- paste(mapply(pad, names(df_chr), widths), collapse = " | ")
  lines <- c(lines, paste0("# ", header))

  for (i in seq_len(nrow(df_chr))) {
    row <- paste(mapply(pad, df_chr[i, ], widths), collapse = " | ")
    lines <- c(lines, paste0("# ", row))
  }

  # 3) Insert directly below the current line in the active RStudio document
  ctx <- .cdm_comment_get_context()
  contents <- ctx$contents

  # RStudio rows are 1-based
  cursor_row <- ctx$selection[[1]]$range$start[["row"]]

  # Find the most recent call line above (or at) the cursor
  call_row <- cursor_row
  while (call_row >= 1 && !grepl("\\bcdmCommentContents\\s*\\(", contents[[call_row]])) {
    call_row <- call_row - 1
  }

  if (call_row < 1) {
    stop("Couldn't find a line containing `cdmCommentContents(` above the cursor.", call. = FALSE)
  }

  # Insert *immediately below* the call line
  .cdm_comment_insert_text(
    location = .cdm_comment_doc_position(call_row + 1, 1),
    text = paste0(paste(lines, collapse = "\n"), "\n")
  )

  invisible(flat)
}


