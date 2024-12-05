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

#' Create a CDM object from a pre-defined set of environment variables
#'
#' This function is intended to be used with the Darwin execution engine. The execution engine
#' runs OHDSI studies in a pre-defined runtime environment and makes several environment
#' variables available for connecting to a CDM database. Programmer writing code to run
#' on the execution engine and simply use `cdm <- cdmFromEnvironment()` to create a cdm reference
#' object to use for their analysis and the database connection and cdm object should be
#' automatically created. This obviates the need for site specific code for connecting
#' to the database and creating the cdm reference object.
#'
#' The environment variables used by this function and provided by the execution engine are listed below.
#'
#' \itemize{
#'    \item{DBMS_TYPE: one of "postgresql", "sql server", "redshift", "duckdb", "snowflake".}
#'    \item{DATA_SOURCE_NAME: a free text name for the CDM given by the person running the study.}
#'    \item{CDM_VERSION: one of "5.3", "5.4".}
#'    \item{DBMS_CATALOG: The database catalog. Important primarily for compound schema names used in SQL Server and Snowflake.}
#'    \item{DBMS_SERVER: The database server URL.}
#'    \item{DBMS_NAME: The database name used for creating the connection.}
#'    \item{DBMS_PORT: The database port number.}
#'    \item{DBMS_USERNAME: The database username needed to authenticate.}
#'    \item{DBMS_PASSWORD: The database password needed to authenticate.}
#'    \item{CDM_SCHEMA: The schema name where the OMOP CDM is located in the database.}
#'    \item{WRITE_SCHEMA: The shema where the user has write access and tables will be created during study execution.}
#' }
#'
#'
#' @param write_prefix,writePrefix (string) An optional prefix to use for all tables written to the CDM.
#'
#' @return A cdm_reference object
#' @export
#'
#' @examples
#' \dontrun{
#'
#' library(CDMConnector)
#'
#' # This will only work in an evironment where the proper variables are present.
#' cdm <- cdmFromEnvironment()
#'
#' # Proceed with analysis using the cdm object.
#'
#' # Close the database connection when done.
#' cdmDisconnect(cdm)
#' }
cdmFromEnvironment <- function(writePrefix = "") {

  lifecycle::deprecate_soft("1.7.0", "cdmFromEnvironment()")

  vars <- c("DBMS_TYPE",
            "DATA_SOURCE_NAME",
            "CDM_VERSION",
            "DBMS_CATALOG",
            "DBMS_SERVER",
            "DBMS_NAME",
            "DBMS_PORT",
            "DBMS_USERNAME",
            "DBMS_PASSWORD",
            "CDM_SCHEMA",
            "WRITE_SCHEMA")

  supported_db <- c("postgresql", "sql server", "redshift", "duckdb", "snowflake")

  if (!(Sys.getenv("DBMS_TYPE") %in% supported_db)) {
    cli::cli_abort("The environment variable DBMS_TYPE must be on one of {paste(supported_db, collapse = ', ')} not `{Sys.getenv('DBMS_TYPE')}`.")
  }

  if (Sys.getenv("DBMS_TYPE") == "duckdb") {
    db <- Sys.getenv("DBMS_NAME")
    if (db == "") {
      db <- "GiBleed"
    }

    checkmate::assert_choice(db, CDMConnector::example_datasets())
    con <- DBI::dbConnect(duckdb::duckdb(), CDMConnector::eunomia_dir(db))
    cdm <- CDMConnector::cdmFromCon(con, "main", "main", cdmVersion = "5.3", cdmName = db)
    return(cdm)
  }

  # "DBMS_CATALOG" is not required
  for (v in vars) {
    if (Sys.getenv(v) == "" && v != "DBMS_CATALOG") {
      cli::cli_abort("Environment variable {v} is required but not set!")
    }
  }

  stringr::str_count(Sys.getenv("CDM_SCHEMA"), "\\.")

  if (Sys.getenv("DBMS_TYPE") %in% c("postgresql", "redshift")) {

    drv <- switch (Sys.getenv("DBMS_TYPE"),
                   "postgresql" = RPostgres::Postgres(),
                   "redshift" = RPostgres::Redshift()
    )

    con <- DBI::dbConnect(drv = drv,
                          dbname   = Sys.getenv("DBMS_NAME"),
                          host     = Sys.getenv("DBMS_SERVER"),
                          user     = Sys.getenv("DBMS_USERNAME"),
                          password = Sys.getenv("DBMS_PASSWORD"),
                          port     = Sys.getenv("DBMS_PORT"))

    if (!DBI::dbIsValid(con)) {
      cli::cli_abort("Database connection failed!")
    }

  } else if (Sys.getenv("DBMS_TYPE") == "sql server") {

    con <- DBI::dbConnect(odbc::odbc(),
                          Driver   = "ODBC Driver 17 for SQL Server",
                          Server   = Sys.getenv("DBMS_SERVER"),
                          Database = Sys.getenv("DBMS_NAME"),
                          UID      = Sys.getenv("DBMS_USERNAME"),
                          PWD      = Sys.getenv("DBMS_PASSWORD"),
                          TrustServerCertificate="yes",
                          Port     = Sys.getenv("DBMS_PORT"))

    if (!DBI::dbIsValid(con)) {
      cli::cli_abort("Database connection failed!")
    }


  } else if (Sys.getenv("DBMS_TYPE") == "snowflake") {
    con <- DBI::dbConnect(odbc::odbc(),
                          DRIVER    = "SnowflakeDSIIDriver",
                          SERVER    = Sys.getenv("DBMS_SERVER"),
                          DATABASE  = Sys.getenv("DBMS_NAME"),
                          UID       = Sys.getenv("DBMS_USERNAME"),
                          PWD       = Sys.getenv("DBMS_PASSWORD"),
                          WAREHOUSE = "COMPUTE_WH_XS")

    if (!DBI::dbIsValid(con)) {
      cli::cli_abort("Database connection failed!")
    }

  } else {
    cli::cli_abort("{Sys.getenv('DBMS_TYPE')} is not a supported database type!")
  }

  # split schemas. If write schema has a dot we need to interpret it as catalog.schema
  # cdm schema should not have a dot

  if (stringr::str_detect(Sys.getenv("WRITE_SCHEMA"), "\\.")) {
    write_schema <- stringr::str_split(Sys.getenv("WRITE_SCHEMA"), "\\.")[[1]]
    if (length(write_schema) != 2) {
      cli::cli_abort("write_schema can have at most one period (.)!")
    }

    stopifnot(nchar(write_schema[1]) > 0, nchar(write_schema[2]) > 0)
    write_schema <- c(catalog = write_schema[1], schema = write_schema[2])
  } else {
    write_schema <- c(schema = Sys.getenv("WRITE_SCHEMA"))
  }

  if (writePrefix != "") {
    if (Sys.getenv("DBMS_TYPE") != "snowflake") {
      write_schema <- c(write_schema, prefix = writePrefix)
    }
  }

  if (stringr::str_detect(Sys.getenv("CDM_SCHEMA"), "\\.")) {
    cli::cli_abort("CDM_SCHEMA cannot contain a period (.)! Use DBMS_CATALOG to add a catalog.")
  }

  if (Sys.getenv("DBMS_CATALOG") != "") {
    cdm_schema <- c(catalog = Sys.getenv("DBMS_CATALOG"), schema = Sys.getenv("CDM_SCHEMA"))
  } else {
    cdm_schema <- Sys.getenv("CDM_SCHEMA")
  }

  cdm <- CDMConnector::cdmFromCon(
    con = con,
    cdmSchema = cdm_schema,
    writeSchema = write_schema,
    cdmVersion = Sys.getenv("CDM_VERSION"),
    cdmName = Sys.getenv("DATA_SOURCE_NAME"))

  if (length(names(cdm)) == 0) {
    cli::cli_abort("CDM object creation failed!")
  }

  return(cdm)
}

#' `r lifecycle::badge("deprecated")
#' @rdname cdmFromEnvironment
#' @export
cdm_from_environment <- function(write_prefix = ""){
  lifecycle::deprecate_soft("1.7.0", "cdm_from_environment()")

  cdmFromEnvironment(writePrefix = write_prefix)
}
