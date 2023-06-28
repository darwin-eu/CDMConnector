withr::local_envvar(
  R_USER_CACHE_DIR = tempfile(),
  .local_envir = teardown_env(),
  EUNOMIA_DATA_FOLDER = Sys.getenv("EUNOMIA_DATA_FOLDER", unset = tempfile())
)

tryCatch({
  if (Sys.getenv("skip_eunomia_download_test") != "TRUE") downloadEunomiaData(overwrite = TRUE)
  }, error = function(e) NA)

# functions used for the test matrix

get_connection <- function(dbms) {
  if (dbms == "duckdb") {
    return(DBI::dbConnect(duckdb::duckdb(), eunomia_dir()))
  }

  if (dbms == "postgres") {
    return(DBI::dbConnect(RPostgres::Postgres(),
                          dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                          host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                          user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                          password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")))
  }

  if (dbms == "local" && Sys.getenv("LOCAL_POSTGRESQL_DBNAME") != "") {
    return(DBI::dbConnect(RPostgres::Postgres(),
                          dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                          host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                          user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                          password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD")))
  }

  if (dbms == "redshift" && Sys.getenv("CDM5_REDSHIFT_DBNAME") != "") {
    return(DBI::dbConnect(RPostgres::Redshift(),
                          dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                          host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                          port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                          user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                          password = Sys.getenv("CDM5_REDSHIFT_PASSWORD")))
  }

  if (dbms == "sqlserver" && Sys.getenv("SQL_SERVER_DRIVER") != "") {
    return(DBI::dbConnect(odbc::odbc(),
                          Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                          Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                          Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                          UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                          PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                          TrustServerCertificate="yes",
                          Port     = 1433))
  }

  if (dbms == "oracle" && "OracleODBC-19" %in% odbc::odbcListDataSources()$name) {
    return(DBI::dbConnect(odbc::odbc(), "OracleODBC-19"))
  }

  if (dbms == "bigquery" && Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH") != "") {

    bigrquery::bq_auth(path = Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH"))

    return(DBI::dbConnect(
      bigrquery::bigquery(),
      project = Sys.getenv("BIGQUERY_PROJECT_ID"),
      dataset = Sys.getenv("BIGQUERY_CDM_SCHEMA")
    ))
  }

  if (dbms == "snowflake" && "Snowflake" %in% odbc::odbcListDataSources()$name) {
    return(DBI::dbConnect(odbc::odbc(), "Snowflake"))
  }

  return(invisible(NULL))
}

get_cdm_schema <- function(dbms) {
  switch (dbms,
          "postgres" = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
          "local" =  Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"),
          "redshift" = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
          "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]],
          "oracle" =  Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
          "duckdb" = "main",
          "bigquery" = Sys.getenv("BIGQUERY_CDM_SCHEMA"),
          "snowflake" = strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]],
          NULL
  )
}

get_write_schema <- function(dbms) {
  switch (dbms,
          "postgres" = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"),
          "local" = Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA"),
          "redshift" = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"),
          "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]],
          "oracle" = Sys.getenv("CDM5_ORACLE_SCRATCH_SCHEMA"),
          "duckdb" = "main",
          "bigquery" = Sys.getenv("BIGQUERY_SCRATCH_SCHEMA"),
          "snowflake" = strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]],
          NULL
  )
}

disconnect <- function(con) {
  if (is.null(con)) return(invisible(NULL))

  if (dbms(con) == "duckdb") {
    DBI::dbDisconnect(con, shutdown = TRUE)
  } else {
    DBI::dbDisconnect(con)
  }
}

