library(testthat)

withr::local_envvar(
  R_USER_CACHE_DIR = tempfile(),
  .local_envir = teardown_env(),
  EUNOMIA_DATA_FOLDER = Sys.getenv("EUNOMIA_DATA_FOLDER", unset = tempfile())
)

tryCatch({
  if (Sys.getenv("skip_eunomia_download_test") != "TRUE") downloadEunomiaData(overwrite = TRUE)
}, error = function(e) NA)

# functions used for the test matrix

get_connection <- function(dbms, DatabaseConnector = FALSE) {

  if (DatabaseConnector) {
    stopifnot(rlang::is_installed("DatabaseConnector"))


    if (dbms == "postgres") {
      cli::cli_inform("Testing with DatabseConector on postgresql")
      return(
        con = DatabaseConnector::connect(
          dbms = "postgresql",
          server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
          user = Sys.getenv("CDM5_POSTGRESQL_USER"),
          password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
      )
    }

    if (dbms == "redshift") {
      cli::cli_inform("Testing with DatabseConector on redshift")
      return(
        DatabaseConnector::connect(
          dbms = "redshift",
          server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
          user = Sys.getenv("CDM5_REDSHIFT_USER"),
          password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"),
          port = Sys.getenv("CDM5_REDSHIFT_PORT"))
      )
    }

    stop(paste("Testing", dbms, "with DatabaseConnector has not been implemented yet."))

  }



  if (dbms == "duckdb") {
    return(DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir())))
  }

  if (dbms == "postgres" && Sys.getenv("CDM5_POSTGRESQL_DBNAME") != "") {
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

  if (dbms == "sqlserver" && Sys.getenv("CDM5_SQL_SERVER_USER") != "") {
    print(Sys.getenv("SQL_SERVER_DRIVER"))
    return(DBI::dbConnect(odbc::odbc(),
                          Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                          # Driver   = "ODBC Driver 17 for SQL Server", #asdf
                          Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                          Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                          UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                          PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                          TrustServerCertificate="yes",
                          Port     = Sys.getenv("CDM5_SQL_SERVER_PORT")))
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

  if (dbms == "snowflake" && Sys.getenv("SNOWFLAKE_USER") != "") {

    return(DBI::dbConnect(odbc::odbc(),
                          SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                          UID = Sys.getenv("SNOWFLAKE_USER"),
                          PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                          DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                          WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                          DRIVER = Sys.getenv("SNOWFLAKE_DRIVER")))
  }

  if (dbms == "spark" && Sys.getenv("DATABRICKS_HTTPPATH") != "") {
    # requires two other environment variables: DATABRICKS_HOST and DATABRICKS_TOKEN
    message("connecting to databricks")
    con <- DBI::dbConnect(
      odbc::databricks(),
      httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
      useNativeQuery = FALSE,
      bigint = "numeric"
    )
    return(con)
  }

  rlang::abort("Could not create connection. Are some environment variables missing?")
}

get_cdm_schema <- function(dbms) {
  s <- switch (dbms,
          "postgres" = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
          "local" =  Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"),
          "redshift" = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
          "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]],
          "oracle" =  Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
          "duckdb" = "main",
          "bigquery" = Sys.getenv("BIGQUERY_CDM_SCHEMA"),
          "snowflake" = strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]],
          "spark" = Sys.getenv("DATABRICKS_CDM_SCHEMA"),
          NULL
  )
  if (length(s) == 0) s <- ""
  return(s)
}

get_write_schema <- function(dbms, prefix = paste0("temp", floor(as.numeric(Sys.time())*100) %% 100000L, "_")) {
  s <- switch (dbms,
          "postgres" = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"),
          "local" = Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA"),
          "redshift" = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"),
          "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]],
          "oracle" = Sys.getenv("CDM5_ORACLE_SCRATCH_SCHEMA"),
          "duckdb" = "main",
          "bigquery" = Sys.getenv("BIGQUERY_SCRATCH_SCHEMA"),
          "snowflake" = strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]],
          "spark" = Sys.getenv("DATABRICKS_SCRATCH_SCHEMA"),
          NULL
  )
  if (length(s) == 0) s <- ""

  if (!is.null(prefix)) {
    if (length(s) == 1) {
      s <- c(schema = s[1], prefix = prefix)
    } else {
      s <- c(catalog = s[1], schema = s[2], prefix = prefix)
    }
  }

  return(s)
}

disconnect <- function(con) {
  if (is.null(con)) return(invisible(NULL))

  if (dbms(con) == "duckdb") {
    DBI::dbDisconnect(con, shutdown = TRUE)
  } else {
    DBI::dbDisconnect(con)
  }
}

# databases supported on github actions
ciTestDbs <- c("duckdb", "postgres", "redshift", "sqlserver", "snowflake")

if (Sys.getenv("CI_TEST_DB") == "") {

  dbToTest <- c(
     # "duckdb"
    # ,
    # "postgres"
    # ,
    "redshift"
    # ,
    # "sqlserver"
    # ,
    # "snowflake"
    # ,
    # "spark"
  )

  } else {
  checkmate::assert_choice(Sys.getenv("CI_TEST_DB"), choices = ciTestDbs)
  dbToTest <- Sys.getenv("CI_TEST_DB")
  print(paste("running CI tests on ", dbToTest))
}

if (Sys.getenv('TEST_USING_DATABASE_CONNECTOR') %in% c("TRUE", "FALSE")) {
  testUsingDatabaseConnector <- as.logical(Sys.getenv('TEST_USING_DATABASE_CONNECTOR'))
} else {
  testUsingDatabaseConnector <- T
}

# make sure we're only trying to test on dbs we have connection details for
if ("postgres" %in% dbToTest & Sys.getenv("CDM5_POSTGRESQL_DBNAME") == "") {
  dbToTest <- dbToTest[dbToTest != "postgres"]
  print("CI tests not run on postgres - CDM5_POSTGRESQL_DBNAME not found")
}
if ("redshift" %in% dbToTest & Sys.getenv("CDM5_REDSHIFT_DBNAME") == "") {
  dbToTest <- dbToTest[dbToTest != "redshift"]
  print("CI tests not run on redshift - CDM5_REDSHIFT_DBNAME not found")
}
if ("sqlserver" %in% dbToTest & Sys.getenv("CDM5_SQL_SERVER_USER") == "") {
  dbToTest <- dbToTest[dbToTest != "sqlserver"]
  print("CI tests not run on sqlserver - CDM5_SQL_SERVER_USER not found")
}
if ("snowflake" %in% dbToTest & Sys.getenv("SNOWFLAKE_USER") == "") {
  dbToTest <- dbToTest[dbToTest != "snowflake"]
  print("CI tests not run on snowflake - SNOWFLAKE_USER not found")
}
if (!rlang::is_installed("duckdb")) {
  dbToTest <- dbToTest[dbToTest != "duckdb"]
  print("CI tests not run on snowflake - duckdb package is not installed")
}
if ("spark" %in% dbToTest & Sys.getenv("DATABRICKS_HTTPPATH") == "") {
  dbToTest <- dbToTest[dbToTest != "spark"]
  print("CI tests not run on spark/databricks - DATABRICKS_HTTPPATH not found")
}
