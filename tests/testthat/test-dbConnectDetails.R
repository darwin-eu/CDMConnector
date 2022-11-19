library(testthat)
library(dplyr, warn.conflicts = FALSE)
library(DBI)

self_contained_query <- function(connection_details, cdm_schema) {
  suppressMessages({
    con <- DBI::dbConnect(connection_details)
  })
  if(dbms(con) == "duckdb") {
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  } else {
    on.exit(DBI::dbDisconnect(con))
  }
  DBI::dbGetQuery(con, paste0("select count(*) as n from ", cdm_schema, ".person"))
}

test_that("dbConnectDetails works on local postgres", {
  skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")
  details <- dbConnectDetails(RPostgres::Postgres(),
                              dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                              host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                              user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                              password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  result <- self_contained_query(details, Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

test_that("dbConnectDetails works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  details <- dbConnectDetails(RPostgres::Postgres(),
                        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  result <- self_contained_query(details, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})


test_that("dbConnectDetails works on sql server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")

  details <- dbConnectDetails(odbc::odbc(),
                        Driver   = "ODBC Driver 18 for SQL Server",
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate="yes",
                        Port     = 1433)

  result <- self_contained_query(details, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

test_that("dbConnectDetails works on Redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  details <- dbConnectDetails(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  result <- self_contained_query(details, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

test_that("dbConnectDetails works on duckdb", {
  skip_if_not_installed("duckdb", minimum_version = "0.5.0")
  details <- dbConnectDetails(duckdb::duckdb(), dbdir = eunomia_dir())

  result <- self_contained_query(details, "main")
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

# DatabaseConnector tests ----
test_that("dbConnectDetails works on postgres using DatabaseConnector", {
  skip_on_ci()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")

  if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") == "") {
    Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "~/DATABASECONNECTOR_JAR_FOLDER")
    DatabaseConnector::downloadJdbcDrivers("postgresql")
  }

  details <- dbConnectDetails(DatabaseConnector::DatabaseConnectorDriver(),
                              dbms = "postgresql",
                              server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                              user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                              password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  result <- self_contained_query(details, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

test_that("dbConnectDetails works on SQL Server using DatabaseConnector", {
  skip_on_ci()
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")

  if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") == "") {
    Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "~/DATABASECONNECTOR_JAR_FOLDER")
    DatabaseConnector::downloadJdbcDrivers("sql server")
  }

  details <- dbConnectDetails(DatabaseConnector::DatabaseConnectorDriver(),
                              dbms = "sql server",
                              server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                              user     = Sys.getenv("CDM5_SQL_SERVER_USER"),
                              password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))

  result <- self_contained_query(details, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

test_that("dbConnectDetails works on redshift using DatabaseConnector", {
  skip_on_ci()
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") == "") {
    Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "~/DATABASECONNECTOR_JAR_FOLDER")
    DatabaseConnector::downloadJdbcDrivers("redshift")
  }

  details <- dbConnectDetails(DatabaseConnector::DatabaseConnectorDriver(),
                              dbms = "redshift",
                              server   = Sys.getenv("CDM5_REDSHIFT_SERVER"),
                              user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                              password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  result <- self_contained_query(details, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
  expect_s3_class(result, "data.frame")
  expect_true("n" %in% names(result))
})

