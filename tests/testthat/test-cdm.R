library(testthat)
library(dplyr)

test_that("cdm reference works locally", {
  skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                        user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                        password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"), select = tbl_group("vocab"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  DBI::dbDisconnect(con)
})


test_that("cdm reference works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), select = tbl_group("vocab"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  DBI::dbDisconnect(con)
})


test_that("cdm reference works on sql server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = "ODBC Driver 18 for SQL Server",
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate="yes",
                        Port     = 1433)

  expect_s3_class(listTables(con, schema = c("CDMV5", "dbo")), "character")

  cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"), select = tbl_group("vocab"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on sql server", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), select = tbl_group("vocab"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  DBI::dbDisconnect(con)
})


test_that("cdm reference works on duckdb", {

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  cdm <- cdm_from_con(con, select = tbl_group("vocab"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  DBI::dbDisconnect(con, shutdown = TRUE)
})





