library(testthat)
library(dplyr, warn.conflicts = FALSE)

### Using DBI drivers ------

test_that("cdm reference works locally", {
  skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                        user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                        password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"), cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "cdm_snapshot")

  expect_true(is.null(verify_write_access(con, write_schema = "scratch")))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "postgresql")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))))

  expect_null(verify_write_access(con, Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), cdm_tables = tbl_group("vocab"))

  expect_error(assert_tables(cdm, "person"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))

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
                        TrustServerCertificate = "yes",
                        Port     = 1433)

  expect_true(is.character(listTables(con, schema = c("CDMV5", "dbo"))))
  expect_true(is.character(listTables(con, schema = c("dbo"))))

  cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"), cdm_tables = tbl_group("vocab"))

  expect_error(assert_tables(cdm, "person"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_null(verify_write_access(con, write_schema = c("tempdb.dbo")))
  expect_null(verify_write_access(con, write_schema = c("tempdb", "dbo")))

  cohort <- dplyr::tibble(cohort_id = 1L,
                          subject_id = 1L:2L,
                          cohort_start_date = c(Sys.Date(), as.Date("2020-02-03")),
                          cohort_end_date = c(Sys.Date(), as.Date("2020-11-04")))

  DBI::dbWriteTable(con, DBI::Id(catalog = "tempdb", schema = "dbo", table = "cohort"), cohort, overwrite = TRUE)

  cdm <- cdm_from_con(con,
                      cdm_schema = c("CDMV5", "dbo"),
                      cdm_tables = tbl_group("vocab"),
                      write_schema = c("tempdb", "dbo"),
                      cohort_tables = "cohort")

  expect_equal(collect(cdm$cohort), cohort)

  DBI::dbRemoveTable(con, DBI::Id(catalog = "tempdb", schema = "dbo", table = "cohort"))

  expect_error(DBI::dbGetQuery(con, "select * from tempdb.dbo.cohort"), "Invalid object")

  expect_equal(dbms(con), "sql server")
  expect_equal(dbms(cdm), "sql server")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))


  expect_null(verify_write_access(con, Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")))

  expect_true(is.character(listTables(con, schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(con), "redshift")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on spark", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("Only run this test manually")

  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks")

  expect_true(is.character(listTables(con, schema = "omop531")))

  cdm <- cdm_from_con(con, cdm_schema = "omop531", cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  cdm$cdm_source
  debugonce(snapshot)
  # expect_s3_class(snapshot(cdm), "cdm_snapshot") # test database has upper case names

  expect_true(is.null(verify_write_access(con, write_schema = "omop531results")))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "spark")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on Oracle", {

  skip_on_ci()
  skip_on_cran()
  # skip("Only run this test manually")

  library(ROracle)
  con <- DBI::dbConnect(DBI::dbDriver("Oracle"),
                        username = Sys.getenv("CDM5_ORACLE_USER"),
                        password= Sys.getenv("CDM5_ORACLE_PASSWORD"),
                        dbname = Sys.getenv("CDM5_ORACLE_SERVER"))

  # allTables <- DBI::dbListTables(con, schema = "CDMV5", full = TRUE)
  writeSchema <- "TEMPEMULSCHEMA"
  cdmSchema <- "CDMV5"

  # List schemas
  # dbGetQuery(con, "select username as schema from sys.all_users")

  expect_true(is.character(listTables(con, schema = cdmSchema)))

  # Oracle test cdm v5.3 is missing visit_detail
  cdm <- cdm_from_con(con, cdm_schema = cdmSchema, cdm_tables = c(tbl_group("default"), -visit_detail))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  # expect_s3_class(snapshot(cdm), "cdm_snapshot") # test database person table is missing birth_datetime

  expect_true(is.null(verify_write_access(con, write_schema = writeSchema)))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "oracle")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on duckdb", {
  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  expect_null(verify_write_access(con, "main"))

  expect_true(is.character(listTables(con)))

  cdm <- cdm_from_con(con, cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "cdm_snapshot")

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("inclusion of cohort tables", {
  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  cohort <- dplyr::tibble(cohort_id = 1L,
                          subject_id = 1L:2L,
                          cohort_start_date = c(Sys.Date(), as.Date("2020-02-03")),
                          cohort_end_date = c(Sys.Date(), as.Date("2020-11-04")))

  DBI::dbExecute(con, "create schema write_schema;")

  expect_null(verify_write_access(con, "write_schema"))

  DBI::dbWriteTable(con, DBI::Id(schema = "write_schema", table_name = "cohort"), cohort)

  expect_equal(listTables(con, schema = "write_schema"), "cohort")

  cdm <- cdm_from_con(con,
                      cdm_tables = c("person", "observation_period"),
                      write_schema = "write_schema",
                      cohort_tables = "cohort")

  expect_output(print(cdm), "CDM")
  expect_equal(collect(cdm$cohort), cohort)
  DBI::dbDisconnect(con, shutdown = TRUE)

})

test_that("collect a cdm", {
  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  cdm <- cdm_from_con(con)

  local_cdm <- collect(cdm)

  expect_s3_class(local_cdm, "cdm_reference")
  expect_equal(local_cdm$person, collect(cdm$person))

  query <- function(con) DBI::dbGetQuery(con, "select count(*) as n from person")

  expect_equal(query(con), query(dbplyr::remote_con(cdm$person)))
  expect_equal(dbms(con), "duckdb")
  expect_equal(dbms(cdm), "duckdb")

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("stow and cdm_from_files works", {
  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomia_is_available())

  save_path <- file.path(tempdir(), paste0("tmp_", paste(sample(letters, 10, replace = TRUE), collapse = "")))
  dir.create(save_path)
  cdm_tables <- c("person", "observation_period", "cdm_source", "vocabulary")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  # Test tidyselect in cdm_from_con. Should not produce message about ambiguous names.
  expect_message(cdm_from_con(con, cdm_tables = tbl_group("vocab")), NA)
  expect_message(cdm_from_con(con, cdm_tables = matches("person|observation_period")), NA)
  expect_message(cdm_from_con(con, cdm_tables = c(person, observation_period)), NA)
  expect_message(cdm_from_con(con, cdm_tables = c("person", "observation_period")), NA)
  expect_message(cdm_from_con(con, cdm_tables = all_of(cdm_tables)), NA)

  cdm <- cdm_from_con(con, cdm_tables = all_of(cdm_tables))

  stow(cdm, path = save_path)

  expect_setequal(list.files(save_path), paste0(cdm_tables, ".parquet"))

  expect_message(cdm_from_files(save_path), NA)
  expect_warning(cdm_from_files(save_path, cdm_tables = "person"), "deprecated")

  local_cdm <- cdm_from_files(save_path)
  expect_s3_class(local_cdm, "cdm_reference")
  expect_equal(local_cdm$person, collect(cdm$person))
  expect_s3_class(snapshot(cdm), "cdm_snapshot")

  local_arrow_cdm <- cdm_from_files(save_path, as_data_frame = FALSE)
  expect_s3_class(local_arrow_cdm, "cdm_reference")
  expect_equal(collect(local_arrow_cdm$person), collect(cdm$person))
  expect_error(validate_cdm(local_arrow_cdm))

  DBI::dbDisconnect(con, shutdown = TRUE)
})


## Using DatabaseConnector DBI driver -----

library(testthat)
library(dplyr, warn.conflicts = FALSE)

test_that("DatabaseConnector cdm reference works on local postgres", {
  skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "postgresql",
                        server   = Sys.getenv("LOCAL_POSTGRESQL_SERVER"),
                        user     = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                        password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"), cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "cdm_snapshot")

  # debugonce(verify_write_access)
  expect_true(is.null(verify_write_access(con, write_schema = "scratch")))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "postgresql")

  DBI::dbDisconnect(con)
})

test_that("DatabaseConnector cdm reference works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "postgresql",
                        server   = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                        user     = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "cdm_snapshot")

  # debugonce(verify_write_access)
  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"))))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "postgresql")

  DBI::dbDisconnect(con)
})


test_that("DatabaseConnector cdm reference works on redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "redshift",
                        server   = Sys.getenv("CDM5_REDSHIFT_SERVER"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), cdm_tables = tbl_group("default"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "cdm_snapshot")

  # debugonce(verify_write_access)
  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"))))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "redshift")

  DBI::dbDisconnect(con)
})


test_that("DatabaseConnector cdm reference works on sql server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
  # skip("DatabaseConnector does not preserve logical datatypes")
  # skip("sql server test database cdm5.dbo.person does not have birth_datetime")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "sql server",
                        server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        user     = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))

  cdm_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]]
  expect_true(is.character(listTables(con, schema = cdm_schema)))

  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, cdm_tables =  c("cdm_source", "person", "observation_period", "vocabulary"))

  expect_error(assert_tables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  # expect_s3_class(snapshot(cdm), "cdm_snapshot")
  # df <- DBI::dbGetQuery(con, "select * from cdmv5.dbo.person")

  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"))))

  expect_true("person" %in% names(cdm))
  expect_s3_class(collect(head(cdm$person)), "data.frame")

  expect_equal(dbms(cdm), "sql server")

  DBI::dbDisconnect(con)
})

test_that("autodetect cdm version works", {
  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomia_is_available())
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  cdm <- cdm_from_con(con, cdm_tables = tbl_group("default"), cdm_version = "auto")
  expect_true(version(cdm) == c("5.3"))
  DBI::dbDisconnect(con, shutdown = TRUE)
})
