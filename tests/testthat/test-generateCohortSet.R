
test_that("cohort generation works on duckdb", {
  skip_if_not_installed("duckdb", "0.6")
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  write_schema <- "main"
  cdm_schema <- "main"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      cdm_tables = c(tbl_group("default")),
                      write_schema = write_schema)

  inst_dir <- system.file(package = "CDMConnector", mustWork = TRUE)

  # test read cohort set with a cohortsToCreate.csv
  withr::with_dir(inst_dir, {
    cohortSet <- readCohortSet("cohorts1")
  })
  expect_equal(nrow(cohortSet), 2)

  # test readCohortSet without cohortsToCreate.csv
  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 2)

  cdm <- addCohortTable(cdm, name = "cohorts", overwrite = TRUE)
  expect_true("cohorts" %in% names(cdm))

  cdm <- generateCohortSet(cdm, cohortSet, cohortTableName = "cohorts", overwrite = TRUE)
  df <- cdm$cohorts %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(names(df) == c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")))

  DBI::dbRemoveTable(con, DBI::SQL("main.cohorts"))
  expect_false("cohorts" %in% listTables(con, schema = write_schema))
  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("cohort generation works on sql server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate="yes",
                        Port     = 1433)

  write_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]]
  cdm_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]]

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      cdm_tables = c(tbl_group("default"), -visit_detail), # visit_detail is missing in sql server database
                      write_schema = write_schema)

  inst_dir <- system.file(package = "CDMConnector", mustWork = TRUE)

  # test read cohort set with a cohortsToCreate.csv
  withr::with_dir(inst_dir, {
    cohortSet <- readCohortSet("cohorts1")
  })
  expect_equal(nrow(cohortSet), 2)

  # test readCohortSet without cohortsToCreate.csv
  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 2)

  cdm <- addCohortTable(cdm, name = "cohorts", overwrite = TRUE)
  expect_true("cohorts" %in% names(cdm))

  cdm <- generateCohortSet(cdm, cohortSet, cohortTableName = "cohorts", overwrite = TRUE)
  df <- cdm$cohorts %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(names(df) == c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")))

  DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1], schema = write_schema[2], table = "cohorts"))
  expect_false("cohorts" %in% listTables(con, schema = write_schema))
  DBI::dbDisconnect(con)
})


test_that("cohort generation works on redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  write_schema <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")
  cdm_schema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")

  cdm <- cdmFromCon(con,
                    cdmSchema = cdm_schema,
                    cdmTables = tbl_group("default"),
                    writeSchema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 2)

  cdm <- addCohortTable(cdm, name = "cohorts", overwrite = TRUE)
  expect_true("cohorts" %in% names(cdm))

  cdm <- generateCohortSet(cdm, cohortSet, cohortTableName = "cohorts", overwrite = TRUE)
  df <- cdm$cohorts %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(names(df) == c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "cohorts"))
  expect_false("cohorts" %in% listTables(con, schema = write_schema))
  DBI::dbDisconnect(con)
})

test_that("cohort generation works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  skip_if_not_installed("CirceR")

  con <- dbConnect(RPostgres::Postgres(),
                   dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                   host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                   user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                   password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
  cdm_schema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

  cdm <- cdmFromCon(con,
                    cdmSchema = cdm_schema,
                    cdmTables = tbl_group("default"),
                    writeSchema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 2)

  cdm <- addCohortTable(cdm, name = "cohorts", overwrite = TRUE)
  expect_true("cohorts" %in% names(cdm))

  cdm <- generateCohortSet(cdm, cohortSet, cohortTableName = "cohorts", overwrite = TRUE)
  df <- cdm$cohorts %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(names(df) == c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "cohorts"))
  expect_false("cohorts" %in% listTables(con, schema = write_schema))
  DBI::dbDisconnect(con)
})

test_that("cohort generation works on spark", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("Only run this test manually")
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks")

  write_schema <- "omop531results"
  cdm_schema <- "omop531"

  cdm <- cdmFromCon(con,
                    cdmSchema = cdm_schema,
                    cdmTables = tbl_group("default"),
                    writeSchema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 2)

  cdm <- addCohortTable(cdm, name = "cohorts", overwrite = TRUE)
  expect_true("cohorts" %in% names(cdm))

  cdm <- generateCohortSet(cdm, cohortSet, cohortTableName = "cohorts", overwrite = TRUE)
  df <- cdm$cohorts %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(names(df) == c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "cohorts"))
  expect_false("cohorts" %in% listTables(con, schema = write_schema))
  DBI::dbDisconnect(con)
})
