
test_that("duckdb cohort generation", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

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
  expect_equal(nrow(cohortSet), 3)
  expect_s3_class(cohortSet, "CohortSet")

  # debugonce(generateCohortSet)
  cdm <- generateCohortSet(cdm, cohortSet, name = "chrt0", computeAttrition = FALSE)
  # check already exists
  expect_error(generateCohortSet(cdm, cohortSet, name = "chrt0", overwrite = FALSE))

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  # expect_s3_class(dplyr::collect(attrition(cdm$chrt0)), "data.frame")
  expect_null(cohortAttrition(cdm$chrt0))
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), 3)

  # cohort table should be lowercase
  expect_error(generateCohortSet(cdm, cohortSet, name = "MYcohorts", overwrite = TRUE))

  # drop tables
  DBI::dbRemoveTable(con, DBI::Id(schema = "main", table = "chrt0"))
  expect_false("chrt0" %in% listTables(con, schema = write_schema))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_count"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_set"))

  # empty data
  expect_error(generateCohortSet(cdm, cohortSet %>% head(0), name = "cohorts", overwrite = TRUE))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("duckdb cohort generation with attrition", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  write_schema <- "main"
  cdm_schema <- "main"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      cdm_tables = c(tbl_group("default")),
                      write_schema = write_schema)

  inst_dir <- system.file(package = "CDMConnector", mustWork = TRUE)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  expect_s3_class(cohortSet, "CohortSet")

  # test readCohortSet with non existing directory
  expect_error(readCohortSet(system.file("cohorts99", package = "CDMConnector", mustWork = FALSE)))

  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           computeAttrition = TRUE,
                           overwrite = TRUE)

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  attrition_df <- dplyr::collect(cohortAttrition(cdm$chrt0))
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), 3)

  # cdm_from_con should find existing tables
  cdm2 <- cdm_from_con(con, "main", write_schema = "main", cohort_tables = "chrt0")

  expect_s3_class(cdm2$chrt0, "GeneratedCohortSet")
  expect_s3_class(cohortAttrition(cdm2$chrt0), "tbl_dbi")
  expect_s3_class(cohortCount(cdm2$chrt0), "tbl_dbi")
  expect_s3_class(cohortSet(cdm2$chrt0), "tbl_dbi")

  # try overwrite=TRUE
  cdm2 <- generateCohortSet(cdm2,
                            cohortSet,
                            name = "chrt0",
                            computeAttrition = TRUE,
                            overwrite = TRUE)

  expect_s3_class(cdm2$chrt0, "GeneratedCohortSet")
  expect_s3_class(cohortAttrition(cdm2$chrt0), "tbl_dbi")
  expect_s3_class(cohortCount(cdm2$chrt0), "tbl_dbi")
  expect_s3_class(cohortSet(cdm2$chrt0), "tbl_dbi")

  # drop tables
  dropTable(cdm, name = dplyr::starts_with("chrt0"))
  expect_false(any(stringr::str_detect(listTables(con, schema = write_schema), "^chrt0")))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("SQL Server cohort generation with attrition", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")
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
                      cdm_tables = c(tbl_group("default"), -visit_detail),
                      write_schema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  cohortSet <- cohortSet[3,]
  expect_s3_class(cohortSet, "CohortSet")

  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           computeAttrition = TRUE,
                           overwrite = TRUE)

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  attrition_df <- dplyr::collect(cohortAttrition(cdm$chrt0))
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), 1)

  # drop tables
  DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1], schema = write_schema[2], table = "chrt0"))
  expect_false("chrt0" %in% listTables(con, schema = write_schema))

  DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1], schema = write_schema[2], table = "chrt0_count"))
  DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1], schema = write_schema[2], table = "chrt0_set"))
  DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1], schema = write_schema[2], table = "chrt0_attrition"))

  DBI::dbDisconnect(con)
})


test_that("Redshift cohort generation with attrition", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")
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

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      cdm_tables = c(tbl_group("default")),
                      write_schema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  cohortSet <- cohortSet[3,]
  expect_s3_class(cohortSet, "CohortSet")

  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           computeAttrition = TRUE,
                           overwrite = TRUE)

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  attrition_df <- dplyr::collect(cohortAttrition(cdm$chrt0))
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), nrow(cohortSet))

  # drop tables
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0"))
  expect_false("chrt0" %in% listTables(con, schema = write_schema))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_count"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_set"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_attrition"))

  DBI::dbDisconnect(con)
})

test_that("Postgres cohort generation with attrition", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  skip_if_not_installed("CirceR")

  con <- dbConnect(RPostgres::Postgres(),
                   dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                   host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                   user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                   password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
  cdm_schema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  cohortSet <- cohortSet[3,]
  expect_s3_class(cohortSet, "CohortSet")

  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           computeAttrition = TRUE,
                           overwrite = TRUE)

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  attrition_df <- dplyr::collect(cohortAttrition(cdm$chrt0))
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), nrow(cohortSet))

  # drop tables
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0"))
  expect_false("chrt0" %in% listTables(con, schema = write_schema))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_count"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_set"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_attrition"))

  DBI::dbDisconnect(con)
})


test_that("cohort generation works on spark", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("manual test")
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks")

  write_schema <- "omop531results"
  cdm_schema <- "omop531"

  cdm <- cdmFromCon(con,
                    cdmSchema = cdm_schema,
                    cdmTables = tbl_group("default"),
                    writeSchema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  cohortSet <- cohortSet[3,]
  expect_s3_class(cohortSet, "CohortSet")
  # debugonce(generateCohortSet)
  options(sqlRenderTempEmulationSchema = write_schema)
  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           computeAttrition = TRUE,
                           overwrite = TRUE)

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  attrition_df <- dplyr::collect(cohortAttrition(cdm$chrt0))
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), nrow(cohortSet))

  # drop tables
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0"))
  expect_false("chrt0" %in% listTables(con, schema = write_schema))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_count"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_set"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_attrition"))

  DBI::dbDisconnect(con)
})


# Test readCohortSet ----

test_that("ReadCohortSet gives informative error when pointed to a file", {
  path <- system.file("cohorts1", "deepVeinThrombosis01.json", package = "CDMConnector", mustWork = TRUE)
  expect_error(readCohortSet(path), "not a directory")
})

test_that("Generation from Capr Cohorts", {
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("Capr")
  library(Capr)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  cdm <- CDMConnector::cdm_from_con(
    con = con,
    cdm_schema = "main",
    write_schema = "main"
  )

  gibleed_cohort_definition <- cohort(
    entry = condition(cs(descendants(192671))),
    attrition = attrition(
      "no RA" = withAll(
        exactly(0,
                condition(cs(descendants(80809))),
                duringInterval(eventStarts(-Inf, Inf))))
    )
  )

  cdm <- generateCohortSet(
    cdm,
    list(gibleed = gibleed_cohort_definition),
    name = "gibleed",
    computeAttrition = TRUE,
    overwrite = TRUE
  )

  expect_gt(nrow(dplyr::collect(cdm$gibleed)), 10)
  DBI::dbDisconnect(con, shutdown = TRUE)
})
