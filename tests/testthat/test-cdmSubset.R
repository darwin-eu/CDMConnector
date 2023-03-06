library(dplyr)

test_that("duckdb subsetting", {
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  cdm <- cdm_from_con(con, "main", write_schema = "main")

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  expect_true(nrow(cohortSet) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")

  class(cdm$gibleed)
  cdm4 <- cdmSubsetCohort(cdm, "gibleed")

  expect_lt(
    length(dplyr::pull(cdm4$person, "person_id")),
    length(dplyr::pull(cdm$person,  "person_id"))
  )

  expect_lt(
    cdm4$condition_occurrence %>% distinct(.data$person_id) %>% tally() %>% pull(.data$n),
    cdm$condition_occurrence  %>% distinct(.data$person_id) %>% tally() %>% pull(.data$n)
  )

  df <- cdmFlatten(cdm4) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("local postgres subsetting", {
  skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")

  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                        user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                        password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  cdm_schema <- Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA")
  write_schema <- "scratch"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  expect_true(nrow(cohortSet) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed", overwrite = TRUE)
  expect_s3_class(cdm$gibleed, "GeneratedCohortSet")

  cdm4 <- cdmSubsetCohort(cdm, "gibleed")

  expect_lt(
    length(dplyr::pull(cdm4$person, "person_id")),
    length(dplyr::pull(cdm$person,  "person_id"))
  )

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "gibleed"))
  expect_false("gibleed" %in% tolower(listTables(con, write_schema)))
  DBI::dbDisconnect(con)
})

test_that("postgres subsetting", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  cdm_schema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  expect_true(nrow(cohortSet) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
  expect_s3_class(cdm$gibleed, "GeneratedCohortSet")

  cdm4 <- cdmSubsetCohort(cdm, "gibleed")

  expect_lt(
    length(dplyr::pull(cdm4$person, "person_id")),
    length(dplyr::pull(cdm$person,  "person_id"))
  )

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "gibleed"))
  expect_false("gibleed" %in% tolower(listTables(con, write_schema)))

  DBI::dbDisconnect(con)
})


test_that("sql server subsetting", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = "ODBC Driver 18 for SQL Server",
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate = "yes",
                        Port     = 1433)

  cdm_schema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
  write_schema <- Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA")

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  expect_true(nrow(cohortSet) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
  expect_s3_class(cdm$gibleed, "GeneratedCohortSet")

  cdm4 <- cdmSubsetCohort(cdm, "gibleed")

  expect_lt(
    length(dplyr::pull(cdm4$person, "person_id")),
    length(dplyr::pull(cdm$person,  "person_id"))
  )

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1],
                                  schema = write_schema[2],
                                  table = "gibleed"))
  expect_false("gibleed" %in% tolower(listTables(con, write_schema)))

  DBI::dbDisconnect(con)
})


test_that("sql server subsetting", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
  skip("birth_datetime column not found in cdm table person")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = "ODBC Driver 18 for SQL Server",
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate = "yes",
                        Port     = 1433)


  cdm_schema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA") %>%
    stringr::str_split("\\.") %>%
    {.[[1]]}

  write_schema <- Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA") %>%
    stringr::str_split("\\.") %>%
    {.[[1]]}

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  # path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  # cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  # expect_true(nrow(cohortSet) == 1)
  # cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
  # expect_s3_class(cdm$gibleed, "GeneratedCohortSet")
  # cdm4 <- cdmSubsetCohort(cdm, "gibleed")
  # expect_lt(
  #   length(dplyr::pull(cdm4$person, "person_id")),
  #   length(dplyr::pull(cdm$person,  "person_id"))
  # )
  # DBI::dbRemoveTable(con, DBI::Id(catalog = write_schema[1],
  #                                 schema = write_schema[2],
  #                                 table = "gibleed"))
  # expect_false("gibleed" %in% tolower(listTables(con, write_schema)))

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})


test_that("redshift subsetting", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))


  cdm_schema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")

  write_schema <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  # path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  # cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  # expect_true(nrow(cohortSet) == 1)
  # cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
  # expect_s3_class(cdm$gibleed, "GeneratedCohortSet")
  # cdm4 <- cdmSubsetCohort(cdm, "gibleed")
  # expect_lt(
  #   length(dplyr::pull(cdm4$person, "person_id")),
  #   length(dplyr::pull(cdm$person,  "person_id"))
  # )
  # DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "gibleed"))
  # expect_false("gibleed" %in% tolower(listTables(con, write_schema)))

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})


test_that("oracle subsetting", {
  skip_on_ci()
  skip_on_cran()
  skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
  skip("birth_datetime column not found in cdm table person")

  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")

  cdm_schema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  write_schema <- "OHDSI"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  # path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  # cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  # expect_true(nrow(cohortSet) == 1)
  # cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
  # expect_s3_class(cdm$gibleed, "GeneratedCohortSet")
  # cdm4 <- cdmSubsetCohort(cdm, "gibleed")
  # expect_lt(
  #   length(dplyr::pull(cdm4$person, "person_id")),
  #   length(dplyr::pull(cdm$person,  "person_id"))
  # )
  # DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "gibleed"))
  # expect_false("gibleed" %in% tolower(listTables(con, write_schema)))

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})


test_that("spark subsetting", {
  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("manual test")

  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks", bigint = "numeric")

  cdm_schema <- "omop531"
  write_schema <- "omop531results"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  cdm2 <- cdmSample(cdm, n = 10)

  expect_equal(nrow(collect(cdm2$person)), 10)

  personId2 <- cdm2$person %>% pull(person_id)

  cdm3 <- cdmSubset(cdm, personId = personId2)

  expect_equal(nrow(collect(cdm3$person)), 10)

  personId3 <- cdm3$person %>% pull(person_id)

  expect_setequal(personId2, personId3)

  # path <- system.file("cohorts2", mustWork = TRUE, package = "CDMConnector")
  # cohortSet <- readCohortSet(path) %>% filter(cohort_name == "GIBleed_male")
  # expect_true(nrow(cohortSet) == 1)
  # cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "gibleed")
  # expect_s3_class(cdm$gibleed, "GeneratedCohortSet")
  # cdm4 <- cdmSubsetCohort(cdm, "gibleed")
  # expect_lt(
  #   length(dplyr::pull(cdm4$person, "person_id")),
  #   length(dplyr::pull(cdm$person,  "person_id"))
  # )
  # DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "gibleed"))
  # expect_false("gibleed" %in% tolower(listTables(con, write_schema)))

  df <- cdmFlatten(cdm3) %>% dplyr::collect()
  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})
