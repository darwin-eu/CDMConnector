library(testthat)
library(dplyr, warn.conflicts = FALSE)

### CDM object DBI drivers ------
test_cdm_from_con <- function(con, cdm_schema, write_schema) {

  cdm <- cdmFromCon(con, cdmSchema = cdm_schema)

  expect_s3_class(cdm, "cdm_reference")
  expect_error(assertTables(cdm, "person"), NA)
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)
  expect_s3_class(cdm, "cdm_reference")
  expect_error(assertWriteSchema(cdm), NA)
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), dbms(attr(cdm, "dbcon")))

  # simple join
  df <- dplyr::inner_join(cdm$person,
                          cdm$observation_period,
                          by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")
}

# test_that("local postgres cdmFromCon", {
#   skip("manual test")
#   skip_if(get_cdm_schema("local") == "")
#   con <- get_connection("local")
#   cdm_schema <- get_cdm_schema("local")
#   write_schema <- get_write_schema("local")
#   test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
#   disconnect(con)
# })



test_that("postgres cdmFromCon", {
  dbms <- "postgres"
  skip_if(get_cdm_schema(dbms) == "")
  con <- get_connection(dbms)
  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms)
  test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  disconnect(con)
})

test_that("sqlserver cdmFromCon", {
  dbms <- "sqlserver"
  skip_if(get_cdm_schema(dbms) == "")
  con <- get_connection(dbms)
  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms)
  test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  disconnect(con)
})

test_that("redshift cdmFromCon", {
  dbms <- "redshift"
  # skip("failing test")
  skip_if(get_cdm_schema(dbms) == "")
  con <- get_connection(dbms)
  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms)
  test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  disconnect(con)
})

test_that("oracle cdmFromCon", {
  dbms <- "oracle"
  skip("failing test")
  skip_if(get_cdm_schema(dbms) == "")
  con <- get_connection(dbms)
  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms)
  test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  disconnect(con)
})

test_that("bigquery cdmFromCon", {
  dbms <- "bigquery"
  skip("failing test")
  skip_if(get_cdm_schema(dbms) == "")
  con <- get_connection(dbms)
  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms)
  test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  disconnect(con)
})

test_that("snowflake cdmFromCon", {
  dbms <- "snowflake"
  skip_if(get_cdm_schema(dbms) == "")
  con <- get_connection(dbms)
  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms)
  test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  disconnect(con)
})

test_that("spark cdm_reference", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("manual test")

  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks", bigint = "numeric")

  expect_true(is.character(listTables(con, schema = "omop531")))

  cdm <- cdmFromCon(con, cdmSchema = "omop531")

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  cdm$cdm_source

  expect_true(is.null(verify_write_access(con, write_schema = "omop531results")))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "spark")

  df <- dplyr::inner_join(cdm$person,
                          cdm$observation_period,
                          by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("oracle cdm_reference", {

  skip_on_ci()
  skip_on_cran()
  skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)

  skip("failing test")

  # library(ROracle)
  # con <- DBI::dbConnect(DBI::dbDriver("Oracle"),
  #                       username = Sys.getenv("CDM5_ORACLE_USER"),
  #                       password= Sys.getenv("CDM5_ORACLE_PASSWORD"),
  #                       dbname = Sys.getenv("CDM5_ORACLE_SERVER"))

  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")

  # allTables <- DBI::dbListTables(con, schema = "CDMV5", full = TRUE)
  writeSchema <- "OHDSI"
  cdmSchema <- "CDMV5"

  # List schemas
  # dbGetQuery(con, "select username as schema from sys.all_users")

  expect_true(is.character(listTables(con, schema = cdmSchema)))

  # Oracle test cdm v5.3 is missing visit_detail
  cdm <- cdmFromCon(con, cdmSchema = cdmSchema)

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  # expect_s3_class(snapshot(cdm), "data.frame") # test database person table is missing birth_datetime

  expect_true(is.null(verify_write_access(con, write_schema = writeSchema)))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "oracle")

  df <- dplyr::inner_join(cdm$person,
                          cdm$observation_period,
                          by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("cdm reference works on bigquery", {
 # need to get tbl(., in_schema) working
  skip("failing test")
  bigrquery::bq_auth(path = Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH"))

  cdm_schema <- Sys.getenv("BIGQUERY_CDM_SCHEMA")
  write_schema <- Sys.getenv("BIGQUERY_SCRATCH_SCHEMA")

  con <- DBI::dbConnect(
    bigrquery::bigquery(),
    project = Sys.getenv("BIGQUERY_PROJECT_ID"),
    dataset = cdm_schema
  )

  expect_true(is.character(listTables(con, schema = cdm_schema)))
  expect_true(is.character(listTables(con, schema = write_schema)))

  cdm <- cdmFromCon(con, cdmSchema = cdm_schema)

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")

  expect_true(is.null(verify_write_access(con, write_schema = "scratch")))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "postgresql")


  DBI::dbDisconnect(con)
})


test_that("duckdb inclusion of cohort tables", {
  skip_if_not(rlang::is_installed("duckdb"))
  skip_if_not(eunomiaIsAvailable())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

  cohort <- dplyr::tibble(cohort_definition_id = 1L,
                          subject_id = 1L:2L,
                          cohort_start_date = c(Sys.Date(), as.Date("2020-02-03")),
                          cohort_end_date = c(Sys.Date(), as.Date("2020-11-04")))

  DBI::dbExecute(con, "create schema write_schema;")

  expect_null(verify_write_access(con, "write_schema"))

  DBI::dbWriteTable(con, DBI::Id(schema = "write_schema", table_name = "cohort"), cohort)

  expect_equal(
    listTables(con, schema = "write_schema"),
    # c("cohort", "cohort_attrition", "cohort_codelist", "cohort_set"))
    "cohort")

  cdm <- cdmFromCon(con,
                    cdmSchema = "main",
                    writeSchema = "write_schema",
                    cohortTables = "cohort") %>%
    cdmSelectTbl("person", "observation_period", "cohort")

  expect_output(print(cdm), "CDM")
  expect_equal(collect(cdm$cohort), cohort)
  DBI::dbDisconnect(con, shutdown = TRUE)

})

test_that("duckdb collect a cdm", {
  skip_if_not(rlang::is_installed("duckdb"))
  skip_if_not(eunomiaIsAvailable())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
  cdm <- cdmFromCon(con, cdmSchema = "main")

  local_cdm <- collect(cdm)

  expect_s3_class(local_cdm, "cdm_reference")
  local_person <- local_cdm$person
  attr(local_person, "cdm_reference") <- NULL # remove the attribute
  expect_equal(local_person, collect(cdm$person))

  query <- function(con) DBI::dbGetQuery(con, "select count(*) as n from person")

  expect_equal(query(con), query(dbplyr::remote_con(cdm$person)))
  expect_equal(dbms(con), "duckdb")
  expect_equal(dbms(cdm), "duckdb")

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("duckdb stow and cdmFromFiles works", {
  skip_if_not(rlang::is_installed("duckdb"))
  skip_if_not(eunomiaIsAvailable())

  save_path <- file.path(tempdir(), paste0("tmp_", paste(sample(letters, 10, replace = TRUE), collapse = "")))
  dir.create(save_path)
  cdm_tables <- c("person", "observation_period", "cdm_source", "vocabulary")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

  # Test tidyselect in cdmFromCon. Should not produce message about ambiguous names.
  expect_message(cdmFromCon(con, "main") %>% cdmSelectTbl(tblGroup("vocab")), NA)
  expect_message(cdmFromCon(con, "main") %>% cdmSelectTbl(matches("person|observation_period")), NA)
  expect_message(cdmFromCon(con, "main") %>% cdmSelectTbl(c(person, observation_period)), NA)
  expect_message(cdmFromCon(con, "main") %>% cdmSelectTbl(c("person", "observation_period")), NA)
  expect_message(cdmFromCon(con, "main") %>% cdmSelectTbl(all_of(cdm_tables)), NA)

  cdm <- cdmFromCon(con, "main") %>% cdmSelectTbl(all_of(cdm_tables))

  stow(cdm, path = save_path, format = "parquet")
  stow(cdm, path = save_path, format = "csv")
  stow(cdm, path = save_path, format = "feather")

  expect_setequal(list.files(save_path, pattern = "*.parquet"), paste0(cdm_tables, ".parquet"))
  expect_setequal(list.files(save_path, pattern = "*csv"), paste0(cdm_tables, ".csv"))
  expect_setequal(list.files(save_path, pattern = "*feather"), paste0(cdm_tables, ".feather"))
  unlink(list.files(save_path, pattern = "*csv", full.names = T))
  unlink(list.files(save_path, pattern = "*feather", full.names = T))

  expect_message(cdmFromFiles(save_path), NA)

  local_cdm <- cdmFromFiles(save_path)
  expect_s3_class(local_cdm, "cdm_reference")
  local_person <- local_cdm$person
  attr(local_person, "cdm_reference") <- NULL
  expect_equal(local_person, collect(cdm$person))
  expect_s3_class(snapshot(cdm), "data.frame")

  local_arrow_cdm <- cdmFromFiles(save_path, asDataFrame = FALSE)
  expect_s3_class(local_arrow_cdm, "cdm_reference")
  expect_equal(collect(local_arrow_cdm$person), collect(cdm$person))
  expect_error(validateCdm(local_arrow_cdm))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

## CDM Object DatabaseConnector driver -----

library(testthat)
library(dplyr, warn.conflicts = FALSE)

test_that("DatabaseConnector cdm reference works on local postgres", {
  skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")
  skip("manual test")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "postgresql",
                        server   = Sys.getenv("LOCAL_POSTGRESQL_SERVER"),
                        user     = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                        password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdmFromCon(con, cdmSchema = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"))

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")

  expect_true(is.null(verify_write_access(con, write_schema = "scratch")))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "postgresql")

  DBI::dbDisconnect(con)
})

test_that("DatabaseConnector cdm reference works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  skip("manual test")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "postgresql",
                        server   = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                        user     = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdmFromCon(con, cdmSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")

  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"))))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "postgresql")

  DBI::dbDisconnect(con)
})


test_that("DatabaseConnector cdm reference works on redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
  skip("manual test")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "redshift",
                        server   = Sys.getenv("CDM5_REDSHIFT_SERVER"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  expect_true(is.character(listTables(con, schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))))

  cdm <- cdmFromCon(con, cdmSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")

  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"))))

  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  expect_equal(dbms(cdm), "redshift")

  DBI::dbDisconnect(con)
})


test_that("DatabaseConnector cdm reference works on sql server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
  skip("manual test")
  # Note that DatabaseConnector does not preserve logical datatypes
  # Note sql server test database cdm5.dbo.person does not have birth_datetime

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms     = "sql server",
                        server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        user     = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))

  cdm_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]]
  expect_true(is.character(listTables(con, schema = cdm_schema)))

  cdm <- cdmFromCon(con, cdmSchema = cdm_schema) %>%
    cdmSelectTbl("cdm_source", "person", "observation_period", "vocabulary")

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  # expect_s3_class(snapshot(cdm), "data.frame")
  # df <- DBI::dbGetQuery(con, "select * from cdmv5.dbo.person")

  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"))))

  expect_true("person" %in% names(cdm))
  expect_s3_class(collect(head(cdm$person)), "data.frame")

  expect_equal(dbms(cdm), "sql server")

  DBI::dbDisconnect(con)
})

test_that("DatabaseConnector cdm reference works on snowflake", {
  skip_if(Sys.getenv("SNOWFLAKE_USER") == "")
  skip("failing test")
  skip("manual test")

  cdm_schema <- Sys.getenv("SNOWFLAKE_CDM_SCHEMA")
  write_schema <- Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA")

  con <- DBI::dbConnect(DatabaseConnector::DatabaseConnectorDriver(),
                        dbms = "snowflake",
                        connectionString = Sys.getenv("SNOWFLAKE_CONNECTION_STRING"),
                        user = Sys.getenv("SNOWFLAKE_USER"),
                        password = Sys.getenv("SNOWFLAKE_PASSWORD"))

  cdm_schema <- strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]]
  expect_true(is.character(listTables(con, schema = cdm_schema)))

  # error here
  cdm <- cdmFromCon(con, cdmSchema = cdm_schema) %>%
    cdmSelectTbl("cdm_source", "person", "observation_period", "vocabulary")

  expect_error(assertTables(cdm, "cost"))
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  # expect_s3_class(snapshot(cdm), "data.frame")
  # df <- DBI::dbGetQuery(con, "select * from cdmv5.dbo.person")

  expect_true(is.null(verify_write_access(con, write_schema = Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"))))

  expect_true("person" %in% names(cdm))
  expect_s3_class(collect(head(cdm$person)), "data.frame")

  expect_equal(dbms(cdm), "sql server")

  DBI::dbDisconnect(con)
})

# CDM utility functions -----
test_that("cdmName works", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main")
  expect_equal(cdmName(cdm), "Synthea synthetic health database")

  cdm <- cdmFromCon(con, "main", cdm_name = "Example CDM")
  expect_equal(cdmName(cdm), "Example CDM")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("autodetect cdm version works", {
  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomiaIsAvailable())
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
  cdm <- cdmFromCon(con, cdmSchema = "main", cdmVersion = "auto")
  expect_true(version(cdm) == c("5.3"))
  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("snapshot works when cdm_source or vocabulary tables are empty", {
  skip_if_not(eunomiaIsAvailable())
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
  cdm <- cdmFromCon(con, "main")
  expect_s3_class(snapshot(cdm), "data.frame")

  DBI::dbExecute(con, "delete from main.cdm_source")

  expect_s3_class(snapshot(cdm), "data.frame")

  DBI::dbExecute(con, "delete from main.vocabulary")
  expect_s3_class(snapshot(cdm), "data.frame")
  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("stow works", {
  skip_if_not(eunomiaIsAvailable())
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
  cdm <- cdmFromCon(con, "main")
  names1 <- names(cdm)
  dOut <- tempfile()
  dir.create(dOut)
  file.exists(dOut)
  CDMConnector::stow(cdm, dOut, format = "duckdb")
  DBI::dbDisconnect(con, shutdown = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file.path(dOut, "cdm.duckdb"))
  cdm <- cdmFromCon(con, cdmSchema = "main")
  names2 <- names(cdm)
  expect_equal(names1, names2)
  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("cdm_select_tbl works", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

  cdm <- cdmFromCon(con, "main")

  expect_equal(names(cdmSelectTbl(cdm, person)), "person")
  expect_equal(names(cdmSelectTbl(cdm, person, observation_period)), c("person", "observation_period"))
  expect_equal(names(cdmSelectTbl(cdm, tblGroup("vocab"))), tblGroup("vocab"))
  expect_equal(names(cdmSelectTbl(cdm, "person")), "person")
  expect_error(names(cdmSelectTbl(cdm, "blah")))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("subsetting cdm tables", {
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con)

  # $ always returns a `tbl`
  expect_s3_class(attr(cdm$person, "cdm_reference"), "cdm_reference")

  # double bracket always returns a `tbl`
  expect_s3_class(attr(cdm[["person"]], "cdm_reference"), "cdm_reference")
  expect_s3_class(attr(cdm[[1]], "cdm_reference"), "cdm_reference")

  # single bracket always returns a cdm
  expect_s3_class(cdm[1], "cdm_reference")
  expect_s3_class(cdm[1:3], "cdm_reference")
  expect_s3_class(cdm["person"], "cdm_reference")
  expect_s3_class(cdm[c("person", "concept")], "cdm_reference")


  # dplyr verbs retain cdm_attribute
  cdm2 <- cdm$person %>%
    dplyr::mutate(a = 1) %>%
    dplyr::filter(.data$person_id < 100) %>%
    dplyr::left_join(cdm$death, by = "person_id") %>%
    dplyr::arrange(.data$person_id) %>%
    attr("cdm_reference")

  expect_s3_class(cdm2, "cdm_reference")

  # cdm_reference is not retained after collect
  expect_null(attr(collect(cdm$person), "cdm_reference"))

  # cdm_reference attribute is retained after computeQuery
  cdm2 <- cdm$person %>%
    dplyr::mutate(a = 1) %>%
    dplyr::filter(.data$person_id < 100) %>%
    dplyr::left_join(cdm$death, by = "person_id") %>%
    dplyr::arrange(.data$person_id) %>%
    computeQuery() %>%
    attr("cdm_reference")

  expect_s3_class(cdm2, "cdm_reference")

  DBI::dbDisconnect(con, shutdown = TRUE)
})








