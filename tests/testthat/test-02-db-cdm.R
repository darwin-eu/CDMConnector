library(testthat)
library(dplyr, warn.conflicts = FALSE)

### CDM object DBI drivers ------
test_cdmFromCon <- function(con, cdmSchema, writeSchema) {
  # expect_error(cdmFromCon(con, cdmSchema = cdmSchema, cdmName = "test"), "write_schema") # write schema is no longer required by CDMConnector but is required by omopgenerics.
  cdm <- cdmFromCon(con, cdmSchema = cdmSchema, cdmName = "test", writeSchema = writeSchema)
  expect_s3_class(cdm, "cdm_reference")
  expect_warning(version(cdm))
  expect_true(cdmVersion(cdm) %in% c("5.3", "5.4"))

  cdmSnapshot <- snapshot(cdm)
  # cdmSnapshot <- snapshot(cdm, computeDataHash = (dbms(con) == "duckdb")) # requires DatabaseConnector 7 to be released first
  expect_s3_class(cdmSnapshot, "data.frame")
  # if (dbms(con) == "duckdb") {
  #   expect_true(nchar(cdmSnapshot$cdm_data_hash) > 1)
  # }

  expect_true("concept" %in% names(cdm))
  expect_s3_class(dplyr::collect(head(cdm$concept)), "data.frame")

  cdm <- cdmFromCon(con, cdmSchema = cdmSchema, writeSchema = writeSchema)
  expect_s3_class(cdm, "cdm_reference")
  # expect_error(assertWriteSchema(cdm), NA)
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")
  expect_equal(dbms(cdm), dbms(attr(cdm, "dbcon")))

  # check cdm_reference attribute
  expect_true("cdm_reference" %in% names(attributes(cdm[["person"]])))
  x <- unclass(cdm)
  expect_false("cdm_reference" %in% names(attributes(x[["person"]])))
  x[["person"]] <- cdm[["person"]] %>% compute()
  expect_true("cdm_reference" %in% names(attributes(x[["person"]])))
  cdm[["person"]] <- cdm[["person"]] %>% compute()
  x <- unclass(cdm)
  expect_false("cdm_reference" %in% names(attributes(x[["person"]])))

  # simple join
  df <- dplyr::inner_join(cdm$person, cdm$observation_period, by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")
}
dbtype="snowflake"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - cdm_from_con"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdmSchema <- get_cdm_schema(dbtype)
    writeSchema <- get_write_schema(dbtype)
    skip_if(any(writeSchema == "") || any(cdmSchema == "") || is.null(con))
    test_cdmFromCon(con, cdmSchema = cdmSchema, writeSchema = writeSchema)
    disconnect(con)
  })
}

test_that("Uppercase tables are stored as lowercase in cdm", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())
  # create a test cdm with upppercase table names
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))

  for (name in listTables(con, "main")) {
    DBI::dbExecute(con,
                   glue::glue("ALTER TABLE {name} RENAME TO {name}2;"))
    DBI::dbExecute(con,
                   glue::glue("ALTER TABLE {name}2 RENAME TO {toupper(name)};"))

  }

  expect_true(all(listTables(con, "main") == toupper(listTables(con, "main"))))

  # check that names in cdm are lowercase
  cdm <- cdmFromCon(con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main")
  expect_true(all(names(cdm) == tolower(names(cdm))))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

# TODO add this test back when we have an example cdm with achilles tables
# test_that("adding achilles", {
#   skip_if_not(eunomiaIsAvailable())
#   skip_if_not_installed("duckdb")
#
#   con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
#
#   expect_error(cdmFromCon(con = con,
#                           cdmSchema =  "main",
#                           writeSchema = "main",
#                           achillesSchema = "main"))
#
#   DBI::dbWriteTable(
#     conn = con,
#     name = "achilles_analysis",
#     value = tibble(
#       analysis_id = 1L, analysis_name = "1", stratum_1_name = "1",
#       stratum_2_name = "1", stratum_3_name = "1", stratum_4_name = "1",
#       stratum_5_name = "1", is_default = 1L, category = "1"),
#     overwrite = TRUE
#   )
#
#   DBI::dbWriteTable(
#     conn = con,
#     name = "achilles_results",
#     value = tibble(analysis_id = 1L,
#                    stratum_1 = "a",
#                    stratum_2 = "b",
#                    stratum_3 = "1",
#                    stratum_4 = "5",
#                    stratum_5 = "u",
#                    count_value = 1500L),
#     overwrite = TRUE
#   )
#
#   DBI::dbWriteTable(
#     conn = con,
#     name = "achilles_results_dist",
#     value = tibble(
#       analysis_id = 1L, count_value = 5L, stratum_1 = "1", stratum_2 = "1",
#       stratum_3 = "1", stratum_4 = "1", stratum_5 = "1", min_value = 1, max_value = 1,
#       avg_value = 1, stdev_value = 1, median_value = 1, p10_value = 1,
#       p25_value = 1, p75_value = 1, p90_value = 1),
#     overwrite = TRUE
#   )
#
#   cdm <- cdmFromCon(con = con,
#                      cdmSchema =  "main",
#                      writeSchema = "main",
#                      achillesSchema = "main")
#
#  expect_true(cdm$achilles_analysis %>% dplyr::pull("analysis_name") == 1)
#  expect_true(cdm$achilles_results %>% dplyr::pull("stratum_1") == "a")
#  expect_true(cdm$achilles_results_dist %>% dplyr::pull("count_value") == 5)
#
#  # we should also be able to add achilles tables manually if in db
#  cdm <- cdmFromCon(
#    con = con, cdmName = "eunomia", cdmSchema =  "main", writeSchema = "main"
#  )
#  cdm$achilles_analysis <- dplyr::tbl(con, "achilles_analysis")
#  # but should not work if tables are not in db (as cdm is db side)
#  expect_error(
#    cdm$achilles_analysis <- dplyr::tibble(
#      analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
#      stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
#      stratum_5_name = 1, is_default = 1, category = 1
#    )
#   )
# # if local tables, insert table would take care of this
#
#  achilles_analysis_tibble <- dplyr::tibble(
#    analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
#    stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
#    stratum_5_name = 1, is_default = 1, category = 1
#  )
#  cdm <- omopgenerics::insertTable(cdm = cdm,
#                                   table = achilles_analysis_tibble,
#                                   name = "achilles_analysis",
#                                   overwrite = TRUE)
#
#  cdm$achilles_analysis <- dplyr::tbl(con, "achilles_analysis")
#
#  # but should not work if tables are not in db (as cdm is db side)
#   expect_error(
#     cdm$achilles_analysis <- dplyr::tibble(
#       analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
#       stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
#       stratum_5_name = 1, is_default = 1, category = 1
#   ))
#
#  # if local tables, insert table would take care of this
#   achilles_analysis_tibble <- dplyr::tibble(
#     analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
#     stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
#     stratum_5_name = 1, is_default = 1, category = 1
#   )
#
#   cdm <- omopgenerics::insertTable(cdm = cdm,
#                                     table = achilles_analysis_tibble,
#                                     name = "achilles_analysis",
#                                     overwrite = TRUE)
#
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })

test_that("adding cohort tables to the cdm", {
  skip_if_not(eunomiaIsAvailable())
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())

  cohorts <- data.frame(
    cohortId = c(1, 2, 3),
    cohortName = c("X", "A", "B"),
    type = c("target", "event", "event")
  )

  cohort_table <- dplyr::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date,    ~cohort_end_date,
    1L,                     5L,           as.Date("2020-01-01"), as.Date("2020-01-01"),
    2L,                     5L,           as.Date("2020-01-10"), as.Date("2020-03-10")
  )

  dplyr::copy_to(dest = con,
                 df = cohort_table,
                 name = "test_cohort_table",
                 overwrite = TRUE)

  expect_equal(unname(sapply(DBI::dbReadTable(con, "test_cohort_table"), class)),
               c("integer", "integer", "Date", "Date"))

  expect_error(cdmFromCon(con,
                    cdmSchema = "main",
                    writeSchema = c(schema = "main"),
                    cohortTables = "test_cohort_table",
                    .softValidation = FALSE)) # error because cohorts out of obs

  expect_no_error(cdmFromCon(con,
                             cdmSchema = "main",
                             writeSchema = c(schema = "main"),
                             cohortTables = "test_cohort_table",
                             .softValidation = TRUE)) # passes without validation

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("writeSchema argument specification and cdmDisconnect works", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main", "main", writePrefix = "tmp_")

  expect_equal(attr(cdm, "write_schema"), c(schema = "main", prefix = "tmp_"))

  cdmDisconnect(cdm)
})

test_that("schema specification with . works", {
  skip("manual test")
  con <- DBI::dbConnect(odbc::odbc(),
                        SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                        UID = Sys.getenv("SNOWFLAKE_USER"),
                        PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                        DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                        WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                        DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))

  cdm_schema <- Sys.getenv("SNOWFLAKE_CDM_SCHEMA")
  write_schema <- Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA")

  cdm <- cdmFromCon(con, cdm_schema, write_schema, writePrefix = "tmp_", cdmName = "test")

  write_schema_split <- stringr::str_split(write_schema, "\\.")[[1]] %>%
    purrr::set_names("catalog", "schema") %>%
    c(., prefix = "tmp_")

  cdm_schema_split <- stringr::str_split(cdm_schema, "\\.")[[1]] %>%
    purrr::set_names("catalog", "schema")

  expect_equal(attr(cdm, "write_schema"), write_schema_split)
  expect_equal(attr(cdm, "cdm_schema"), cdm_schema_split)

  DBI::dbDisconnect(con)

})


test_that("DatabaseConnector DBI connections work with duckdb", {

  testthat::skip_if_not_installed("DatabaseConnector")
  testthat::skip_if_not_installed("duckdb")

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    "duckdb",
    server = CDMConnector::eunomiaDir()
  )

  con <- DatabaseConnector::connect(connectionDetails)

  suppressMessages({
    cdm <- CDMConnector::cdmFromCon(
      con = con,
      cdmSchema = "main",
      writeSchema = "main"
    )
  })

  df <- dplyr::collect(head(cdm$person))
  expect_true(is.data.frame(df))
  cdmDisconnect(cdm)
})


test_that("DatabaseConnector DBI connections work with RPostgres", {

  skip_if_not_installed("DatabaseConnector")
  skip_if_not_installed("RPostgres")
  skip_if_not("postgres" %in% dbToTest)
  skip_on_cran()

  connectionDetails <- DatabaseConnector::createDbiConnectionDetails(
    dbms = "postgresql",
    drv = RPostgres::Postgres(),
    dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
    host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
  )

  writeSchema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
  cdmSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

  con <- DatabaseConnector::connect(connectionDetails)

  cdm <- CDMConnector::cdmFromCon(
    con = con,
    cdmSchema = cdmSchema,
    writeSchema = writeSchema
  )

  df <- dplyr::collect(head(cdm$person))
  expect_true(is.data.frame(df))
  cdmDisconnect(cdm)
})



