#
# test_intesect <- function(con, write_schema) {
#   # test every case (Allen's interval algebra) for two intervals and two people
#   require(dplyr)
#   intervals <- tibble::tribble(
#     ~relationship,  ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     "reference",    1,                     1,           "2022-01-05",       "2022-01-10",
#     "precedes",     2,                     1,           "2022-01-01",       "2022-01-03",
#     "meets",        3,                     1,           "2022-01-01",       "2022-01-04",
#     "overlaps",     5,                     1,           "2022-01-01",       "2022-01-05",
#     "finished_by",  7,                     1,           "2022-01-01",       "2022-01-10",
#     "contains",     8,                     1,           "2022-01-01",       "2022-01-15",
#     "starts",       9,                     1,           "2022-01-05",       "2022-01-07",
#     "equals",      10,                     1,           "2022-01-05",       "2022-01-10",
#     "reference",    1,                     2,           "2022-01-05",       "2022-01-10",
#     "precedes",     2,                     2,           "2022-01-01",       "2022-01-03",
#     "meets",        3,                     2,           "2022-01-01",       "2022-01-04",
#     "overlaps",     5,                     2,           "2022-01-01",       "2022-01-05",
#     "finished_by",  7,                     2,           "2022-01-01",       "2022-01-10",
#     "contains",     8,                     2,           "2022-01-01",       "2022-01-15",
#     "starts",       9,                     2,           "2022-01-05",       "2022-01-07",
#     "equals",      10,                     2,           "2022-01-05",       "2022-01-10") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   if (dbms(con) == "oracle") {
#     # TODO there is an issue inserting dates into an Oracle database
#     df <- intervals %>%
#       dplyr::mutate(dplyr::across(dplyr::matches("date"), as.character))
#
#     DBI::dbWriteTable(con, "tmp_intervals", df, temporary = TRUE, overwrite = TRUE)
#
#     db <- tbl(con, "tmp_intervals") %>%
#       mutate(cohort_start_date = TO_DATE(cohort_start_date, "YYYY-MM-DD")) %>%
#       mutate(cohort_start_date = TO_DATE(cohort_end_date, "YYYY-MM-DD")) %>%
#       compute_query()
#   } else {
#     DBI::dbWriteTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)), intervals, overwrite = TRUE)
#     db <- dplyr::tbl(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))
#   }
#
#   # precedes
#   x <- db %>%
#     filter(relationship %in% c("reference", "precedes")) %>%
#     intersect_cohorts() %>%
#     collect()
#
#   expect_equal(names(x), c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"))
#   expect_equal(nrow(x), 0)
#
#   # meets
#   x <- db %>%
#     filter(relationship %in% c("reference", "meets")) %>%
#     intersect_cohorts() %>%
#     collect()
#
#   expect_equal(nrow(x), 0)
#
#   # overlaps
#   x <- db %>%
#     filter(relationship %in% c("reference", "overlaps")) %>%
#     intersect_cohorts() %>%
#     collect() %>%
#     arrange(subject_id)
#
#   expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,                     1,           "2022-01-05",       "2022-01-05",
#     1,                     2,           "2022-01-05",       "2022-01-05") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   expect_equal(x, expected)
#
#
#   # finished_by
#   x <- db %>%
#     filter(relationship %in% c("reference", "finished_by")) %>%
#     intersect_cohorts() %>%
#     collect() %>%
#     arrange(subject_id)
#
#   expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,                     1,           "2022-01-05",       "2022-01-10",
#     1,                     2,           "2022-01-05",       "2022-01-10") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   expect_equal(x, expected)
#
#   # contains
#   x <- db %>%
#     filter(relationship %in% c("reference", "contains")) %>%
#     intersect_cohorts() %>%
#     collect() %>%
#     arrange(subject_id)
#
#   expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,                     1,           "2022-01-05",       "2022-01-10",
#     1,                     2,           "2022-01-05",       "2022-01-10") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   expect_equal(x, expected)
#
#   # starts
#   x <- db %>%
#     filter(relationship %in% c("reference", "starts")) %>%
#     intersect_cohorts() %>%
#     collect() %>%
#     arrange(subject_id)
#
#   expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,                     1,           "2022-01-05",       "2022-01-07",
#     1,                     2,           "2022-01-05",       "2022-01-07") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   expect_equal(x, expected)
#
#   # equals
#   x <- db %>%
#     filter(relationship %in% c("reference", "equals")) %>%
#     intersect_cohorts() %>%
#     collect() %>%
#     arrange(subject_id)
#
#   expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,                     1,           "2022-01-05",       "2022-01-10",
#     1,                     2,           "2022-01-05",       "2022-01-10") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   expect_equal(x, expected)
#
#   DBI::dbRemoveTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))
# }
#
# test_that("intersect - duckdb", {
#   con <- DBI::dbConnect(duckdb::duckdb())
#   write_schema <- "main"
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
# test_that("intersect - local postgres", {
#   skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                           dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
#                           host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
#                           user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
#                           password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))
#
#   write_schema <- Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA")
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("intersect - postgres", {
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#
#   con <- dbConnect(RPostgres::Postgres(),
#                    dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                    host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#   write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("intersect - sqlserver", {
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate="yes",
#                         Port     = 1433)
#   write_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]]
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("intersect - oracle", {
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
#   skip("failing test that should pass")
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#   write_schema <- "OHDSI"
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("intersect - redshift", {
#   skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
#
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                         port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                         user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                         password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#
#   write_schema <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("intersect - bigquery", {
#   skip_if(Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH") == "")
#   skip("failing test")
#
#   library(bigrquery)
#   bq_auth(path = Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH"))
#   cdm_schema <- Sys.getenv("BIGQUERY_CDM_SCHEMA")
#   write_schema <- Sys.getenv("BIGQUERY_SCRATCH_SCHEMA")
#
#   con <- DBI::dbConnect(
#     bigrquery::bigquery(),
#     project = Sys.getenv("BIGQUERY_PROJECT_ID"),
#     dataset = cdm_schema
#   )
#
#   test_intesect(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# # test collapse helper function -----
#
# test_collapse <- function(con, write_schema) {
#   require(dplyr)
#
#   intervals <- tibble::tribble(
#     ~relationship,  ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     "reference",    1,                     1,           "2022-01-05",       "2022-01-10",
#     "precedes",     1,                     1,           "2022-01-01",       "2022-01-03",
#     "meets",        1,                     1,           "2022-01-01",       "2022-01-04",
#     "overlaps",     1,                     1,           "2022-01-01",       "2022-01-05",
#     "finished_by",  1,                     1,           "2022-01-01",       "2022-01-10",
#     "contains",     1,                     1,           "2022-01-01",       "2022-01-15",
#     "starts",       1,                     1,           "2022-01-05",       "2022-01-07",
#     "equals",       1,                     1,           "2022-01-05",       "2022-01-10",
#     "reference",    1,                     2,           "2022-01-05",       "2022-01-10",
#     "precedes",     1,                     2,           "2022-01-01",       "2022-01-03",
#     "meets",        1,                     2,           "2022-01-01",       "2022-01-04",
#     "overlaps",     1,                     2,           "2022-01-01",       "2022-01-05",
#     "finished_by",  1,                     2,           "2022-01-01",       "2022-01-10",
#     "contains",     1,                     2,           "2022-01-01",       "2022-01-15",
#     "starts",       1,                     2,           "2022-01-05",       "2022-01-07",
#     "equals",       1,                     2,           "2022-01-05",       "2022-01-10",
#     "reference",    2,                     1,           "2022-01-05",       "2022-01-10",
#     "precedes",     2,                     1,           "2022-01-01",       "2022-01-03",
#     "meets",        2,                     1,           "2022-01-01",       "2022-01-04",
#     "overlaps",     2,                     1,           "2022-01-01",       "2022-01-05",
#     "finished_by",  2,                     1,           "2022-01-01",       "2022-01-10",
#     "contains",     2,                     1,           "2022-01-01",       "2022-01-15",
#     "starts",       2,                     1,           "2022-01-05",       "2022-01-07",
#     "equals",       2,                     1,           "2022-01-05",       "2022-01-10",
#     "reference",    2,                     2,           "2022-01-05",       "2022-01-10",
#     "precedes",     2,                     2,           "2022-01-01",       "2022-01-03",
#     "meets",        2,                     2,           "2022-01-01",       "2022-01-04",
#     "overlaps",     2,                     2,           "2022-01-01",       "2022-01-05",
#     "finished_by",  2,                     2,           "2022-01-01",       "2022-01-10",
#     "contains",     2,                     2,           "2022-01-01",       "2022-01-15",
#     "starts",       2,                     2,           "2022-01-05",       "2022-01-07",
#     "equals",       2,                     2,           "2022-01-05",       "2022-01-10") %>%
#     dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   if (dbms(con) == "oracle") {
#     # TODO there is an issue inserting dates into an Oracle database
#     df <- intervals %>%
#       dplyr::mutate(dplyr::across(dplyr::matches("date"), as.character))
#
#     DBI::dbWriteTable(con, "tmp_intervals", df, temporary = TRUE, overwrite = TRUE)
#
#     db <- tbl(con, "tmp_intervals") %>%
#       mutate(cohort_start_date = TO_DATE(cohort_start_date, "YYYY-MM-DD")) %>%
#       mutate(cohort_start_date = TO_DATE(cohort_end_date, "YYYY-MM-DD")) %>%
#       compute_query()
#   } else {
#     DBI::dbWriteTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)), intervals, overwrite = TRUE)
#     db <- dplyr::tbl(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))
#   }
#
#   # precedes. input should equal output.
#   x <- db %>%
#     filter(relationship %in% c("reference", "precedes")) %>%
#     select(-relationship)
#
#   x_expected <- collect(x) %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
#
#   # meets - input should equal output.
#   x <- db %>%
#     filter(relationship %in% c("reference", "meets")) %>%
#     select(-relationship)
#
#   x_expected <- collect(x) %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
#
#   # overlaps - collapse should return one row per person-cohort combination
#   x <- db %>%
#     filter(relationship %in% c("reference", "overlaps")) %>%
#     select(-relationship)
#
#   x_expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,           1,       "2022-01-01",     "2022-01-10",
#     1,           2,       "2022-01-01",     "2022-01-10",
#     2,           1,       "2022-01-01",     "2022-01-10",
#     2,           2,       "2022-01-01",     "2022-01-10"
#   ) %>% dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
#
#   # finished_by - collapse should return one row per person-cohort combination
#   x <- db %>%
#     filter(relationship %in% c("reference", "finished_by")) %>%
#     select(-relationship)
#
#   x_expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,           1,       "2022-01-01",     "2022-01-10",
#     1,           2,       "2022-01-01",     "2022-01-10",
#     2,           1,       "2022-01-01",     "2022-01-10",
#     2,           2,       "2022-01-01",     "2022-01-10"
#   ) %>% dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
#
#   # contains - collapse should return one row per person-cohort combination
#   x <- db %>%
#     filter(relationship %in% c("reference", "contains")) %>%
#     select(-relationship)
#
#   x_expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,           1,       "2022-01-01",     "2022-01-15",
#     1,           2,       "2022-01-01",     "2022-01-15",
#     2,           1,       "2022-01-01",     "2022-01-15",
#     2,           2,       "2022-01-01",     "2022-01-15"
#   ) %>% dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
#
#   # starts - collapse should return one row per person-cohort combination
#   x <- db %>%
#     filter(relationship %in% c("reference", "starts")) %>%
#     select(-relationship)
#
#   x_expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,           1,       "2022-01-05",     "2022-01-10",
#     1,           2,       "2022-01-05",     "2022-01-10",
#     2,           1,       "2022-01-05",     "2022-01-10",
#     2,           2,       "2022-01-05",     "2022-01-10"
#   ) %>% dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
#
#   # equals - collapse should return one row per person-cohort combination
#   x <- db %>%
#     filter(relationship %in% c("reference", "equals")) %>%
#     select(-relationship)
#
#   x_expected <- tibble::tribble(
#     ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
#     1,           1,       "2022-01-05",     "2022-01-10",
#     1,           2,       "2022-01-05",     "2022-01-10",
#     2,           1,       "2022-01-05",     "2022-01-10",
#     2,           2,       "2022-01-05",     "2022-01-10"
#   ) %>% dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))
#
#   x_actual <- x %>%
#     cohortCollapse() %>%
#     collect() %>%
#     arrange(cohort_definition_id, subject_id, cohort_start_date)
#
#   expect_equal(x_actual, x_expected)
# }
#
#
# test_that("collapse - duckdb", {
#   con <- DBI::dbConnect(duckdb::duckdb())
#   write_schema <- "main"
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
# test_that("collapse - local postgres", {
#   skip_if(Sys.getenv("LOCAL_POSTGRESQL_USER") == "")
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                         dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
#                         host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
#                         user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
#                         password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))
#
#   write_schema <- Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA")
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("collapse - postgres", {
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#
#   con <- dbConnect(RPostgres::Postgres(),
#                    dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                    host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#   write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("collapse - sqlserver", {
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate="yes",
#                         Port     = 1433)
#   write_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]]
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("collapse - oracle", {
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
#   skip("failing test that should pass")
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#   write_schema <- "OHDSI"
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("collapse - redshift", {
#   skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
#
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                         port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                         user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                         password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#
#   write_schema <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
# test_that("intersect - bigquery", {
#   skip_if(Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH") == "")
#   skip("failing test")
#
#   library(bigrquery)
#   bq_auth(path = Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH"))
#   cdm_schema <- Sys.getenv("BIGQUERY_CDM_SCHEMA")
#   write_schema <- Sys.getenv("BIGQUERY_SCRATCH_SCHEMA")
#
#   con <- DBI::dbConnect(
#     bigrquery::bigquery(),
#     project = Sys.getenv("BIGQUERY_PROJECT_ID"),
#     dataset = cdm_schema
#   )
#
#   test_collapse(con, write_schema)
#   DBI::dbDisconnect(con)
# })
#
