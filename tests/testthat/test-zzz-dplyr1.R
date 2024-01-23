# copy_to with overwrite=T does not work on Oracle. Global temp tables need to be truncated before being dropped.

# test_that("Oracle dplyr works", {
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
#   skip_on_ci() # need development version of dbplyr
#
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#
#   cdm <- CDMConnector::cdm_from_con(
#     con = con,
#     cdm_schema = "CDMV5"
#   )
#
#   df <- cdm$observation_period %>%
#     dplyr::mutate(new_date = !!asDate(observation_period_start_date)) %>% #as.Date translation is incorrect
#     head() %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   df <- cdm$person %>%
#     # dplyr::left_join(cdm$observation_period, by = "person_id") %>% # this fails
#     dplyr::left_join(cdm$observation_period, by = "person_id", x_as = "x", y_as = "y") %>%
#     head() %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#
#   df <- cdm$person %>%
#     dplyr::slice_sample(n = 100) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   cdm$person %>%
#     dplyr::select(year_of_birth, month_of_birth, day_of_birth) %>%
#     dplyr::mutate(dob = as.Date(paste0(
#       .data$year_of_birth, "/",
#       .data$month_of_birth, "/",
#       .data$day_of_birth
#     ))) %>%
#     dbplyr::sql_render()
#
#   DBI::dbDisconnect(con)
# })
#
#
# # Test dbplyr quantile translation -----
#
#
# test_that("quantile translation works on postgres", {
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#   skip("manual test")
#
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                         dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                         host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                         user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                         password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#
#
#   cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
#
#   # fails
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#   #
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on sql server", {
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#   skip("manual test")
#
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate = "yes",
#                         Port     = 1433)
#
#   cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"))
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   # fails
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::collect()
#
#   # expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on redshift", {
#   skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
#   skip("manual test")
#
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                         port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                         user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                         password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#
#   cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
#
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#   #
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on Oracle", {
#   skip_on_ci()
#   skip_on_cran()
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
#   skip("manual test")
#
#   cdm_schema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#
#   cdm <- cdm_from_con(con, cdm_schema = cdm_schema)
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on Spark", {
#   skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
#   skip("manual test")
#
#   con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks", bigint = "numeric")
#
#   cdm <- cdm_from_con(con, cdm_schema = "omop531")
#
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#   #
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on duckdb", {
#   skip_if_not(rlang::is_installed("duckdb"))
#   skip_if_not(eunomia_is_available())
#   skip("manual test")
#
#   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#
#   cdm <- cdm_from_con(con, cdm_schema = "main")
#
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
#
# test_that("Oracle inSchema works", {
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDrivers())
#
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#   cdm_schema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
#
#   person <- dplyr::tbl(con, inSchema(cdm_schema, "PERSON", dbms(con))) %>%
#     dplyr::rename_all(tolower)
#
#   observation_period <- dplyr::tbl(con, inSchema(cdm_schema, "OBSERVATION_PERIOD", dbms(con))) %>%
#     dplyr::rename_all(tolower)
#
#   df <- dplyr::inner_join(person, observation_period, by = "person_id") %>%
#     head(2) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
#


# TODO not operator on a column does not work on sqlserver
# dplyr::tbl(attr(cdm, "dbcon"), inSchema(attr(cdm, "write_schema"),
#                                         tempName,
#                                         dbms = dbms(con))) %>%
#   dplyr::filter(!.data$is_excluded)
