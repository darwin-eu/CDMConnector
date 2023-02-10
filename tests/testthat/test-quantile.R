

test_that("quantile translation works on postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")

  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))



  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))

  # fails
  # df <- cdm$drug_exposure %>%
  #   dplyr::select(drug_concept_id, days_supply) %>%
  #   dplyr::group_by(drug_concept_id) %>%
  #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
  #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
  #   dplyr::collect()
  #
  # expect_s3_class(df, "data.frame")

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("quantile translation works on sql server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = "ODBC Driver 18 for SQL Server",
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate = "yes",
                        Port     = 1433)

  cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"), cdm_tables = c("person", "drug_exposure"))

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::distinct(drug_concept_id, q05_days_supply) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  # fails
  # df <- cdm$drug_exposure %>%
  #   dplyr::select(drug_concept_id, days_supply) %>%
  #   dplyr::group_by(drug_concept_id) %>%
  #   dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
  #   dplyr::collect()

  # expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("quantile translation works on redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), cdm_tables = c("person", "drug_exposure"))

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::distinct(drug_concept_id, q05_days_supply) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("quantile translation works on Oracle", {
  skip_on_ci()
  skip_on_cran()
  skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)

  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")

  cdm <- cdm_from_con(con, cdm_schema = "CDMV5", cdm_tables = c("person", "drug_exposure"))

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::distinct(drug_concept_id, q05_days_supply) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("quantile translation works on Spark", {
  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("Only run this test manually")

  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks", bigint = "numeric")

  cdm <- cdm_from_con(con, cdm_schema = "omop531")

  # df <- cdm$drug_exposure %>%
  #   dplyr::select(drug_concept_id, days_supply) %>%
  #   dplyr::group_by(drug_concept_id) %>%
  #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
  #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
  #   dplyr::collect()
  #
  # expect_s3_class(df, "data.frame")

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con)
})

test_that("quantile translation works on duckdb", {
  skip_if_not(rlang::is_installed("duckdb"))
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  cdm <- cdm_from_con(con, cdm_schema = "main", cdm_tables = c("person", "drug_exposure"))

  # df <- cdm$drug_exposure %>%
  #   dplyr::select(drug_concept_id, days_supply) %>%
  #   dplyr::group_by(drug_concept_id) %>%
  #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
  #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
  #   dplyr::collect()

  # expect_s3_class(df, "data.frame")

  df <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  DBI::dbDisconnect(con, shutdown = TRUE)
})
