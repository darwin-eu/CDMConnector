library(testthat)
library(CDMConnector)
test_that("generate_cohort_set works with write_prefix on snowflake", {

  # skip("failing test that should pass")

  con <- DBI::dbConnect(odbc::odbc(),
                        SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                        UID = Sys.getenv("SNOWFLAKE_USER"),
                        PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                        DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                        WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                        DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))

  cdm_schema <- strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]]
  write_schema <- strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]]

  write_schema <- c(catalog = write_schema[1], schema = write_schema[2], prefix = "test_")

  cdm <- cdm_from_con(con, cdm_schema, write_schema)

  cohort_set <- read_cohort_set(system.file("cohorts3", package = "CDMConnector", mustWork = TRUE)) %>%
    dplyr::slice(4)

  cohort_table_name <- paste0("tmp_cohort", as.integer(Sys.time()) %% 1000)

  on.exit(drop_table(cdm, starts_with("tmp_cohort")), add = TRUE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  cdm <- generate_cohort_set(cdm, cohort_set, name = cohort_table_name, overwrite = TRUE)

  expect_s3_class(cdm[[cohort_table_name]], "GeneratedCohortSet")
})



