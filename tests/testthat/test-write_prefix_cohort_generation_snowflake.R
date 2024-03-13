
test_that("generate_cohort_set works with write_prefix on snowflake", {
  skip_if_not_installed("CirceR")
  skip_on_cran()
  skip_if_not("snowflake" %in% dbToTest)
  prefix <- paste0("test", as.integer(Sys.time()) %% 10000, "_")
  con <- get_connection("snowflake")
  cdm_schema <- get_cdm_schema("snowflake")
  write_schema <- get_write_schema("snowflake", prefix = prefix)

  skip_if(is.null(con))
  skip_if(cdm_schema == "")
  skip_if(write_schema == "")

  cdm <- cdm_from_con(
    con = con, cdm_name = "snowflake", cdm_schema = cdm_schema,
    write_schema = write_schema
  )

  cohort_set <- read_cohort_set(system.file("cohorts3", package = "CDMConnector", mustWork = TRUE)) %>%
    dplyr::slice(4)

  cohort_table_name <- paste0("tmp_cohort", as.integer(Sys.time()) %% 1000)

  on.exit(dropTable(cdm, dplyr::everything()), add = TRUE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  cdm <- generate_cohort_set(cdm, cohort_set, name = cohort_table_name, overwrite = TRUE)

  expect_s3_class(cdm[[cohort_table_name]], "GeneratedCohortSet")
})



