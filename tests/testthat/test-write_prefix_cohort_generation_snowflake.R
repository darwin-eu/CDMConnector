
test_that("generateCohortSet works with write_prefix on snowflake", {
  skip_if_not_installed("CirceR")
  skip_on_cran()
  skip_if_not("snowflake" %in% dbToTest)
  con <- get_connection("snowflake")
  cdm_schema <- get_cdm_schema("snowflake")
  write_schema <- get_write_schema("snowflake")
  local_db_cleanup(con, write_schema)

  skip_if(is.null(con))
  skip_if(any(cdm_schema == ""))
  skip_if(any(write_schema == ""))

  cdm <- cdmFromCon(
    con = con, cdmName = "snowflake", cdmSchema = cdm_schema,
    writeSchema = write_schema
  )

  cohort_set <- readCohortSet(system.file("cohorts3", package = "CDMConnector", mustWork = TRUE)) %>%
    dplyr::slice(4)

  cohort_table_name <- "tmp_cohort"

  cdm <- generateCohortSet(cdm, cohort_set, name = cohort_table_name, overwrite = TRUE)

  expect_s3_class(cdm[[cohort_table_name]], "cohort_table")
})



