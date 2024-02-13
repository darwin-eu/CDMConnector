

test_bind_cohorts <- function(con, cdm_schema, write_schema) {

  cdm <- cdm_from_con(con, cdm_schema, write_schema, cdm_name = "test")

  cohort_set1 <- read_cohort_set(system.file("cohorts1", package = "CDMConnector"))
  cohort_set2 <- read_cohort_set(system.file("cohorts2", package = "CDMConnector")) %>%
    dplyr::mutate(cohort_name = paste0("another_", cohort_name)) # names must be unique

  cdm <- generate_cohort_set(cdm, cohort_set1, name = "cohort1")
  cdm <- generate_cohort_set(cdm, cohort_set2, name = "cohort2")

  cdm <- omopgenerics::bind(cdm$cohort1, cdm$cohort2, name = "cohort3")

  expect_s3_class(cdm$cohort3, "cohort_table")
  omopgenerics::dropTable(cdm, c("cohort1", "cohort2", "cohort3"))
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet"), {
    skip_if_not_installed("CirceR")
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_bind_cohorts(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}

