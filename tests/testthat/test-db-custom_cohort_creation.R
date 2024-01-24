
test_custom_derived_cohort <- function(con, cdm_schema, write_schema) {
  skip_if_not_installed("CirceR")
  cdm <- cdm_from_con(con, cdm_schema, write_schema)
  cohort_set <- read_cohort_set(system.file("cohorts3", package = "CDMConnector"))
  cdm <- generate_cohort_set(cdm, cohort_set, name = "cohort")

  cdm$cohort2 <- cdm$cohort %>%
    dplyr::filter(!!datediff("cohort_start_date", "cohort_end_date") >= 14) %>%
    dplyr::mutate(cohort_definition_id = 10 + cohort_definition_id) %>%
    dplyr::union_all(
      cdm$cohort %>%
        dplyr::filter(!!datediff("cohort_start_date", "cohort_end_date") >= 21) %>%
        dplyr::mutate(cohort_definition_id = 100 + cohort_definition_id)
    ) %>%
    dplyr::union_all(
      cdm$cohort %>%
        dplyr::filter(!!datediff("cohort_start_date", "cohort_end_date") >= 28) %>%
        dplyr::mutate(cohort_definition_id = 1000 + cohort_definition_id)
    ) %>%
    compute(name = "cohort2", temporary = FALSE, overwrite = TRUE)

  expect_warning({
    chr <- new_generated_cohort_set(cdm$cohort2) # this function creates the cohort object and metadata
  }, "deprecated")

  expect_s3_class(cdm$cohort2, "GeneratedCohortSet")

}

# dbToTest <- c("duckdb", "snowflake", "postgres", "sqlserver", "redshift")

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - test_custom_derived_cohort"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")

    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_custom_derived_cohort(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}


