
test_that("visR attrition diagram works", {
  skip_if_not_installed("visR")
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, "main", "main")
  cohort_set <- read_cohort_set(system.file("cohorts2", package = "CDMConnector"))
  cdm <- generate_cohort_set(cdm, cohort_set, name = "cohort", overwrite = T)

  expect_error({
    cohort_attrition(cdm$cohort) %>%
      visR::visr()
  })

  expect_no_error({
    cohort_attrition(cdm$cohort) %>%
      dplyr::filter(cohort_definition_id == 3) %>%
      visR::visr()
  })

  DBI::dbDisconnect(con, shutdown = TRUE)
})



