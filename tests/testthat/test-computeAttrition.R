test_that("computeAttrition", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  write_schema <- "main"
  cdm_schema <- "main"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      cdm_tables = c(tbl_group("default")),
                      write_schema = write_schema)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE)) %>%
    dplyr::rename(cohortId = cohort_definition_id) %>%
    dplyr::rename(cohortName = cohort_name)

  nm <- "cohort_inclusion_result"
  DBI::dbCreateTable(con,
                     name = inSchema(nm, write_schema),
                     fields = c(
                       cohort_definition_id = "INT",
                       inclusion_rule_mask = "INT",
                       person_count = "INT",
                       mode_id = "INT"))

  # has to be fixed yet
  expect_error(computeAttrition(cdm,
                                cohortStem = "cohort",
                                cohortSet,
                                cohortId = NULL))

  DBI::dbDisconnect(con, shutdown = TRUE)
})
