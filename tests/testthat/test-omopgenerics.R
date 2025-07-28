
test_that("omopgenerics::bind works for combining cohort tables", {
  skip_if_not_installed("duckdb")
  skip_if_not("duckdb" %in% dbToTest)
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main", "main", cdmName = "gibleed")

  cohortSet <- CDMConnector::readCohortSet(system.file("cohorts2", package = "CDMConnector")) %>%
    dplyr::filter(.data$cohort_name == "gibleed_male")

  expect_equal(nrow(cohortSet), 1)

  cdm <- CDMConnector::generateCohortSet(cdm,
                                         cohortSet,
                                         name = "cohort1",
                                         overwrite = TRUE)

  cdm <- generateConceptCohortSet(
    cdm,
    conceptSet = list(a_drug = c(40213160)),
    name = "cohort2"
  )

  cdm <- omopgenerics::bind(cdm$cohort1, cdm$cohort2, name = "cohort3")

  expect_equal(
    nrow(dplyr::collect(cdm$cohort1)) + nrow(dplyr::collect(cdm$cohort2)),
    nrow(dplyr::collect(cdm$cohort3))
  )

  expect_equal(nrow(settings(cdm$cohort3)), 2)

  expect_equal(nrow(cohortCount(cdm$cohort3)), 2)

  cdmDisconnect(cdm)

})









