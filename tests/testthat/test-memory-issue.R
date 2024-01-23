# test to address a memory leak issue
# https://github.com/darwin-eu-dev/CDMConnector/issues/312
# fixed in v1.1.3

test_that("memory leak does not happen", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )

  conceptSet <- list(asthma = 317009)

  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_1"
  )

  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_2",
    overwrite = TRUE
  )

  # print(object.size(cdm), units = "MB")
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_3",
    overwrite = TRUE
  )
  # print(object.size(cdm), units = "MB")
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_4",
    overwrite = TRUE
  )
  # print(object.size(cdm), units = "MB")
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_5",
    overwrite = TRUE
  )

  # in the memory leak we had an issue where subsequent cohort tables
  # were much larger
  # This was strange error. Also can use pryr::object_size() and waldo::compare() to investigate
  expect_equal(object.size(cdm$asthma_1), object.size(cdm$asthma_5))

  cdm2 <- cdmSubsetCohort(cdm = cdm, cohortTable = "asthma_5")
  cdm2 <- unclass(cdm2)
  for (nm in names(cdm2)) {
    expect_false("cdm_reference" %in% names(attributes(cdm2[[nm]])))
  }

  DBI::dbDisconnect(con, shutdown = TRUE)
})

