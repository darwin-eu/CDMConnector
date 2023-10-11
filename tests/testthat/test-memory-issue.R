# test to address a memory leak issue
# https://github.com/darwin-eu-dev/CDMConnector/issues/312
# fixed in v1.1.3


test_that("memory leak does not happen", {

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, "main", "main")

  #2.1Mb
  conceptSet <- list(asthma = 317009)
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_1"
  )

  initial_size <- round(as.numeric(stringr::str_extract(format(object.size(cdm), units = "MB"), "[\\d\\.]+")))
  #15Mb
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_1",
    overwrite = TRUE
  )

  # print(object.size(cdm), units = "MB")
  #53Mb
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_1",
    overwrite = TRUE
  )
  # print(object.size(cdm), units = "MB")
  #167Mb
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_1",
    overwrite = TRUE
  )
  #510Mb
  # print(object.size(cdm), units = "MB")
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
    name = "asthma_1",
    overwrite = TRUE
  )
  #1.5Gb

  size <- round(as.numeric(stringr::str_extract(format(object.size(cdm), units = "MB"), "[\\d\\.]+")))
  expect_equal(size, initial_size)
  DBI::dbDisconnect(con, shutdown = TRUE)
})
