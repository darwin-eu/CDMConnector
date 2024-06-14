library(CDMConnector)
library(testthat)
test_that("cohort codelist attributes are preserved", {

  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )
  cdm <- generateConceptCohortSet(cdm, name = "celecoxib",
                                  conceptSet = list(celecoxib = 1118084))

  cl <- cohortCodelist(cdm$celecoxib, 1)

  df <- dplyr::collect(attr(cdm$celecoxib, "cohort_codelist"))
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 1)
  expect_equal(names(cl), "celecoxib")
  expect_equal(cl[[1]], 1118084L)

  cdm2 <- cdm_from_con(
    con = con, cdm_name = "eunomia",
    cdm_schema = "main",
    write_schema = "main",
    cohort_tables = "celecoxib"
  )
  # double check that the initial code list is still there
  df <- dplyr::collect(attr(cdm$celecoxib, "cohort_codelist"))
  expect_s3_class(df, "data.frame")

  # check that the new reference also has the codelist
  cl2 <- cohortCodelist(cdm2$celecoxib, 1)
  df2 <- dplyr::collect(attr(cdm2$celecoxib, "cohort_codelist"))
  expect_s3_class(df2, "data.frame")
  expect_equal(nrow(df2), 1)
  expect_equal(names(cl), "celecoxib")
  expect_equal(cl[[1]], 1118084L)

  DBI::dbDisconnect(con, shutdown = T)

})
