test_that("cdm_select_tbl works", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  cdm <- cdm_from_con(con, "main")

  expect_equal(names(cdm_select_tbl(cdm, person)), "person")
  expect_equal(names(cdm_select_tbl(cdm, person, observation_period)), c("person", "observation_period"))
  expect_equal(names(cdm_select_tbl(cdm, tbl_group("vocab"))), tbl_group("vocab"))
  expect_equal(names(cdm_select_tbl(cdm, "person")), "person")
  expect_error(names(cdm_select_tbl(cdm, "blah")))

  DBI::dbDisconnect(con, shutdown = TRUE)
})
