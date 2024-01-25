
test_that("assertTables works with local cdms", {
  skip_if_not_installed('arrow')
  skip_if_not_installed("duckdb")
  skip_on_cran()
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )

  expect_equal(version(cdm), "5.3")
  expect_error(assertTables(cdm, "concept"), NA)

  # local r
  cdm_r <- dplyr::collect(cdm)
  expect_equal(version(cdm_r), "5.3")
  assertTables(cdm_r, "concept")

  # arrow
  path <- tempfile()
  dir.create(path)
  stow(cdm, path)

  cdm2 <- cdm_from_files(path, cdm_name = "test", as_data_frame = TRUE)
  expect_s3_class(cdm2, "cdm_reference")

  cdm_arrow <- cdm_from_files(path, cdm_name = "test", as_data_frame = FALSE)
  attr(cdm_arrow, "cdm_version")

  expect_error(assertTables(cdm_arrow, "concept"), NA)
  expect_error(assertTables(cdm_arrow, "blah"))

  DBI::dbDisconnect(con, shutdown = T)
})


