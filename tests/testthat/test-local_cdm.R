
test_that("assertTables works with local cdms", {
  skip("arrow::write_parquet is failing")
  skip_if_not_installed('arrow')
  skip_if_not_installed("duckdb")
  skip_on_cran()
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
  )

  expect_equal(cdmVersion(cdm), "5.3")
  expect_error(assertTables(cdm, "concept"), NA)

  # local r
  cdm_r <- dplyr::collect(cdm)
  expect_equal(cdmVersion(cdm_r), "5.3")
  expect_warning(assertTables(cdm_r, "concept")) # deprecated

  # arrow
  path <- tempfile()
  dir.create(path)
  stow(cdm, path)

  cdm2 <- cdmFromFiles(path, cdmName = "test", asDataFrame = TRUE)
  expect_s3_class(cdm2, "cdm_reference")

  cdm_arrow <- cdmFromFiles(path, cdmame = "test", as_data_frame = FALSE)
  attr(cdm_arrow, "cdm_version")

  expect_error(assertTables(cdm_arrow, "concept"), NA)
  expect_error(assertTables(cdm_arrow, "blah"))

  DBI::dbDisconnect(con, shutdown = T)
})



test_that("softValidation is passed correctly", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

  # create overlapping observation periods
  op <- dplyr::tbl(con, "observation_period") |> dplyr::collect()
  DBI::dbAppendTable(con, "observation_period", op)

  expect_error(cdmFromCon(con, "main", "main", .softValidation = F),
               "overlap")

  # soft validation ignores this
  expect_no_error(cdmFromCon(con, "main", "main", .softValidation = T))

  DBI::dbDisconnect(con, shutdown = T)
})
