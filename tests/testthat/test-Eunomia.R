test_that("downloadEunomiaData", {
  skip("manual test")
  path <- downloadEunomiaData()
  expect_true(nchar(path) > 0)

  path <- downloadEunomiaData("GiBleed", overwrite = T)
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir("GiBleed")))
  cdm <- cdm_from_con(con, "main", cdm_name = "test")
  expect_s3_class(cdm, "cdm_reference")
  DBI::dbDisconnect(con, shutdown = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir("synthea-anemia-10k")))
  cdm <- cdm_from_con(con, "main")
  expect_s3_class(cdm, "cdm_reference")
  DBI::dbDisconnect(con, shutdown = TRUE)

  path <- downloadEunomiaData("synthea-breast_cancer-10k", overwrite = T)
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir("synthea-breast_cancer-10k")))
  cdm <- cdm_from_con(con, "main")
  expect_s3_class(cdm, "cdm_reference")
  DBI::dbDisconnect(con, shutdown = TRUE)

  path <- downloadEunomiaData("synthea-covid19-10k", overwrite = T)
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir("synthea-covid19-10k")))
  cdm <- cdm_from_con(con, "main")
  expect_s3_class(cdm, "cdm_reference")
  DBI::dbDisconnect(con, shutdown = TRUE)

  path <- downloadEunomiaData("synthea-covid19-200k", overwrite = T)
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir("synthea-covid19-200k")))
  cdm <- cdm_from_con(con, "main")
  expect_s3_class(cdm, "cdm_reference")
  DBI::dbDisconnect(con, shutdown = TRUE)

  expect_error(downloadEunomiaData(datasetName = NULL))
  expect_error(downloadEunomiaData(pathToData = NULL))
})

test_that("empty cdm works", {
  # skip("manual test")
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available("empty_cdm"))

  expect_true("empty_cdm" %in% example_datasets())

  expect_no_error({
    con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir("empty_cdm")))
    cdm <- cdm_from_con(con, "main", "main", cdm_name = "empty_cdm")
  })

  rowcounts <- purrr::map_int(cdm_select_tbl(cdm, -tbl_group("vocab")),
                              ~dplyr::pull(dplyr::count(.), 'n'))

  expect_true(all(rowcounts == 0))
  DBI::dbDisconnect(con, shutdown = T)
})


