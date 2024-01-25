test_that("pool connections work", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("pool")
  drv <- duckdb::duckdb(eunomia_dir())
  pool <- pool::dbPool(drv)

  cdm <- cdm_from_con(con = pool,
                      cdm_schema = "main",
                      write_schema = "main")

  expect_s3_class(cdm, "cdm_reference")
  expect_equal(dbms(pool), "duckdb")
  df <- cdm$person %>%
    dplyr::filter(.data$person_id < 10) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  duckdb::duckdb_shutdown(drv)
  pool::poolClose(pool)
})

