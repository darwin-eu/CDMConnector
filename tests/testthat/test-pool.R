test_that("pool connections work", {

  skip_if_not_installed("pool")
  pool <- pool::dbPool(
    drv = duckdb::duckdb(),
    dbdir = eunomia_dir()
  )

  cdm <- cdm_from_con(con = pool, cdm_name = "eunomia", cdm_schema = "main")

  expect_s3_class(cdm, "cdm_reference")
  expect_equal(dbms(pool), "duckdb")
  df <- cdm$person %>%
    dplyr::filter(.data$person_id < 10) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  pool::poolClose(pool)
})

