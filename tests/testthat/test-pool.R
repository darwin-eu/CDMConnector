test_that("pool connections work", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("pool")
  skip_if_not("duckdb" %in% dbToTest)
  dbpath <- eunomiaDir()
  drv <- duckdb::duckdb(dbpath)
  pool <- pool::dbPool(drv)
  withr::defer({
    try(pool::poolClose(pool), silent = TRUE)
    try(duckdb::duckdb_shutdown(drv), silent = TRUE)
    unlink(dbpath)
  })

  cdm <- cdmFromCon(con = pool,
                    cdmSchema = "main",
                    writeSchema = "main")

  expect_s3_class(cdm, "cdm_reference")
  expect_equal(dbms(pool), "duckdb")
  df <- cdm$person %>%
    dplyr::filter(.data$person_id < 10) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")
})

