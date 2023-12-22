test_that("cdm_sample works", {
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con = con, cdm_schema = "main", write_schema = "main", cdm_name = "test")
  cdm_sampled <- cdm_sample(cdm, n = 10)

  df <- cdm_sampled$person %>%
    dplyr::tally() %>%
    dplyr::collect()

  expect_equal(as.double(df$n), 10)

  DBI::dbDisconnect(con, shutdown = TRUE)
})
