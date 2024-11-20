test_that("write_prefix works with cdm_from_con", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir()))

  DBI::dbExecute(con, "create schema scratch")
  cdm <- cdmFromCon(con, "main", "scratch", writePrefix = "tmp_")

  cdm$count <- cdm$person %>%
    dplyr::tally() %>%
    dplyr::compute(temporary = F, name = "count")

  expect_true("tmp_count" %in% listTables(con, schema = "scratch"))

  DBI::dbDisconnect(con, shutdown = TRUE)
})
