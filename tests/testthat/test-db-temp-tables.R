
test_temp_tables <- function(dbtype) {
  # create a temp table with dbWriteTable

  con <- get_connection(dbtype)

  if (dbtype == "snowflake") {
    # temp tables don't seem to work on snowflake with dbWriteTable
    DBI::dbExecute(con, glue::glue("USE SCHEMA {paste(get_write_schema(dbtype), collapse = '.')};"))

    DBI::dbExecute(con, 'CREATE TEMPORARY TABLE "mytemptable" (id NUMBER, creation_date DATE);')
    expect_true("mytemptable" %in% list_tables(con, get_write_schema(dbtype)))
    disconnect(con)

    con <- get_connection(dbtype)
    expect_true("mytemptable" %in% list_tables(con, get_write_schema(dbtype)))
    return(NULL)
  }

  DBI::dbWriteTable(con, name = "temp_cars", value = cars[1,], temporary = TRUE)
  expect_true("temp_cars" %in% list_tables(con))

  expect_error(
    DBI::dbWriteTable(con, name = "temp_cars", value = cars[1,], temporary = TRUE, overwrite = FALSE)
  )

  expect_no_error(
    DBI::dbWriteTable(con, name = "temp_cars", value = cars[1,], temporary = TRUE, overwrite = TRUE)
  )

  df <- dplyr::tbl(con, "temp_cars") %>%
    dplyr::collect()

  expect_true(nrow(df) == 1)

  disconnect(con)

  # disconnecting removes temp tables
  con <- get_connection(dbtype)
  expect_false("temp_cars" %in% list_tables(con))
  disconnect(con)
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - temp_tables"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    if (dbtype %in% c("spark", "sqlserver", "snowflake", "bigquery")) skip("failing test")
    skip_if(get_write_schema(dbtype) == "")
    con <- get_connection(dbtype)
    test_temp_tables(dbtype)
    disconnect(con)
  })
}
