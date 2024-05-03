
test_benchmarkCDMConnector <- function(con, cdm_schema, write_schema) {

  cdm <- cdm_from_con(
    con = con, cdm_name = "test", cdm_schema = cdm_schema,
    write_schema = write_schema
  )

  expect_no_error(bench <- benchmarkCDMConnector(cdm))

  expect_error(bench <- benchmarkCDMConnector("not a cdm"))

}

# for now only test this on duckdb. it takes a while to run.
for (dbtype in "duckdb") {
  test_that(glue::glue("{dbtype} - dplyr queries"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_benchmarkCDMConnector(con, cdm_schema, write_schema)
    disconnect(con)
  })
}



