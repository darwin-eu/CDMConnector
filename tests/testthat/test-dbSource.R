
test_cdm_from_con <- function(con, cdm_schema, write_schema) {
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = cdm_schema,
    write_schema = write_schema
  )
  # insert table
  tab <- datasets::cars
  nam <- inSchema(schema = write_schema, table = "x_test")
  DBI::dbWriteTable(conn = con, name = nam, value = tab)
  x <- dplyr::tbl(src = con, nam)
  expect_no_error(cdm$x_test <- x)
  expect_true(inherits(cdm$x_test, "cdm_table"))
  expect_error(cdm$other_name <- x)
  expect_error(cdm$local <- tab)
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - db source insert from source table"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    con <- get_connection(dbtype)
    cli::cat_rule(paste("running cdm test on ", dbtype))
    cli::cat_line(paste("DBI::dbIsValid(con):", DBI::dbIsValid(con)))
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}
