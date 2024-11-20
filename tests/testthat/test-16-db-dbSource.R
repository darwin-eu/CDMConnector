
test_cdm_from_con <- function(con, cdm_schema, write_schema) {
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia",
    cdmSchema = cdm_schema,
    writeSchema = write_schema
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
  # insertTable
  expect_no_error(cdm <- insertTable(cdm = cdm, name = "xxx", table = cars))
  expect_true("xxx" %in% names(cdm))
  # list tables
  expect_no_error(ls <- listSourceTables(cdm = cdm))
  expect_identical(sort(ls), c("x_test", "xxx"))
  cdm[["x_test"]] <- NULL
  expect_false("x_test" %in% names(cdm))
  expect_true("x_test" %in% listSourceTables(cdm = cdm))
  expect_no_error(cdm <- readSourceTable(cdm = cdm, name = "x_test"))
  expect_true("x_test" %in% names(cdm))

  # row order is not deterministic
  expect_identical(
    dplyr::as_tibble(cars) |> dplyr::arrange(speed, dist),
    cdm[["x_test"]] |> dplyr::collect() |> dplyr::as_tibble() |> dplyr::arrange(speed, dist)
  )
  expect_no_error(cdm <- dropSourceTable(cdm = cdm, "x_test"))
  expect_false("x_test" %in% names(cdm))
  expect_false("x_test" %in% listSourceTables(cdm = cdm))
  expect_true(setdiff(ls, listSourceTables(cdm = cdm)) == "x_test")
  expect_no_error(dropSourceTable(cdm = cdm, dplyr::everything()))
  expect_identical(listSourceTables(cdm = cdm), character())
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - db source insert from source table"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}



