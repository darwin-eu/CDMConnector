
# inSchema ----------
# test prefix option

test_in_schema <- function(con, write_schema, cdm_schema) {

  # upload a table to the database
  if (is.null(con) || is.null(write_schema)) { return(invisible(NULL)) }

  if ("temp_test" %in% list_tables(con, schema = write_schema)) {
    DBI::dbRemoveTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)))
  }

  expect_no_error(
    DBI::dbWriteTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)), cars)
  )

  if (dbms(con) != "snowflake") {
    # overwrite does not work on snowflake
    expect_no_error(
      DBI::dbWriteTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)), cars, overwrite = TRUE)
    )
  }

  expect_true("temp_test" %in% list_tables(con, write_schema))

  # <BigQueryConnection> uses an old dbplyr interface
  # i Please install a newer version of the package or contact the maintainer
  # This warning is displayed once every 8 hours.
  # Backtrace:
  #   1. test_in_schema(con, write_schema, get_cdm_schema("bigquery"))

  suppressWarnings({
    df <- dplyr::tbl(con, inSchema(write_schema, "temp_test", dbms = dbms(con))) %>%
      dplyr::mutate(a = 1) %>% # needed for duckdb for some reason??
      dplyr::collect() %>%
      dplyr::select("speed", "dist") %>%
      as.data.frame() %>%
      dplyr::arrange(.data$speed, .data$dist)
  })
  expect_equal(df, dplyr::arrange(cars, .data$speed, .data$dist))

  tables <- list_tables(con, cdm_schema) %>% tolower()
  expect_true(all(c("person", "observation_period") %in% tables))

  DBI::dbRemoveTable(con, inSchema(write_schema, "temp_test", dbms = dbms(con)))
  expect_false("temp_test" %in% list_tables(con, write_schema))
}


# dbToTest <- c(
#   "duckdb"
#   ,"postgres"
#   ,"redshift"
#   ,"sqlserver"
#   # ,"oracle" # not compatible with requested type: [type=character; target=double].
#   ,"snowflake"
#   ,"bigquery"
# )

# dbtype = "oracle"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - inSchema"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran()
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)

    write_schema <- get_write_schema(dbtype, prefix = paste0("tmp", as.integer(Sys.time()), "_"))
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_in_schema(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}


# test_that("oracle - list_tables only", {
# only needed when the previous test was failing
#   con <- get_connection("oracle")
#   tables <- list_tables(con, get_cdm_schema("oracle")) %>% tolower
#   expect_true(all(c("person", "observation_period") %in% tables))
#   disconnect(con)
# })

test_that("getFullTableNameQuoted", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )

  result <- getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = NULL)
  expect_equal(as.character(result), "myTable")

  result <- getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = c("mySchema"))
  expect_equal(result, "mySchema.myTable")

  result <- getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = c("mySchema", "dbo"))
  expect_equal(result, "mySchema.dbo.myTable")

  # invalid input
  expect_error(getFullTableNameQuoted(x = NULL, name = "myTable", schema = c("mySchema", "dbo")))
  expect_error(getFullTableNameQuoted(x = cdm$person, name = NULL, schema = c("mySchema", "dbo")))
  expect_error(getFullTableNameQuoted(x = cdm$person, name = "myTable", schema = -1))

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that(".dbIsValid", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  result <- .dbIsValid(con)
  expect_true(result)

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("inSchema", {
  result <- inSchema(schema = "main", table = "myTable")
  expect_equal(as.character(class(result)), "Id")
  tableSchemaNames <- as.character(attr(result, "name"))
  expect_equal(tableSchemaNames[1], "main")
  expect_equal(tableSchemaNames[2], "myTable")

  result <- inSchema(schema = c("main", "dbo"), table = "myTable")
  expect_equal(as.character(class(result)), "Id")
  tableSchemaNames <- as.character(attr(result, "name"))
  expect_equal(tableSchemaNames[1], "main")
  expect_equal(tableSchemaNames[2], "dbo")
  expect_equal(tableSchemaNames[3], "myTable")
})
