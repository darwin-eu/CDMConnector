# Test DBI functions we rely on
# DBI::dbRemoveTable
# DBI::dbWriteTable (using temporary and overwrite arguments)

test_dbi <- function(con, cdm_schema, write_schema) {

  df <- dplyr::tibble(
    alogical = TRUE,
    achar = "a",
    aint = 1L,
    afloat = 1.5
  )

  # TODO make sure that overwrite works
  if ("temp_test" %in% list_tables(con, write_schema)) {
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)))
    # DBI::dbRemoveTable(con, DBI::Id(write_schema, "temp_test"))
  }

  # if (methods::is(con, "DatabaseConnectorConnection")) {
    # database connector turns logical types to integers and gives a warning
    # expect_warning(
    #   DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)), df),
    #   "logical"
    # )
  # } else {
    DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)), df)
    # DBI::dbWriteTable(con, DBI::Id(write_schema, "temp_test"), df)
  # }

  expect_no_error({
    DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)), df, overwrite = T)
    # DBI::dbWriteTable(con, DBI::Id(write_schema, "temp_test"), df, overwrite = T)
  })

  expect_true("temp_test" %in% list_tables(con, schema = write_schema))
  # nm <- paste(c(write_schema, "temp_test"), collapse = ".")

  # duckdb and snowflake do not support the I() syntax
  # db <- {if (dbms(con) != "duckdb") dplyr::tbl(con, I(nm)) else dplyr::tbl(con, nm)} %>%
  db <- dplyr::tbl(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con))) %>%
    dplyr::collect() %>%
    dplyr::tibble() %>%
    dplyr::select("alogical", "achar", "aint", "afloat") # bigquery can return columns in any order apparently
  # TODO open issue on bigrquery about columns being returned in any order.

  # TODO: There is an issue with oracle's odbc type conversion
  if (CDMConnector::dbms(con) == "oracle") {
    db$alogical <- as.logical(db$alogical)
    db$aint <- as.integer(db$aint)
  }

  if (methods::is(con, "DatabaseConnectorConnection")) {
    # logical types not supported
    expect_true(all.equal(df[,-1], db[,-1]))
  } else {
    expect_true(all.equal(df, db))
  }

  # table names can be uppercase! (e.g. Oracle)
  table_names <- listTables(con, cdm_schema)
  person_table_name <- table_names[tolower(table_names) == "person"]
  stopifnot(length(person_table_name) == 1)

  nm <- paste(c(cdm_schema, person_table_name), collapse = ".")

  # person <- {if (dbms(con) != "duckdb") dplyr::tbl(con, I(nm)) else dplyr::tbl(con, nm)} %>%
  person <- dplyr::tbl(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con))) %>%
    head(1) %>%
    dplyr::collect()

  expect_true(nrow(person) == 1)

  DBI::dbRemoveTable(con, DBI::Id(write_schema, "temp_test"))
  # TODO add this test
  # DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "temp_test"))
  # expect_false("temp_test" %in% list_tables(con, schema = write_schema))
  #
  # expect_no_error(
  #   DBI::dbCreateTable(con,
  #                      inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)),
  #                      fields = c(speed = "integer", dist = "integer"))
  # )
  #
  # # bigquery gives warning
  # expect_no_error(
  #   DBI::dbAppendTable(con,
  #                      name = inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)),
  #                      value = cars)
  # )
  # # error on bigquery
  #
  # DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)))
}

# dbToTest = c("")
dbtype = "snowflake"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - dbi"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    write_schema <- get_write_schema(dbtype)
    # Note: DBI does not need to support the write prefix
    if ("prefix" %in% names(write_schema)) {
      write_schema <- write_schema[names(write_schema) != "prefix"]
    }
    cdm_schema <- get_cdm_schema(dbtype)
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_dbi(con, cdm_schema, write_schema)
    disconnect(con)
  })
}


# DBI::dbexists table does not seem to work on snowflake with odbc using an Id

# TODO test overwrite in dbWriteTable - seems to be failing for snowflake


