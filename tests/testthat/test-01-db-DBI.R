# Test DBI functions we rely on
# DBI::dbRemoveTable
# DBI::dbWriteTable (using temporary and overwrite arguments)

test_dbi <- function(con, cdm_schema, write_schema) {

  # Use a unique table name to avoid collisions between concurrent CI runs
  tbl_name <- paste0("dbitest_", format(as.hexmode(sample.int(.Machine$integer.max, 1)), width = 8))

  df <- dplyr::tibble(
    alogical = TRUE,
    achar = "a",
    aint = 1L,
    afloat = 1.5
  )

  # Cleanup any stale table from a previous run
  if (tolower(tbl_name) %in% tolower(listTables(con, write_schema))) {
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con)))
  }

  DBI::dbWriteTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con)), df)

  expect_no_error({
    DBI::dbWriteTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con)), df, overwrite = T)
  })

  # Case-insensitive check for Snowflake (unquoted identifiers become uppercase)
  expect_true(tolower(tbl_name) %in% tolower(listTables(con, schema = write_schema)))

  db <- dplyr::tbl(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))) %>%
    dplyr::collect() %>%
    dplyr::tibble() %>%
    dplyr::select("alogical", "achar", "aint", "afloat") # bigquery can return columns in any order apparently

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

  if (CDMConnector::dbms(con) == "bigquery") {
    # BigQuery cannot infer a type from an all-NULL column. insertTable() must
    # create an explicit schema so that typed missing values retain their type.
    typed_missing_name <- paste0("dbitypedmissing_", format(as.hexmode(sample.int(.Machine$integer.max, 1)), width = 8))
    typed_missing <- dplyr::tibble(
      date_value = as.Date(NA),
      datetime_value = as.POSIXct(NA),
      integer_value = as.integer(NA),
      logical_value = as.logical(NA),
      numeric_value = as.numeric(NA),
      character_value = as.character(NA),
      numeric_text_value = "0"
    )
    source <- dbSource(con, writeSchema = write_schema)
    omopgenerics::insertTable(source, typed_missing_name, typed_missing)

    information_schema <- paste(c(write_schema, "INFORMATION_SCHEMA.COLUMNS"), collapse = ".")
    column_types <- DBI::dbGetQuery(
      con,
      glue::glue("SELECT column_name, data_type FROM `{information_schema}` WHERE table_name = '{typed_missing_name}'")
    )
    expect_equal(
      stats::setNames(column_types$data_type, column_types$column_name),
      c(date_value = "DATE", datetime_value = "TIMESTAMP", integer_value = "INT64",
        logical_value = "BOOL", numeric_value = "FLOAT64", character_value = "STRING",
        numeric_text_value = "STRING")
    )
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = typed_missing_name, dbms = dbms(con)))
  }

  # table names can be uppercase! (e.g. Oracle)
  table_names <- listTables(con, cdm_schema)
  person_table_name <- table_names[tolower(table_names) == "person"]
  stopifnot(length(person_table_name) == 1)

  nm <- paste(c(cdm_schema, person_table_name), collapse = ".")

  person <- dplyr::tbl(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))) %>%
    head(1) %>%
    dplyr::collect()

  expect_true(nrow(person) == 1)

  DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con)))
  # TODO add this test
  # DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "temp_test"))
  # expect_false("temp_test" %in% listTables(con, schema = write_schema))
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
# dbtype = "snowflake"
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
