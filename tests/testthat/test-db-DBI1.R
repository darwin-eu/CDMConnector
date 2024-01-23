# Test DBI functions we rely on

test_dbi <- function(con, cdm_schema, write_schema) {
  df <- dplyr::tibble(
    logical = TRUE,
    char = "a",
    int = 1L,
    float = 1.5
  )

  if ("temp_test" %in% list_tables(con, write_schema)) {
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)))
  }

  # DBI::dbWriteTable(con, DBI::Id(schema = write_schema, table = "temp_test"), df)
  # DBI::dbWriteTable(con, "cars", head(cars, 3), overwrite = T)
  # dplyr::tbl(con, "cars")

  DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)), df)
  expect_true("temp_test" %in% list_tables(con, schema = write_schema))

  db <- dplyr::tbl(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con))) %>%
    dplyr::collect() %>%
    dplyr::tibble() %>%
    dplyr::select("logical", "char", "int", "float") # bigquery can return columns in any order apparently

  # TODO: There is an issue with oracle's odbc type conversion
  if (dbms(con) == "oracle") {
    db$logical <- as.logical(db$logical)
    db$int <- as.integer(db$int)
  }

  expect_true(all.equal(df, db))

  # table names can be uppercase! (e.g. Oracle)
  table_names <- listTables(con, cdm_schema)
  person_table_name <- table_names[tolower(table_names) == "person"]
  stopifnot(length(person_table_name) == 1)

  person <- dplyr::tbl(con, inSchema(schema = cdm_schema, table = person_table_name, dbms = dbms(con))) %>%
    head(1) %>%
    dplyr::collect()

  expect_true(nrow(person) == 1)

  DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)))
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

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - dbi"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    write_schema <- get_write_schema(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    con <- get_connection(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_dbi(con, cdm_schema, write_schema)
    disconnect(con)
  })
}

# TODO test dplyr::copy_to with temp and non-temp tables as well as the overwrite argument of dbWriteTable

# DBI::dbexists table does not seem to work on snowflake with odbc using an Id

# TODO test overwrite in dbWriteTable - seems to be failing for snowflake


