# Test DBI functions we rely on

test_dbi <- function(con, cdm_schema, write_schema) {
  df <- data.frame(logical = TRUE, char = "a", int = 1L, float = 1.5, stringsAsFactors = FALSE)
  # df1 <- dplyr::tibble(logical = TRUE, chr = "a", int = 1L) # this gives a warning

  if ("temp_test" %in% listTables(con, write_schema)) {
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)))
  }

  # DBI::dbWriteTable(con, DBI::Id(schema = write_schema, table = "temp_test"), df)
  DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)), df)
  expect_true("temp_test" %in% listTables(con, schema = write_schema))

  db <- dplyr::tbl(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con))) %>%
    dplyr::collect() %>%
    as.data.frame() %>%
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

  person_tbl <- dplyr::tbl(con, inSchema(schema = cdm_schema, table = person_table_name, dbms = dbms(con))) %>%
    head(1) %>%
    dplyr::collect()

  expect_true(nrow(person_tbl) == 1)

  DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)))
  # DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "temp_test"))
  expect_false("temp_test" %in% listTables(con, schema = write_schema))
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - dbi"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    write_schema <- get_write_schema(dbtype)
    skip_if(write_schema == "")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    test_dbi(con, cdm_schema, write_schema)
    disconnect(con)
  })
}
