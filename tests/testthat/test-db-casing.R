
test_lowercase <- function(con, cdm_schema, write_schema) {

  cars2 <- dplyr::rename_all(cars, toupper)

  # overwrite argument does not work on all dbms
  if ("cars" %in% list_tables(con, write_schema)) {
    DBI::dbRemoveTable(con, inSchema(write_schema, "cars", dbms = dbms(con)))
  }

  DBI::dbWriteTable(con, inSchema(write_schema, "cars", dbms = dbms(con)), cars2, overwrite = T)
  df <- dplyr::tbl(con, inSchema(write_schema, "cars", dbms = dbms(con)))
  expect_s3_class(df, "tbl_sql")

  df <- df %>%
    dplyr::rename_all(tolower) %>%
    head(5) %>%
    dplyr::collect()

  expect_true(nrow(df) == 5)
  expect_true(all(names(df) == tolower(names(df))))

  DBI::dbRemoveTable(con, inSchema(write_schema, "cars", dbms = dbms(con)))
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - lowercase columns"), {
    if (!(dbtype %in% ciTestDbs))) skip_on_ci()
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_lowercase(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}



