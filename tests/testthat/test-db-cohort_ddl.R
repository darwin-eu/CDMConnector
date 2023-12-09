test_cohort_ddl <- function(con, write_schema) {
  skip_if_not("prefix" %in% names(write_schema))
  name <- "testcohort"

  expect_no_error(
    createCohortTables(con,
                       writeSchema = write_schema,
                       name = name,
                       computeAttrition = FALSE)
  )

  tables <- sort(list_tables(con, schema = write_schema))

  if (dbms(con) %in% c("oracle", "snowflake")) {
    name <- toupper(name)
  }

  expect_true(name %in% tables)

  tables_to_drop <- stringr::str_subset(tables, name)

  for (tb in tables_to_drop) {
    DBI::dbRemoveTable(con, inSchema(write_schema, tb, dbms = dbms(con)))
  }

  tables <- list_tables(con, schema = write_schema)
  for (tb in tables_to_drop) {
    expect_false(tb %in% tables)
  }
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - createCohortTables"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    con <- get_connection(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || is.null(con))
    test_cohort_ddl(con, write_schema = write_schema)
    disconnect(con)
  })
}
