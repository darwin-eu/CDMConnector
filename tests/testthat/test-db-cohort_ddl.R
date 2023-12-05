test_cohort_ddl <- function(con, write_schema) {
  expect_no_error(
    createCohortTables(con,
                       writeSchema = write_schema,
                       name = "testcohort",
                       computeAttrition = FALSE)
  )

  tables <- sort(list_tables(con, schema = write_schema))

  # cleanup ...
  # tables_to_drop <- tables %>% stringr::str_subset("TEST")
  # purrr::walk(tables_to_drop, ~DBI::dbRemoveTable(con, inSchema(write_schema , .)))

  name <- paste0(tidyr::replace_na(write_schema["prefix"], ""), "testcohort")

  if (dbms(con) %in% c("oracle", "snowflake")) {
    name <- toupper(name)
  }

  expect_true(name %in% tables)

  if ("prefix" %in% names(write_schema)) {
    write_schema <- unname(write_schema[names(write_schema) != "prefix"])
  }

  DBI::dbRemoveTable(con, inSchema(write_schema, name, dbms = dbms(con)))
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
