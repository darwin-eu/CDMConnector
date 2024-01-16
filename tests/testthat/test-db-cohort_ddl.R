test_cohort_ddl <- function(con, write_schema) {
  skip_if_not("prefix" %in% names(write_schema))
  name <- "testcohort"

  # without attrition
  expect_no_error(
    createCohortTables(con,
                       writeSchema = write_schema,
                       name = name,
                       computeAttrition = FALSE)
  )

  # list_tables subsets based on prefix and strips prefix from names
  tables <- sort(list_tables(con, schema = write_schema))
  expect_true(name %in% tables)

  # with attrition - table are overwritten
  expect_no_error(
    createCohortTables(con,
                       writeSchema = write_schema,
                       name = name,
                       computeAttrition = TRUE)
  )

  tables <- list_tables(con, schema = write_schema)
  expect_true(name %in% tables)
  expect_true(paste0(name, "_inclusion") %in% tables)
  expect_true(paste0(name, "_inclusion_result") %in% tables)
  expect_true(paste0(name, "_inclusion_stats") %in% tables)
  expect_true(paste0(name, "_summary_stats") %in% tables)
  expect_true(paste0(name, "_censor_stats") %in% tables)

  tables_to_drop <- stringr::str_subset(tables, name)

  for (tb in tables_to_drop) {
    DBI::dbRemoveTable(con, inSchema(write_schema, tb, dbms = dbms(con)))
  }

  tables <- list_tables(con, schema = write_schema)
  expect_false(name %in% tables)
  expect_false(paste0(name, "_inclusion") %in% tables)
  expect_false(paste0(name, "_inclusion_result") %in% tables)
  expect_false(paste0(name, "_inclusion_stats") %in% tables)
  expect_false(paste0(name, "_summary_stats") %in% tables)
  expect_false(paste0(name, "_censor_stats") %in% tables)
}

# dbtype = "snowflake"
# dbToTest = c("snowflake", "sqlserver", "postgres", "redshift", "duckdb")
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - createCohortTables"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran()
    con <- get_connection(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || is.null(con))
    test_cohort_ddl(con, write_schema = write_schema)
    disconnect(con)
  })
}
