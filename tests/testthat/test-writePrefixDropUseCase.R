test_that("dropping all tables with write_prefix works as exepected", {

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomia_dir()))

  write_prefix <- 'study01_'
  write_schema <- c(schema = "main", prefix = write_prefix)

  cdm <- cdm_from_con(con = con,
                      cdm_schema = "main",
                      write_schema = write_schema,
                      cdm_name = "my_duckdb_database")

  cdm <- generate_concept_cohort_set(cdm = cdm, concept_set = list("a" = 4112343), name = "my_new_table_1")
  cdm <- generate_concept_cohort_set(cdm = cdm, concept_set = list("b" = 28060), name = "my_new_table_2")

  # use write schema instead of "main".
  # The prefix is hidden becuase it is treated as part of the schema.
  # When we ask for tables in the "write_schema" we only get tables that begin with the prefix
  expect_true(length(list_tables(con, write_schema)) > 0)

  expect_no_error(
    dropTable(cdm, dplyr::starts_with(write_prefix))
  )

  expect_true(length(listTables(con, write_schema)) == 0)

  DBI::dbDisconnect(con, shutdown = T)

})
