#
# test_that("drop table works in opposition to insert table", {
#
#   con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir()))
#
#   write_prefix <- 'study01_'
#   write_schema <- c(schema = "main", prefix = write_prefix)
#
#   # debugonce(cdm_from_con)
#   cdm <- cdm_from_con(con = con,
#                       cdm_schema = "main",
#                       write_schema = write_schema,
#                       cdm_name = "my_duckdb_database")
#
#   dropTable(cdm, dplyr::everything()) # should not be needed.
#
#   listTables(con, schema = write_schema)
#
#   name <- paste0(c(sample(letters, 5, replace = TRUE), "_test_table"), collapse = "")
#   table <- dplyr::arrange(datasets::cars, dplyr::across(c("speed","dist")))
#   cdm <- insertTable(cdm = cdm, name = name, table = table)
#
#   listTables(con, schema = write_schema)
#
#   expect_true(name %in% names(cdm))
#
#   cdm[[name]] <- NULL
#
#   debugonce(dropTable)
#
#   attributes(cdm)
#   src <- attr(cdm, "cdm_source")
#
#   sloop::s3_dispatch(  dropTable(cdm, name = name))
#
#   dropTable(cdm, name = name)
#
#   listTables(con, schema = write_schema)
#
#   DBI::dbDisconnect(con, shutdown = T)
#
#
# })
#
#
#
# test_that("dropping all tables with write_prefix works as exepected", {
#
#   con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir()))
#
#   write_prefix <- 'study01_'
#   write_schema <- c(schema = "main", prefix = write_prefix)
#
#   # debugonce(cdm_from_con)
#   cdm <- cdm_from_con(con = con,
#                       cdm_schema = "main",
#                       write_schema = write_schema,
#                       cdm_name = "my_duckdb_database")
#
#   list_tables(con, write_schema)
#   expect_true(length(list_tables(con, write_schema)) == 0)
#
#   cdm <- generate_concept_cohort_set(cdm = cdm, concept_set = list("a" = 4112343), name = "my_new_table_1")
#   cdm <- generate_concept_cohort_set(cdm = cdm, concept_set = list("b" = 28060), name = "my_new_table_2")
#
#   # use write schema instead of "main".
#   # The prefix is hidden becuase it is treated as part of the schema.
#   # When we ask for tables in the "write_schema" we only get tables that begin with the prefix
#   expect_true(length(list_tables(con, write_schema)) > 0)
#
#   expect_no_error(
#     dropTable(cdm, dplyr::starts_with(write_prefix))
#   )
#
#   expect_true(length(listTables(con, write_schema)) == 0)
#
#   DBI::dbDisconnect(con, shutdown = T)
# })
#
#
#
