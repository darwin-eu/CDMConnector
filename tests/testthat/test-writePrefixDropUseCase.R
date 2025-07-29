
test_that("drop table works in opposition to insert table", {
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir()))

  writePrefix <- 'study01_'
  writeSchema <- c(schema = "main", prefix = writePrefix)

  cdm <- cdmFromCon(con = con,
                    cdmSchema = "main",
                    writeSchema = writeSchema,
                    cdmName = "myDuckdbDatabase")

  dropSourceTable(cdm, dplyr::everything()) # should not be needed.

  expect_equal(listTables(con, schema = writeSchema), character(0L))
  expect_equal(length(listTables(con, schema = "main")), 39)

  name <- paste0(c(sample(letters, 5, replace = TRUE), "_test_table"), collapse = "")
  table <- dplyr::arrange(datasets::cars, dplyr::across(c("speed","dist")))
  cdm <- insertTable(cdm = cdm, name = name, table = table)

  expect_equal(listTables(con, schema = writeSchema), name)

  expect_true(name %in% names(cdm))

  cdm[[name]] <- NULL

  # attributes(cdm)
  # src <- attr(cdm, "cdm_source")
  # sloop::s3_dispatch(  dropTable(cdm, name = name))

  dropSourceTable(cdm, name = name)

  expect_equal(listTables(con, schema = writeSchema), character(0L))

  DBI::dbDisconnect(con)
})



test_that("dropping all tables with write_prefix works as exepected", {
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir()))

  writePrefix <- "study01_"
  writeSchema <- c(schema = "main", prefix = writePrefix)

  cdm <- cdmFromCon(
    con = con,
    cdmSchema = "main",
    writeSchema = writeSchema,
    cdmName = "myDuckdbDatabase"
  )

  expect_true(length(listTables(con, writeSchema)) == 0)

  cdm <- generateConceptCohortSet(cdm = cdm, conceptSet = list("a" = 4112343), name = "my_new_table_1")
  cdm <- generateConceptCohortSet(cdm = cdm, conceptSet = list("b" = 28060), name = "my_new_table_2")

  # use write schema instead of "main".
  # The prefix is hidden because it is treated as part of the schema.
  # When we ask for tables in the "write_schema" we only get tables that begin with the prefix
  expect_true(length(listTables(con, writeSchema)) > 0)

  # this is not the way to drop all prefixed tables. use starts_with to drop a subset of prefixed tables.
  expect_no_error(
    dropSourceTable(cdm, dplyr::starts_with(writePrefix))
  )

  DBI::dbWriteTable(con, "cars", cars)

  # dropping everything just drops stuff in prefixed in the write schema
  expect_no_error(
    dropSourceTable(cdm, dplyr::everything())
  )

  expect_true(length(listTables(con, writeSchema)) == 0)
  expect_true("cars" %in% listTables(con, "main"))
  expect_true(length(listTables(con, "main")) > 0) # cdm tables are still there

  # check that insertTable prefixes table names
  cdm <- insertTable(cdm, "cars2", cars)
  expect_true("study01_cars2" %in% DBI::dbListTables(con))

  # DBI::dbListTables gives us full names including the prefix while
  # CDMConnector::listTables strips off the prefix since it is treated as a sub-schema.

  DBI::dbDisconnect(con)
})



