test_that("prefixed tables are dropped", {

  # only testing on snowflake for now. should be the same for other dbs.
  # TODO: run this on all databases
  # skip_if_not("snowflake" %in% dbToTest)

  con <- DBI::dbConnect(odbc::odbc(),
                        SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                        UID = Sys.getenv("SNOWFLAKE_USER"),
                        PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                        DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                        WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                        DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))

  (cdm_schema <- strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]])
  (write_schema <- strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]])

  # Be sure to set a writePrefix when creating your cdm
  writePrefix <- "p3_c1_001_"
  cdm <- cdmFromCon(con,
                    cdm_schema,
                    write_schema,
                    writePrefix = writePrefix,
                    cdmName = "snowflake-test")

  # example table created during a study
  cdm$new_table <- cdm$cdm_source %>%
    dplyr::compute("new_table")

  tempTables <- listTables(con, schema = write_schema) %>%
    stringr::str_subset(writePrefix)

  expect_true(length(tempTables) > 0)
# debugonce(cdmDisconnect)
  # when you disconnect, set dropPrefixTables = TRUE. This should remove the intermediate tables.
  cdmDisconnect(cdm, dropPrefixTables = TRUE)

  # reconnect and check that the tables are gone.
  con <- DBI::dbConnect(odbc::odbc(),
                        SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                        UID = Sys.getenv("SNOWFLAKE_USER"),
                        PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                        DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                        WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                        DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))

  # the intermediate prefixed tables are gone
  tempTables <- listTables(con, schema = write_schema) |>
    stringr::str_subset(writePrefix)

  expect_true(length(tempTables) == 0)

  DBI::dbDisconnect(con)
})


test_that("temporary duckdb files are cleaned up on disconnect", {
  skip_if_not_installed("duckdb")
  skip_on_cran()
  # skip_if_not("duckdb" %in% dbToTest)
  tempDuckdbFile <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir(databaseFile = tempDuckdbFile))
  cdm <- cdmFromCon(con, "main", "main")
  expect_true(file.exists(tempDuckdbFile))
  cdmDisconnect(cdm)
  expect_false(file.exists(tempDuckdbFile))
})

