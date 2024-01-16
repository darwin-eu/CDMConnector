

# issue https://github.com/darwin-eu/CDMConnector/issues/12
test_that("write_schema can be in a separate database in snowflake", {
  skip("manual test")
  skip_on_cran()

  con <- DBI::dbConnect(odbc::odbc(),
                        SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                        UID = Sys.getenv("SNOWFLAKE_USER"),
                        PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                        DATABASE = "OMOP_SYNTHETIC_DATASET",
                        WAREHOUSE = "COMPUTE_WH_XS",
                        DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))

  cdm_schema <- c(catalog = "OMOP_SYNTHETIC_DATASET", schema = "CDM53")

  # named components are optional if there are just two. I will assume it is database, schema.
  write_schema <- c(catalog = "ATLAS", schema = "RESULTS")
  write_schema <- c("ATLAS", "RESULTS")

  # optionally add a prefix. If a prefix is used then schema components shoule be named.
  write_schema <- c(catalog = "ATLAS", schema = "RESULTS", prefix = "temp_")

  cdm <- cdm_from_con(con, cdm_schema, write_schema)

  new_table <- cdm$person %>%
    head(5) %>%
    computeQuery(name = "person_subset",
                 temporary = FALSE,
                 schema = write_schema,
                 overwrite = TRUE)

  new_table <- dplyr::collect(new_table)

  expect_s3_class(new_table, "data.frame")
  expect_equal(nrow(new_table), 5)

  # check that overwrite works
  new_table <- cdm$person %>%
    head(5) %>%
    computeQuery(name = "person_subset",
                 temporary = FALSE,
                 schema = write_schema,
                 overwrite = TRUE)

  new_table <- dplyr::collect(new_table)

  expect_s3_class(new_table, "data.frame")
  expect_equal(nrow(new_table), 5)

  DBI::dbDisconnect(con)
})

