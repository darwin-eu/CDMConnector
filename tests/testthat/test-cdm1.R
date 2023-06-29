library(testthat)
library(dplyr, warn.conflicts = FALSE)

### CDM object DBI drivers ------
test_cdm_from_con <- function(con, cdm_schema, write_schema) {
  cdm <- cdm_from_con(con, cdm_schema = cdm_schema)
  expect_s3_class(cdm, "cdm_reference")
  expect_error(assert_tables(cdm, "person"), NA)
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")

  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  expect_s3_class(cdm, "cdm_reference")
  expect_error(assert_write_schema(cdm), NA)
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")
  expect_equal(dbms(cdm), dbms(attr(cdm, "dbcon")))

  # simple join
  df <- dplyr::inner_join(cdm$person, cdm$observation_period, by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")
}

dbToTest <- c(
  "duckdb"
  ,"postgres"
  ,"redshift"
  ,"sqlserver"
  # ,"oracle" # requires development dbplyr version to work
  ,"snowflake"
  ,"bigquery"
)

# dbtype = "bigquery"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - dbi"), {
    skip_if(get_cdm_schema(dbtype) == "")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}
