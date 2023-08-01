

test_copy_cdm_to <- function(con, schema) {

  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con1, cdm_schema = "main", cdm_name = "test") %>%
      cdm_select_tbl("person", "vocabulary")

  # create another cdm
  prefix <- paste0("tbl", as.integer(Sys.time()), "_")
  cdm2 <- copy_cdm_to(con, cdm, schema = schema, prefix = prefix)
  expect_setequal(names(cdm), names(cdm2))
  expect_s3_class(cdm2, "cdm_reference")

  expect_error(copy_cdm_to(con, cdm, schema = schema, prefix = prefix))

  # create another cdm
  cdm3 <- copy_cdm_to(con, cdm, schema = schema, prefix = prefix, overwrite = TRUE)
  expect_setequal(names(cdm), names(cdm3))
  expect_s3_class(cdm3, "cdm_reference")

  DBI::dbRemoveTable(con, inSchema(schema, paste0(prefix, "person"), dbms = dbms(con)))
  DBI::dbRemoveTable(con, inSchema(schema, paste0(prefix, "vocabulary"), dbms = dbms(con)))

  DBI::dbDisconnect(con1, shutdown = T)
}

dbToTest <- c(
  "duckdb"
  ,"postgres"
  ,"redshift"
  ,"sqlserver"
  # ,"oracle" # requires development dbplyr version to work
  # ,"snowflake"
  # ,"bigquery"
)

# dbtype = "redshift"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - copy_cdm_to"), {
    if (dbtype != "duckdb") skip_on_ci()
    con <- get_connection(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || is.null(con))
    test_copy_cdm_to(con, write_schema)
    disconnect(con)
  })
}



test_that("duckdb - copy_cdm_to without prefix", {

  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm1 <- cdm_from_con(con1, cdm_schema = "main")

  con2 <- DBI::dbConnect(duckdb::duckdb())
  cdm2 <- copy_cdm_to(con2, cdm1, schema = "main")

  expect_setequal(names(cdm1), names(cdm2))
  expect_s3_class(cdm2, "cdm_reference")

  con3 <- DBI::dbConnect(duckdb::duckdb())
  cdm3 <- copy_cdm_to(con3, dplyr::collect(cdm1), schema = "main")

  expect_setequal(names(cdm1), names(cdm3))
  expect_s3_class(cdm3, "cdm_reference")

  DBI::dbDisconnect(con1, shutdown = T)
  DBI::dbDisconnect(con2, shutdown = T)
  DBI::dbDisconnect(con3, shutdown = T)
})

