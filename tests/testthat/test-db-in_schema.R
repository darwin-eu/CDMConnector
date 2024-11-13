
# fails on duckdb

test_in_schema <- function(con, cdm_schema) {

  tbls <- listTables(con, cdm_schema)
  nm <- stringr::str_subset(tbls, "person|PERSON")
  stopifnot(tolower(nm) == "person")

  if (length(cdm_schema) == 1) {
    db <- dplyr::tbl(con, dbplyr::in_schema(cdm_schema, nm))
  } else if (length(cdm_schema) == 2) {
    db <- dplyr::tbl(con, dbplyr::in_catalog(cdm_schema[1], cdm_schema[2], nm))
  }

  df <- db %>%
    head() %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - date functions"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype == "duckdb") skip("failing test")
    cdm_schema <- get_cdm_schema(dbtype)
    skip_if(any(cdm_schema == ""))
    con <- get_connection(dbtype)
    skip_if(is.null(con))
    test_in_schema(con, cdm_schema)
    disconnect(con)
  })
}

# https://github.com/darwin-eu/CDMConnector/issues/28
test_that("catalog works on spark", {
  # skip("manual test") # spark tests are manual because the test server needs to be started

  con <- DBI::dbConnect(
    odbc::databricks(),
    httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
    useNativeQuery = FALSE
  )

  # DBI::dbGetQuery(con, "show catalogs;")
  # DBI::dbExecute(con, "USE CATALOG spark_catalog;")

  debugonce(CDMConnector::cdmFromCon)
  cdm <- cdmFromCon(con = con,
                    cdmSchema = "spark_catalog.gibleed",
                    writeSchema = "spark_catalog.scratch")

  expect_s3_class(cdm, "cdm_reference")

  cdm <- cdmFromCon(con = con,
                    cdmSchema = c(catalog="spark_catalog", schema="gibleed"),
                    writeSchema = c(catalog="spark_catalog", schema="scratch"))

  expect_s3_class(cdm, "cdm_reference")
  cdmDisconnect(cdm)
})

