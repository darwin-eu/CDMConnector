
# dbToTest <- c(
#   "duckdb"
#   ,"postgres"
#   ,"redshift"
#   ,"sqlserver"
#   ,"snowflake"
#
#   # ,"spark"
#   # ,"oracle"
#   ,"bigquery"
# )

# dbtype = "oracle"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - verify_write_access"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran()
    write_schema <- get_write_schema(dbtype)
    con <- get_connection(dbtype)
    skip_if(any(write_schema == "") || is.null(con))
    expect_no_error(verify_write_access(con, write_schema = write_schema))
    disconnect(con)
  })
}

