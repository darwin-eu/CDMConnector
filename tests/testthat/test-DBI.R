# # Test DBI functions we rely on
#
# test_dbi <- function(con, write_schema) {
#   df <- data.frame(logical = TRUE, char = "a", int = 1L, float = 1.5, stringsAsFactors = FALSE)
#   # df1 <- dplyr::tibble(logical = TRUE, chr = "a", int = 1L) # this gives a warning
#
#   DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con)), df)
#
#   db <- dplyr::tbl(con, inSchema(schema = write_schema, table = "temp_test", dbms = dbms(con))) %>%
#     dplyr::collect() %>%
#     as.data.frame()
#
#   expect_true(all.equal(df, db))
#   expect_true("temp_test" %in% list_tables(con, schema = write_schema))
#   DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "temp_test"))
#   expect_false("temp_test" %in% list_tables(con, schema = write_schema))
# }
#
# test_that("duckdb - dbi", {
#   con <- get_connection("duckdb")
#   write_schema <- get_write_schema("duckdb")
#   test_dbi(con, write_schema)
#   disconnect(con)
# })
#
# test_that("postgres - dbi", {
#   con <- get_connection("postgres")
#   write_schema <- get_write_schema("postgres")
#   test_dbi(con, write_schema)
#   disconnect(con)
# })
#
# test_that("sqlserver - dbi", {
#   con <- get_connection("sqlserver")
#   write_schema <- get_write_schema("sqlserver")
#   test_dbi(con, write_schema)
#   disconnect(con)
# })
#
# test_that("redshift - dbi", {
#   con <- get_connection("redshift")
#   write_schema <- get_write_schema("sqlserver")
#   test_dbi(con, write_schema)
#   disconnect(con)
# })
#
# test_that("oracle - date functions", {
#   skip("failing test")
#   con <- get_connection("oracle")
#   test_date_functions(con)
#   disconnect(con)
# })
#
# test_that("bigquery - date functions", {
#   skip("failing test")
#   con <- get_connection("bigquery")
#   test_date_functions(con)
#   disconnect(con)
# })
#
# test_that("snowflake - date functions", {
#   skip("failing test")
#   con <- get_connection("snowflake")
#   test_date_functions(con)
#   disconnect(con)
# })
#
#
#
# test_that("dbWriteTable works on redshift", {
#   skip_if(Sys.getenv("REDSHIFT_USER") == "")
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("REDSHIFT_HOST"),
#                         port     = Sys.getenv("REDSHIFT_PORT"),
#                         user     = Sys.getenv("REDSHIFT_USER"),
#                         password = Sys.getenv("REDSHIFT_PASSWORD"))
#
#   tablename <- "public.test"
#   df1 <- data.frame(logical = TRUE, chr = "a", int = 1L)
#   # df1 <- dplyr::tibble(logical = TRUE, chr = "a", int = 1L) # this gives a warning
#   DBI::dbWriteTable(con, DBI::SQL(tablename), df1)
#   df2 <- DBI::dbReadTable(con, DBI::SQL(tablename))
#   DBI::dbRemoveTable(con, DBI::SQL(tablename))
#   expect_true(all.equal(df1, df2))
#   DBI::dbDisconnect(con)
# })
