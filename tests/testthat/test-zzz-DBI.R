# Test the basics

test_that("dbWriteTable works on redshift", {
  skip_if(Sys.getenv("REDSHIFT_USER") == "")
  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("REDSHIFT_DBNAME"),
                        host     = Sys.getenv("REDSHIFT_HOST"),
                        port     = Sys.getenv("REDSHIFT_PORT"),
                        user     = Sys.getenv("REDSHIFT_USER"),
                        password = Sys.getenv("REDSHIFT_PASSWORD"))

  tablename <- "public.test"
  df1 <- data.frame(logical = TRUE, chr = "a", int = 1L)
  # df1 <- dplyr::tibble(logical = TRUE, chr = "a", int = 1L) # this gives a warning
  DBI::dbWriteTable(con, DBI::SQL(tablename), df1)
  df2 <- DBI::dbReadTable(con, DBI::SQL(tablename))
  DBI::dbRemoveTable(con, DBI::SQL(tablename))
  expect_true(all.equal(df1, df2))
  DBI::dbDisconnect(con)
})
