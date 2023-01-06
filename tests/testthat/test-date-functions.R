
test_that("Date functions work on duckdb", {
  con <- DBI::dbConnect(duckdb::duckdb())
  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)
  expect_equal(df$dif_years, 1)
  expect_equal(df$dif_days, 365)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)
  expect_equal(df$dif_days2, 1)
  expect_equal(df$dif_days3, -1)

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("Date functions work on Postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname =   Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host =     Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user =     Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)
  expect_equal(df$dif_years, 1)
  expect_equal(df$dif_days, 365)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(df$dif_days2, 1)
  expect_equal(df$dif_days3, -1)

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)


  DBI::dbDisconnect(con)
})


test_that("Date functions work on SQL Server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
  con <-   con <- DBI::dbConnect(odbc::odbc(),
                                 Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                                 Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                                 Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                                 UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                                 PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                                 TrustServerCertificate="yes",
                                 Port     = 1433)

  suppressMessages({
    date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate", overwrite = TRUE, temporary = TRUE)
  })

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)
  expect_equal(df$dif_years, 1)
  expect_equal(df$dif_days, 365)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(df$dif_days2, 1)
  expect_equal(df$dif_days3, -1)

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)

  DBI::dbDisconnect(con)
})


test_that("Date functions work on Redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)

  # TODO Datediff returns integer64 on redshift - How to handle different return types?
  expect_equal(as.integer(df$dif_years), 1)
  expect_equal(as.integer(df$dif_days), 365)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(as.integer(df$dif_days2), 1)
  expect_equal(as.integer(df$dif_days3), -1)

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)

  DBI::dbDisconnect(con)
})

test_that("Date functions work on spark", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("only run test manually")

  con <- DBI::dbConnect(odbc::odbc(), "Databricks", bigint = "numeric")

  DBI::dbWriteTable(con, name = DBI::SQL("omop531results.tmpdate"), value = data.frame(date1 = as.Date("1999-01-01")))
  date_tbl <- dplyr::tbl(con, DBI::Id(schema = "omop531results", table = "tmpdate"))

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)

  # TODO Datediff returns integer64 on spark - How to handle different return types? Use the bigint argument when connecting?
  expect_equal(as.integer(df$dif_years), 1)
  expect_equal(as.integer(df$dif_days), 365)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(as.integer(df$dif_days2), 1)
  expect_equal(as.integer(df$dif_days3), -1)

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)

  DBI::dbRemoveTable(con, DBI::SQL("omop531results.tmpdate"))

  DBI::dbDisconnect(con)
})


test_that("Date functions work on Oracle", {
  skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)

  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")


  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)

  expect_equal(as.integer(df$dif_years), 1)
  expect_equal(as.integer(df$dif_days), 365)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(as.integer(df$dif_days2), 1)
  expect_equal(as.integer(df$dif_days3), -1)

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::days(1), 1)
  expect_equal(lubridate::interval(df$date1, df$date3) / lubridate::days(1), -1)


  dplyr::mutate(dob = as.Date(paste0(
    .data$year_of_birth1, "/",
    .data$month_of_birth1, "/",
    .data$day_of_birth1
  )))

  date_tbl2 <- dplyr::copy_to(con, data.frame(y = 2000L, m = 10L, d = 10L), name = "tmpdate2", overwrite = TRUE, temporary = TRUE)

  # This fails on Oracle
  # df <- date_tbl2 %>%
  #   dplyr::mutate(date_from_parts = as.Date(paste0(
  #   .data$year_of_birth1, "/",
  #   .data$month_of_birth1, "/",
  #   .data$day_of_birth1
  # )))

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
    .data$y, "/",
    .data$m, "/",
    .data$d
  ))) %>%
    collect()

  DBI::dbDisconnect(con)
})
