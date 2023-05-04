
test_that("Date functions work on duckdb", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb())

  date_df <- dplyr::tibble(
    date1 = as.Date(c("2000-12-01", "2000-12-01", "2000-12-01", "2000-12-01")),
    date2 = as.Date(c("2001-12-01", "2001-12-02", "2001-11-30", "2001-01-01"))
  )

  date_tbl <- dplyr::copy_to(con, date_df, name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date3 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_true(all((lubridate::interval(df$date1, df$date3) / lubridate::years(1)) == 1))
  expect_equal(df$dif_years, c(1, 1, 0, 0))
  expect_equal(df$dif_days, c(365, 366, 364,  31))

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect()

  expect_true(all(lubridate::interval(df$date1, df$date2) / lubridate::days(1) == 1))
  expect_true(all(lubridate::interval(df$date1, df$date3) / lubridate::days(1) == -1))
  expect_true(all(df$dif_days2 == 1))
  expect_true(all(df$dif_days3 == -1))

  # can add a date and an integer column
  df <- date_tbl %>%
    dplyr::mutate(number1 = 1L, minus1 = -1L) %>%
    dplyr::mutate(date2 = !!dateadd("date1", "number1", interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", "minus1", interval = "day")) %>%
    dplyr::collect()


  expect_true(all(lubridate::interval(df$date1, df$date2) / lubridate::days(1) == 1))
  expect_true(all(lubridate::interval(df$date1, df$date3) / lubridate::days(1) == -1))


  date_tbl2 <- dplyr::copy_to(con, data.frame(y = 2000L, m = 10L, d = 11L), name = "tmpdate2", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
      as.character(.data$y), "-",
      as.character(.data$m), "-",
      as.character(.data$d)
    ))) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))

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

  date_tbl2 <- dplyr::copy_to(con, data.frame(y = 2000L, m = 10L, d = 11L), name = "tmpdate2", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
      as.character(.data$y), "-",
      as.character(.data$m), "-",
      as.character(.data$d)
    ))) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))

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

  suppressMessages({
    date_tbl2 <- dplyr::copy_to(con, data.frame(y = 2000L, m = 10L, d = 11L), name = "tmpdate2", overwrite = TRUE, temporary = TRUE)
  })

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
      as.character(.data$y), "-",
      as.character(.data$m), "-",
      as.character(.data$d)
    ))) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))

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

  date_tbl2 <- dplyr::copy_to(con, data.frame(y = 2000L, m = 10L, d = 11L), name = "tmpdate2", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
      as.character(.data$y), "-",
      as.character(.data$m), "-",
      as.character(.data$d)
    ))) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))

  DBI::dbDisconnect(con)
})

test_that("Date functions work on Spark", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  skip("manual test")

  con <- DBI::dbConnect(odbc::odbc(), "Databricks", bigint = "numeric")

  DBI::dbWriteTable(con, name = DBI::SQL("omop531results.tmpdate"), value = data.frame(date1 = as.Date("1999-01-01")))
  date_tbl <- dplyr::tbl(con, DBI::Id(schema = "omop531results", table = "tmpdate"))

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(lubridate::interval(df$date1, df$date2) / lubridate::years(1), 1)

  # Datediff returns integer64 on spark but we can use the bigint argument when connecting to have it return a double instead.
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

  DBI::dbWriteTable(con, name = DBI::SQL("omop531results.tmpdate2"), value = data.frame(y = 2000L, m = 10L, d = 11L))
  date_tbl2 <- dplyr::tbl(con, DBI::Id(schema = "omop531results", table = "tmpdate2"))

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
      as.character(.data$y), "-",
      as.character(.data$m), "-",
      as.character(.data$d)
    ))) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))
  DBI::dbRemoveTable(con, DBI::SQL("omop531results.tmpdate2"))
  DBI::dbDisconnect(con)
})

test_that("Date functions work on Oracle", {
  skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)

  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")

  # "tmpdate3" %in% listTables(con)
  # DBI::dbExecute(con, 'truncate table "tmpdate3"')
  # DBI::dbRemoveTable(con, "tmpdate3")

  # dbWriteTable(con, "tmpdate", data.frame(date1 = as.Date("1999-01-01")), overwrite = TRUE)
  # date_tbl <- dplyr::tbl(con, "tmpdate")
  # copy_to with overwrite=T does not work on Oracle. Global temp tables need to be truncated before being dropped.
  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate4", overwrite = TRUE, temporary = TRUE)

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

  date_tbl2 <- dplyr::copy_to(con, data.frame(y = 2000L, m = 10L, d = 11L), name = "tmpdate2", overwrite = TRUE, temporary = TRUE)

  # This fails on Oracle. asDate provides the workaround.
  # df <- date_tbl2 %>%
  #   dplyr::mutate(date_from_parts = as.Date(paste0(
  #   .data$year_of_birth1, "/",
  #   .data$month_of_birth1, "/",
  #   .data$day_of_birth1
  # )))

  df <- date_tbl2 %>%
    dplyr::mutate(date_from_parts = !!asDate(paste0(
    as.character(.data$y), "-",
    as.character(.data$m), "-",
    as.character(.data$d)
  ))) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))

  DBI::dbDisconnect(con)
})

test_that("test year, month, day functionality", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  date_tbl <- dplyr::copy_to(con,
                             data.frame(
                               birth_date = as.Date("1993-04-19"),
                               date1 = as.Date("2023-04-17"),
                               date2 = as.Date("2023-04-22")
                              ),
                             name = "tmp",
                             temporary = TRUE)
  df <- date_tbl %>%
    dplyr::mutate(year = !!extract("birth_date", "year")) %>%
    dplyr::mutate(month = !!extract("birth_date", "month")) %>%
    dplyr::mutate(day = !!extract("birth_date", "day")) %>%
    dplyr::mutate(days1 = !!datediff("birth_date", "date1", "day")) %>%
    dplyr::mutate(month1 = !!datediff("birth_date", "date1", "month")) %>%
    dplyr::mutate(year1 = !!datediff("birth_date", "date1", "year")) %>%
    dplyr::mutate(days2 = !!datediff("birth_date", "date2", "day")) %>%
    dplyr::mutate(month2 = !!datediff("birth_date", "date2", "month")) %>%
    dplyr::mutate(year2 = !!datediff("birth_date", "date2", "year")) %>%
    dplyr::collect()
  expect_true(df$year == 1993)
  expect_true(df$month == 4)
  expect_true(df$day == 19)
  expect_true(df$days1 == 10955)
  expect_true(df$month1 == 359)
  expect_true(df$year1 == 29)
  expect_true(df$days2 == 10960)
  expect_true(df$month2 == 360)
  expect_true(df$year2 == 30)
  DBI::dbDisconnect(con)
})
