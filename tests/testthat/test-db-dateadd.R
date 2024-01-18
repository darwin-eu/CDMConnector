
test_date_functions <- function(con, write_schema) {

  date_df <- dplyr::tibble(
    date1 = as.Date(c("2000-12-01", "2000-12-01", "2000-12-01", "2000-12-01")),
    date2 = as.Date(c("2001-12-01", "2001-12-02", "2001-11-30", "2001-01-01")),
    y = 2000L, m = 10L, d = 11L
  )

  # upload the date table
  if ("tmpdate" %in% list_tables(con, write_schema)) {
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)))
  }

  # TODO can this be fixed?
  if (dbms(con) == "oracle") {
    # uploading date types gives 'Numeric value out of range' error
    date_df %>%
      dplyr::mutate(date1 = as.character(.data$date1), date2 = as.character(.data$date2)) %>%
      {DBI::dbWriteTable(con,
                         inSchema(schema = write_schema, table = "tmpdate0", dbms = dbms(con)),
                         value = .,
                         overwrite = TRUE)}

    date_tbl <- dplyr::tbl(con, inSchema(schema = write_schema, table = "tmpdate0", dbms = dbms(con))) %>%
      dplyr::mutate(date1 = !!asDate(date1), date2 = !!asDate(date2)) %>%
      computeQuery(temporary = FALSE, name = "tmpdate", schema = write_schema, overwrite = TRUE)

    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "tmpdate0", dbms = dbms(con)))
  } else {
    DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)), date_df)
    expect_true("tmpdate" %in% list_tables(con, write_schema))
    date_tbl <- dplyr::tbl(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)))
  }
  # test datediff
  df <- date_tbl %>%
    dplyr::mutate(date3 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::select("date1", "date2", "date3", "dif_years", "dif_days") %>%
    dplyr::collect() %>%
    dplyr::left_join(date_df, ., by = c("date1", "date2")) %>%
    dplyr::mutate(dif_years = as.numeric(dif_years), dif_days = as.numeric(dif_days))

  expect_true(all((lubridate::interval(df$date1, df$date3) / lubridate::years(1)) == 1))
  expect_equal(df$dif_years, c(1, 1, 0, 0))
  expect_equal(df$dif_days, c(365, 366, 364,  31))

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "day")) %>%
    dplyr::mutate(date3 = !!dateadd("date1", -1, interval = "day")) %>%
    dplyr::mutate(dif_days2 = !!datediff("date1", "date2", interval = "day")) %>%
    dplyr::mutate(dif_days3 = !!datediff("date1", "date3", interval = "day")) %>%
    dplyr::collect() %>%
    # dplyr::mutate(dplyr::across(1:3, ~as.Date(., origin = "1970-01-01"))) %>%
    dplyr::mutate(dif_days2 = as.integer(dif_days2), dif_days3 = as.integer(dif_days3))

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

  # test creation of date from parts
  if (dbms(con) != "bigquery") {
    # TODO paste0 translation incorrect on bigquery
    df <- date_tbl %>%
      dplyr::transmute(date_from_parts = paste0(
        as.character(.data$y), "-",
        as.character(.data$m), "-",
        as.character(.data$d)
      )) %>%
      dplyr::mutate(date_from_parts = !!asDate(date_from_parts)) %>%
      dplyr::collect() %>%
      dplyr::distinct()

    expect_equal(as.Date(df$date_from_parts), as.Date("2000-10-11"))
  }

  # test datepart
  df <- date_tbl %>%
    dplyr::transmute(
      year  = !!datepart("date1", "year"),
      month = !!datepart("date1", "month"),
      day   = !!datepart("date1", "day")) %>%
    dplyr::collect() %>%
    dplyr::distinct()

  expect_equal(df$year, 2000)
  expect_equal(df$month, 12)
  expect_equal(df$day, 1)
}

# dbtype = "bigquery"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - date functions"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran()
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == ""))
    con <- get_connection(dbtype)
    skip_if(is.null(con))
    test_date_functions(con, write_schema)
    disconnect(con)
  })
}

# TODO as.Date translation fails on Oracle. asDate provides the workaround.
# df <- date_tbl2 %>%
#   dplyr::mutate(date_from_parts = as.Date(paste0(
#   .data$year_of_birth1, "/",
#   .data$month_of_birth1, "/",
#   .data$day_of_birth1
# )))

test_that('dateadd works without pipe', {
  skip("failing test")
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(con, "tbl", data.frame(date = as.Date("2020-01-01")))

  db <- dplyr::tbl(con, "tbl")

  df1 <- db %>%
    dplyr::mutate(next_day = !!dateadd("date", 1, "day")) %>%
    dplyr::collect()


  df2 <- db |>
    dplyr::mutate(next_day = !!dateadd("date", 1, "day")) |>
    dplyr::collect()

  # dateadd does not work without the magrittr pipe
  df3 <- dplyr::mutate(db, next_day = !!dateadd("date", 1, "day")) %>%
    dplyr::collect()

  expect_equal(df1, df2)
  expect_equal(df2, df3)

  DBI::dbDisconnect(con, shutdown = T)
})

