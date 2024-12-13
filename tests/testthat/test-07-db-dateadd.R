
test_date_functions <- function(con, write_schema) {

  date_df <- dplyr::tibble(
    date1 = as.Date(c("2000-12-01", "2000-12-01", "2000-12-01", "2000-12-01")),
    date2 = as.Date(c("2001-12-01", "2001-12-02", "2001-11-30", "2001-01-01")),
    y = 2000L, m = 10L, d = 11L
  )

  # upload the date table
  if ("tmpdate" %in% listTables(con, write_schema)) {
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
      dplyr::mutate(date1 = as.Date(date1), date2 = as.Date(date2)) %>%
      compute(temporary = FALSE, name = "tmpdate", overwrite = TRUE)

    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "tmpdate0", dbms = dbms(con)))
  } else {
    DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)), date_df)
    expect_true("tmpdate" %in% listTables(con, write_schema))
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
      dplyr::mutate(date_from_parts = as.Date(date_from_parts)) %>%
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
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
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

# expect_equal(test_translate_sql(difftime(start_date, end_date, units = "days")), sql("DATEDIFF(`end_date`, `start_date`)"))
# expect_equal(test_translate_sql(difftime(start_date, end_date)), sql("DATEDIFF(`end_date`, `start_date`)"))

# expect_equal(test_translate_sql(add_years(x, 1)), sql("ADD_MONTHS(`x`, 1.0 * 12.0)"))
# expect_equal(test_translate_sql(add_days(x, 1)), sql("DATE_ADD(`x`, 1.0)"))
# expect_equal(test_translate_sql(date_build(2020, 1, 1)), sql("MAKE_DATE(2020.0, 1.0, 1.0)"))
# expect_equal(test_translate_sql(date_build(year_column, 1L, 1L)), sql("MAKE_DATE(`year_column`, 1, 1)"))
# expect_equal(test_translate_sql(get_year(date_column)), sql("DATE_PART('YEAR', `date_column`)"))
# expect_equal(test_translate_sql(get_month(date_column)), sql("DATE_PART('MONTH', `date_column`)"))
# expect_equal(test_translate_sql(get_day(date_column)), sql("DATE_PART('DAY', `date_column`)"))


# The development version of dbplyr contains translations for clock functions that we can use instead of the workarounds.

test_clock_functions <- function(con, write_schema) {

  # skip("manual test")
  skip_if_not_installed("dbplyr", "2.5.0")

  date_df <- dplyr::tibble(
    date1 = as.Date(c("2000-12-01", "2000-12-01", "2000-12-01", "2000-12-01")),
    date2 = as.Date(c("2001-12-01", "2001-12-02", "2001-11-30", "2001-01-01")),
    y = 2000L, m = 10L, d = 11L
  )

  # upload the date table
  if ("tmpdate" %in% listTables(con, write_schema)) {
    DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)))
  }

  # Note there is an issue with uploading date types to Oracle
  DBI::dbWriteTable(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)), date_df)
  expect_true("tmpdate" %in% listTables(con, write_schema))
  date_tbl <- dplyr::tbl(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)))

  # add_years is not working on spark or duckdb
  # add_days, difftime does not work on duckdb
  # add_years and add_days return datetimes on postgres making the as.Date() conversion is needed
  # date_build does not work on redshift or duckdb https://github.com/tidyverse/dbplyr/pull/1513

  library(clock)
  # on postgres we need the as.Date conversion around add_years and add_days
  df <- date_tbl %>%
    dplyr::mutate(y2 = clock::get_year(date1),
                  m2 = clock::get_month(date1),
                  d2 = clock::get_day(date1))

  if (dbms(con) != "duckdb") {
    df <- dplyr::mutate(df,
                        dif_days = difftime(date1, date2),
                        date3 = as.Date(clock::add_years(date1, 1L)),
                        date4 = as.Date(clock::add_days(date1, 1L)),
                        date7 = as.Date(add_years(date1, m)),
                        date8 = as.Date(add_days(date1, m)))
  }

  if (dbms(con) == "spark") {
    # clock::add_years does not work on spark. must use add_days
    # https://github.com/tidyverse/dbplyr/pull/1511 fixes it
    # for now we can always use add_days
    df <- dplyr::mutate(df, date3 = as.Date(clock::add_days(date1, 1L*365L)))
  }

  if (!(dbms(con) %in% c("duckdb", "redshift"))) {
    df <- dplyr::mutate(df,
                        date5 = date_build(2020L, 1L, 1L),
                        date6 = date_build(y, m, d))
  }

  df <- dplyr::collect(df) %>%
    dplyr::mutate_if(~class(.) == "integer64", as.integer)

  expect_equal(unique(df$y2), 2000)
  expect_equal(unique(df$m2), 12)
  expect_equal(unique(df$d2), 1)

  # some of these translations are failing on certain database platforms
  if (!(dbms(con) %in% c("duckdb"))) {
    expect_equal(sort(df$dif_days), sort(c(365, 366, 364, 31))) # row order is not preserved on snowflake
    expect_equal(unique(df$date3), as.Date("2001-12-01"))
    expect_equal(unique(df$date4), as.Date("2000-12-02"))
    expect_equal(unique(df$date8), as.Date("2000-12-11"))
  }
  if (!(dbms(con) %in% c("spark", "duckdb"))) expect_equal(unique(df$date7), as.Date("2010-12-01")) # fails on spark

  if (!(dbms(con) %in% c("duckdb", "redshift"))) {
    expect_equal(unique(df$date5), as.Date("2020-01-01"))
    expect_equal(unique(df$date6), as.Date("2000-10-11"))
  }

  DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = "tmpdate", dbms = dbms(con)))
}
# dbtype = "spark"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - date functions"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == ""))
    con <- get_connection(dbtype)
    skip_if(is.null(con))
    test_clock_functions(con, write_schema)
    disconnect(con)
  })
}

test_as.Date <- function(con, cdm_schema, write_schema) {

 # debugonce(cdmFromCon)
 cdm <- cdmFromCon(con,
                   cdmSchema = cdm_schema,
                   writeSchema = write_schema,
                   cdmName = "test")

  df <- cdm$person %>%
    head(1) %>%
    dplyr::mutate(string = paste(
      as.character(.data$year_of_birth),
      as.character(.data$month_of_birth),
      as.character(.data$day_of_birth), sep = "-")) %>%
    dplyr::transmute(date1 = as.Date("2020-01-01"),
                     date2 = as.Date(.data$string)) %>%
    dplyr::collect()

  expect_equal(df$date1, as.Date("2020-01-01"))
  expect_s3_class(df$date2, "Date")
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - date functions"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(cdm_schema == "", write_schema == ""))
    con <- get_connection(dbtype)
    skip_if(is.null(con))
    test_as.Date(con, cdm_schema, write_schema)
    disconnect(con)
  })
}







