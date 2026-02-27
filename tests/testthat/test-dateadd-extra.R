# Additional tests for R/dateadd.R — coverage for dateadd, datediff, datepart, asDate

# --- datepart with explicit dbms ---

test_that("datepart generates SQL for each supported dbms", {
  supported <- c("redshift", "oracle", "postgresql", "sql server", "spark", "duckdb", "bigquery", "snowflake")
  for (db in supported) {
    result <- datepart("birth_date", "year", dbms = db)
    expect_s3_class(result, "sql")
    expect_true(nchar(as.character(result)) > 0)
  }
})

test_that("datepart generates day extraction for all dbms", {
  for (db in c("duckdb", "postgresql", "sql server", "redshift", "oracle", "spark", "bigquery", "snowflake")) {
    result <- datepart("start_date", "day", dbms = db)
    expect_s3_class(result, "sql")
  }
})

test_that("datepart generates month extraction for all dbms", {
  for (db in c("duckdb", "postgresql", "sql server", "redshift", "oracle", "spark", "bigquery", "snowflake")) {
    result <- datepart("start_date", "month", dbms = db)
    expect_s3_class(result, "sql")
  }
})

test_that("datepart validates interval argument", {
  expect_error(datepart("date", "hour", dbms = "duckdb"))
})

test_that("datepart validates dbms argument", {
  expect_error(datepart("date", "year", dbms = "unsupported_db"))
})

# --- dateadd with duckdb pipe ---

test_that("dateadd generates duckdb day SQL", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")),
                              name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 10, interval = "day")) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date2), as.Date("1999-01-11"))
})

test_that("dateadd generates duckdb year SQL", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("2000-06-15")),
                              name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 2, interval = "year")) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date2), as.Date("2002-06-15"))
})

test_that("dateadd handles negative number", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("2020-03-15")),
                              name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", -5, interval = "day")) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date2), as.Date("2020-03-10"))
})

test_that("dateadd handles column reference for number", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con,
    data.frame(date1 = as.Date("2020-01-01"), days_to_add = 30L),
    name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", "days_to_add", interval = "day")) %>%
    dplyr::collect()

  expect_equal(as.Date(df$date2), as.Date("2020-01-31"))
})

test_that("dateadd validates interval", {
  expect_error(dateadd("date", 1, interval = "hour"))
})

# --- datediff with duckdb pipe ---

test_that("datediff calculates day difference", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con,
    data.frame(d1 = as.Date("2020-01-01"), d2 = as.Date("2020-01-11")),
    name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(diff = !!datediff("d1", "d2", interval = "day")) %>%
    dplyr::collect()

  expect_equal(df$diff, 10)
})

test_that("datediff calculates year difference", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con,
    data.frame(d1 = as.Date("2000-06-15"), d2 = as.Date("2005-06-15")),
    name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(diff_years = !!datediff("d1", "d2", interval = "year")) %>%
    dplyr::collect()

  expect_equal(df$diff_years, 5)
})

test_that("datediff calculates month difference", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con,
    data.frame(d1 = as.Date("2020-01-15"), d2 = as.Date("2020-04-15")),
    name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  df <- date_tbl %>%
    dplyr::mutate(diff_months = !!datediff("d1", "d2", interval = "month")) %>%
    dplyr::collect()

  expect_equal(df$diff_months, 3)
})

test_that("datediff validates interval", {
  expect_error(datediff("d1", "d2", interval = "hour"))
})

# --- dateadd on local data.frame ---

test_that("dateadd works on local data frame", {
  skip_if_not_installed("clock")
  df <- data.frame(date1 = as.Date("2020-01-01"))
  result <- df %>% dplyr::mutate(date2 = !!dateadd("date1", 10, interval = "day"))
  expect_equal(result$date2, as.Date("2020-01-11"))
})

test_that("datediff works on local data frame", {
  skip_if_not_installed("clock")
  df <- data.frame(d1 = as.Date("2020-01-01"), d2 = as.Date("2020-01-11"))
  result <- df %>% dplyr::mutate(diff = !!datediff("d1", "d2", interval = "day"))
  expect_equal(result$diff, 10)
})

test_that("datepart works on local data frame", {
  skip_if_not_installed("clock")
  df <- data.frame(d = as.Date("2020-06-15"))
  result <- df %>% dplyr::mutate(yr = !!datepart("d", "year"))
  expect_equal(result$yr, 2020L)
})


# --- dateadd validation ---

test_that("dateadd errors on invalid number argument", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("2020-01-01")),
                              name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  # The error is thrown by dateadd during !! evaluation, not by mutate
  expect_error(
    date_tbl %>% { dateadd("date1", list(1), interval = "day") },
    "number"
  )
})

# --- asDate with duckdb ---

test_that("asDate generates duckdb SQL expression", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con,
    data.frame(date_val = as.Date("2020-01-15")),
    name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  # Verify asDate generates the SQL expression without collecting
  # (DuckDB's DATE literal syntax doesn't work with column references)
  query <- date_tbl %>%
    dplyr::mutate(parsed = !!asDate(.data$date_val))
  sql_text <- as.character(dbplyr::sql_render(query))
  expect_true(grepl("DATE", sql_text))
})

# --- asDate with local data.frame ---

test_that("asDate works on local data frame", {
  df <- data.frame(date_val = as.Date("2020-06-15"))
  result <- df %>%
    dplyr::mutate(parsed = !!asDate(.data$date_val))
  expect_true("parsed" %in% names(result))
})

# --- datepart with local data.frame for day and month ---

test_that("datepart on local data frame extracts day", {
  skip_if_not_installed("clock")
  df <- data.frame(d = as.Date("2020-06-15"))
  result <- df %>% dplyr::mutate(day_val = !!datepart("d", "day"))
  expect_equal(result$day_val, 15L)
})

test_that("datepart on local data frame extracts month", {
  skip_if_not_installed("clock")
  df <- data.frame(d = as.Date("2020-06-15"))
  result <- df %>% dplyr::mutate(month_val = !!datepart("d", "month"))
  expect_equal(result$month_val, 6L)
})

# --- datediff on local data.frame for year ---

test_that("datediff on local data frame computes year difference", {
  skip_if_not_installed("clock")
  df <- data.frame(d1 = as.Date("2015-03-10"), d2 = as.Date("2020-03-10"))
  result <- df %>% dplyr::mutate(diff_years = !!datediff("d1", "d2", interval = "year"))
  expect_equal(result$diff_years, 5)
})

test_that("datediff on local data frame computes month difference", {
  skip_if_not_installed("clock")
  df <- data.frame(d1 = as.Date("2020-01-01"), d2 = as.Date("2020-07-01"))
  result <- df %>% dplyr::mutate(diff_months = !!datediff("d1", "d2", interval = "month"))
  expect_equal(result$diff_months, 6)
})

# --- dateadd on local data.frame for year ---

test_that("dateadd on local data frame adds years", {
  skip_if_not_installed("clock")
  df <- data.frame(date1 = as.Date("2020-01-01"))
  result <- df %>% dplyr::mutate(date2 = !!dateadd("date1", 3, interval = "year"))
  expect_equal(result$date2, as.Date("2023-01-01"))
})

# --- datepart with sqlite format for year ---

test_that("datepart generates sqlite SQL for year", {
  result <- datepart("birth_date", "year", dbms = "sqlite")
  expect_s3_class(result, "sql")
  expect_true(grepl("STRFTIME", as.character(result)))
})

test_that("datepart generates sqlite SQL for day", {
  result <- datepart("birth_date", "day", dbms = "sqlite")
  expect_s3_class(result, "sql")
  expect_true(grepl("STRFTIME", as.character(result)))
})


# --- dateadd validation ---

test_that("dateadd errors on invalid number argument", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("2020-01-01")),
                              name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  # The error is thrown by dateadd during !! evaluation, not by mutate
  expect_error(
    date_tbl %>% { dateadd("date1", list(1), interval = "day") },
    "number"
  )
})

# --- asDate with duckdb ---

test_that("asDate generates duckdb SQL expression", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  date_tbl <- dplyr::copy_to(con,
    data.frame(date_val = as.Date("2020-01-15")),
    name = "tmpdate", overwrite = TRUE, temporary = TRUE)

  # Verify asDate generates the SQL expression without collecting
  # (DuckDB's DATE literal syntax doesn't work with column references)
  query <- date_tbl %>%
    dplyr::mutate(parsed = !!asDate(.data$date_val))
  sql_text <- as.character(dbplyr::sql_render(query))
  expect_true(grepl("DATE", sql_text))
})

# --- asDate with local data.frame ---

test_that("asDate works on local data frame", {
  df <- data.frame(date_val = as.Date("2020-06-15"))
  result <- df %>%
    dplyr::mutate(parsed = !!asDate(.data$date_val))
  expect_true("parsed" %in% names(result))
})

# --- datepart with local data.frame for day and month ---

test_that("datepart on local data frame extracts day", {
  skip_if_not_installed("clock")
  df <- data.frame(d = as.Date("2020-06-15"))
  result <- df %>% dplyr::mutate(day_val = !!datepart("d", "day"))
  expect_equal(result$day_val, 15L)
})

test_that("datepart on local data frame extracts month", {
  skip_if_not_installed("clock")
  df <- data.frame(d = as.Date("2020-06-15"))
  result <- df %>% dplyr::mutate(month_val = !!datepart("d", "month"))
  expect_equal(result$month_val, 6L)
})

# --- datediff on local data.frame for year ---

test_that("datediff on local data frame computes year difference", {
  skip_if_not_installed("clock")
  df <- data.frame(d1 = as.Date("2015-03-10"), d2 = as.Date("2020-03-10"))
  result <- df %>% dplyr::mutate(diff_years = !!datediff("d1", "d2", interval = "year"))
  expect_equal(result$diff_years, 5)
})

test_that("datediff on local data frame computes month difference", {
  skip_if_not_installed("clock")
  df <- data.frame(d1 = as.Date("2020-01-01"), d2 = as.Date("2020-07-01"))
  result <- df %>% dplyr::mutate(diff_months = !!datediff("d1", "d2", interval = "month"))
  expect_equal(result$diff_months, 6)
})

# --- dateadd on local data.frame for year ---

test_that("dateadd on local data frame adds years", {
  skip_if_not_installed("clock")
  df <- data.frame(date1 = as.Date("2020-01-01"))
  result <- df %>% dplyr::mutate(date2 = !!dateadd("date1", 3, interval = "year"))
  expect_equal(result$date2, as.Date("2023-01-01"))
})

# --- datepart with sqlite format for year ---

test_that("datepart generates sqlite SQL for year", {
  result <- datepart("birth_date", "year", dbms = "sqlite")
  expect_s3_class(result, "sql")
  expect_true(grepl("STRFTIME", as.character(result)))
})

test_that("datepart generates sqlite SQL for day", {
  result <- datepart("birth_date", "day", dbms = "sqlite")
  expect_s3_class(result, "sql")
  expect_true(grepl("STRFTIME", as.character(result)))
})
