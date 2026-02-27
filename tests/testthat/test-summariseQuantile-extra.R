# Tests for R/summariseQuantile.R

# --- summariseQuantile ---

test_that("summariseQuantile computes quantiles on duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_test", overwrite = TRUE, temporary = TRUE)

  result <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    summariseQuantile(mpg, probs = c(0, 0.5, 1), nameSuffix = "mpg") %>%
    dplyr::collect()

  expect_true(nrow(result) == 3) # 3 cyl groups
  expect_true("p0_mpg" %in% names(result))
  expect_true("p50_mpg" %in% names(result))
  expect_true("p100_mpg" %in% names(result))
})

test_that("summariseQuantile errors on non-tbl_sql", {
  expect_error(summariseQuantile(mtcars, mpg, probs = c(0.5)))
})

# --- summariseQuantile1 ---

test_that("summariseQuantile1 works on data.frame", {
  result <- CDMConnector:::summariseQuantile1(
    mtcars, x = "mpg", probs = c(0, 0.5, 1), nameSuffix = "{x}"
  )
  expect_true("q00_mpg" %in% names(result))
  expect_true("q50_mpg" %in% names(result))
  expect_true("q100_mpg" %in% names(result))
})

test_that("summariseQuantile1 works on duckdb tbl_sql", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_q1", overwrite = TRUE, temporary = TRUE)

  result <- CDMConnector:::summariseQuantile1(
    mtcars_tbl, x = "mpg", probs = c(0, 0.5, 1), nameSuffix = "{x}"
  ) %>% dplyr::collect()

  expect_true("q00_mpg" %in% names(result))
  expect_true("q50_mpg" %in% names(result))
})

test_that("summariseQuantile1 errors on non-tbl and non-data.frame", {
  expect_error(CDMConnector:::summariseQuantile1(
    "not a table", x = "mpg", probs = c(0.5)
  ))
})

test_that("summariseQuantile1 with grouping on data.frame", {
  result <- CDMConnector:::summariseQuantile1(
    dplyr::group_by(mtcars, cyl),
    x = "mpg", probs = c(0.25, 0.75), nameSuffix = "{x}"
  )
  expect_equal(nrow(result), 3) # 3 groups
  expect_true("q25_mpg" %in% names(result))
  expect_true("q75_mpg" %in% names(result))
})

test_that("summariseQuantile1 with grouping on duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_q1g", overwrite = TRUE, temporary = TRUE)

  result <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    CDMConnector:::summariseQuantile1(x = "mpg", probs = c(0.25, 0.75), nameSuffix = "{x}") %>%
    dplyr::collect()

  expect_equal(nrow(result), 3)
})

# --- summariseQuantile2 ---

test_that("summariseQuantile2 single variable on duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_q2", overwrite = TRUE, temporary = TRUE)

  result <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    summariseQuantile2("mpg", probs = c(0.25, 0.75), nameSuffix = "{x}_q") %>%
    dplyr::collect()

  expect_equal(nrow(result), 3)
  expect_true("q25_mpg_q" %in% names(result))
  expect_true("q75_mpg_q" %in% names(result))
})

test_that("summariseQuantile2 multiple variables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_q2m", overwrite = TRUE, temporary = TRUE)

  result <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    summariseQuantile2(c("mpg", "hp"), probs = c(0.5), nameSuffix = "{x}") %>%
    dplyr::collect()

  expect_equal(nrow(result), 3)
  expect_true("q50_mpg" %in% names(result))
  expect_true("q50_hp" %in% names(result))
})

test_that("summariseQuantile2 errors on non-tbl and non-data.frame", {
  expect_error(summariseQuantile2("not_a_table", "mpg", probs = 0.5))
})

test_that("summariseQuantile2 errors on multi-var without {x}", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_q2e", overwrite = TRUE, temporary = TRUE)

  expect_error(
    summariseQuantile2(mtcars_tbl, c("mpg", "hp"), probs = 0.5, nameSuffix = "val"),
    "\\{x\\}"
  )
})

test_that("summariseQuantile2 on data.frame", {
  result <- summariseQuantile2(
    dplyr::group_by(mtcars, cyl),
    "mpg", probs = c(0.25, 0.75), nameSuffix = "{x}"
  )
  expect_equal(nrow(result), 3)
})

test_that("summariseQuantile2 without grouping uses cross_join", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "mtcars_q2u", overwrite = TRUE, temporary = TRUE)

  result <- mtcars_tbl %>%
    summariseQuantile2(c("mpg", "hp"), probs = c(0.5), nameSuffix = "{x}") %>%
    dplyr::collect()

  expect_equal(nrow(result), 1)
  expect_true("q50_mpg" %in% names(result))
  expect_true("q50_hp" %in% names(result))
})


# --- summariseQuantile1 with data.frame ---

test_that("summariseQuantile1 works with data.frame", {
  df <- data.frame(
    group = c("A", "A", "A", "B", "B"),
    value = c(10, 20, 30, 40, 50)
  )
  df_grouped <- dplyr::group_by(df, group)
  result <- CDMConnector:::summariseQuantile1(df_grouped, x = "value", probs = c(0.5))
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true(any(grepl("q50", names(result))))
})

test_that("summariseQuantile1 works with multiple quantiles", {
  df <- data.frame(
    group = c("A", "A", "A", "A"),
    value = c(10, 20, 30, 40)
  )
  df_grouped <- dplyr::group_by(df, group)
  result <- CDMConnector:::summariseQuantile1(df_grouped, x = "value", probs = c(0.25, 0.5, 0.75))
  expect_true(any(grepl("q25", names(result))))
  expect_true(any(grepl("q50", names(result))))
  expect_true(any(grepl("q75", names(result))))
})

test_that("summariseQuantile1 respects nameSuffix", {
  df <- data.frame(value = c(1, 2, 3))
  result <- CDMConnector:::summariseQuantile1(df, x = "value", probs = c(0.5), nameSuffix = "custom")
  expect_true(any(grepl("custom", names(result))))
})

test_that("summariseQuantile1 errors on non-data.frame/non-tbl_sql", {
  expect_error(
    CDMConnector:::summariseQuantile1("not_data", x = "val", probs = 0.5),
    "tbl_sql or a data.frame"
  )
})

# --- summariseQuantile1 with tbl_sql (duckdb) ---

test_that("summariseQuantile1 works with duckdb tbl_sql", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(
    group = c("A", "A", "A", "B", "B"),
    value = c(10L, 20L, 30L, 40L, 50L)
  )
  DBI::dbWriteTable(con, "test_quant", df)
  tbl_ref <- dplyr::tbl(con, "test_quant") %>% dplyr::group_by(group)

  result <- CDMConnector:::summariseQuantile1(tbl_ref, x = "value", probs = c(0.5)) %>%
    dplyr::collect()
  expect_true(nrow(result) > 0)
  expect_true(any(grepl("q50", names(result))))
})

test_that("summariseQuantile1 works with ungrouped duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(value = c(10L, 20L, 30L, 40L, 50L))
  DBI::dbWriteTable(con, "test_quant", df)
  tbl_ref <- dplyr::tbl(con, "test_quant")

  result <- CDMConnector:::summariseQuantile1(tbl_ref, x = "value", probs = c(0.25, 0.5, 0.75)) %>%
    dplyr::collect()
  expect_true(nrow(result) == 1)
  expect_true(any(grepl("q25", names(result))))
  expect_true(any(grepl("q75", names(result))))
})

# --- summariseQuantile2 ---

test_that("summariseQuantile2 works with duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(
    group = c("A", "A", "A", "B", "B"),
    value = c(10L, 20L, 30L, 40L, 50L)
  )
  DBI::dbWriteTable(con, "test_quant", df)
  tbl_ref <- dplyr::tbl(con, "test_quant") %>% dplyr::group_by(group)

  result <- summariseQuantile2(tbl_ref, x = "value", probs = c(0.5)) %>%
    dplyr::collect()
  expect_true(nrow(result) > 0)
})


# --- summariseQuantile1 with data.frame ---

test_that("summariseQuantile1 works with data.frame", {
  df <- data.frame(
    group = c("A", "A", "A", "B", "B"),
    value = c(10, 20, 30, 40, 50)
  )
  df_grouped <- dplyr::group_by(df, group)
  result <- CDMConnector:::summariseQuantile1(df_grouped, x = "value", probs = c(0.5))
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true(any(grepl("q50", names(result))))
})

test_that("summariseQuantile1 works with multiple quantiles", {
  df <- data.frame(
    group = c("A", "A", "A", "A"),
    value = c(10, 20, 30, 40)
  )
  df_grouped <- dplyr::group_by(df, group)
  result <- CDMConnector:::summariseQuantile1(df_grouped, x = "value", probs = c(0.25, 0.5, 0.75))
  expect_true(any(grepl("q25", names(result))))
  expect_true(any(grepl("q50", names(result))))
  expect_true(any(grepl("q75", names(result))))
})

test_that("summariseQuantile1 respects nameSuffix", {
  df <- data.frame(value = c(1, 2, 3))
  result <- CDMConnector:::summariseQuantile1(df, x = "value", probs = c(0.5), nameSuffix = "custom")
  expect_true(any(grepl("custom", names(result))))
})

test_that("summariseQuantile1 errors on non-data.frame/non-tbl_sql", {
  expect_error(
    CDMConnector:::summariseQuantile1("not_data", x = "val", probs = 0.5),
    "tbl_sql or a data.frame"
  )
})

# --- summariseQuantile1 with tbl_sql (duckdb) ---

test_that("summariseQuantile1 works with duckdb tbl_sql", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(
    group = c("A", "A", "A", "B", "B"),
    value = c(10L, 20L, 30L, 40L, 50L)
  )
  DBI::dbWriteTable(con, "test_quant", df)
  tbl_ref <- dplyr::tbl(con, "test_quant") %>% dplyr::group_by(group)

  result <- CDMConnector:::summariseQuantile1(tbl_ref, x = "value", probs = c(0.5)) %>%
    dplyr::collect()
  expect_true(nrow(result) > 0)
  expect_true(any(grepl("q50", names(result))))
})

test_that("summariseQuantile1 works with ungrouped duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(value = c(10L, 20L, 30L, 40L, 50L))
  DBI::dbWriteTable(con, "test_quant", df)
  tbl_ref <- dplyr::tbl(con, "test_quant")

  result <- CDMConnector:::summariseQuantile1(tbl_ref, x = "value", probs = c(0.25, 0.5, 0.75)) %>%
    dplyr::collect()
  expect_true(nrow(result) == 1)
  expect_true(any(grepl("q25", names(result))))
  expect_true(any(grepl("q75", names(result))))
})

# --- summariseQuantile2 ---

test_that("summariseQuantile2 works with duckdb", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  df <- data.frame(
    group = c("A", "A", "A", "B", "B"),
    value = c(10L, 20L, 30L, 40L, 50L)
  )
  DBI::dbWriteTable(con, "test_quant", df)
  tbl_ref <- dplyr::tbl(con, "test_quant") %>% dplyr::group_by(group)

  result <- summariseQuantile2(tbl_ref, x = "value", probs = c(0.5)) %>%
    dplyr::collect()
  expect_true(nrow(result) > 0)
})
