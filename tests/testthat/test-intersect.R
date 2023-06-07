
test_intesect <- function(con, write_schema) {
  # test every case (Allen's interval algebra) for two intervals and two people
  require(dplyr)
  intervals <- tibble::tribble(
    ~relationship,  ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    "reference",    1,                     1,           "2022-01-05",       "2022-01-10",
    "precedes",     2,                     1,           "2022-01-01",       "2022-01-03",
    "meets",        3,                     1,           "2022-01-01",       "2022-01-04",
    "overlaps",     5,                     1,           "2022-01-01",       "2022-01-05",
    "finished_by",  7,                     1,           "2022-01-01",       "2022-01-10",
    "contains",     8,                     1,           "2022-01-01",       "2022-01-15",
    "starts",       9,                     1,           "2022-01-05",       "2022-01-07",
    "equals",      10,                     1,           "2022-01-05",       "2022-01-10",
    "reference",    1,                     2,           "2022-01-05",       "2022-01-10",
    "precedes",     2,                     2,           "2022-01-01",       "2022-01-03",
    "meets",        3,                     2,           "2022-01-01",       "2022-01-04",
    "overlaps",     5,                     2,           "2022-01-01",       "2022-01-05",
    "finished_by",  7,                     2,           "2022-01-01",       "2022-01-10",
    "contains",     8,                     2,           "2022-01-01",       "2022-01-15",
    "starts",       9,                     2,           "2022-01-05",       "2022-01-07",
    "equals",      10,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  DBI::dbWriteTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)), intervals, overwrite = TRUE)

  db <- dplyr::tbl(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))

  # precedes
  x <- db %>%
    filter(relationship %in% c("reference", "precedes")) %>%
    intersect_cohorts() %>%
    collect()

  expect_equal(names(x), c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"))
  expect_equal(nrow(x), 0)

  # meets
  x <- db %>%
    filter(relationship %in% c("reference", "meets")) %>%
    intersect_cohorts() %>%
    collect()

  expect_equal(nrow(x), 0)

  # overlaps
  x <- db %>%
    filter(relationship %in% c("reference", "overlaps")) %>%
    intersect_cohorts() %>%
    collect() %>%
    arrange(subject_id)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-05",
    1,                     2,           "2022-01-05",       "2022-01-05") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)


  # finished_by
  x <- db %>%
    filter(relationship %in% c("reference", "finished_by")) %>%
    intersect_cohorts() %>%
    collect() %>%
    arrange(subject_id)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-10",
    1,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  # contains
  x <- db %>%
    filter(relationship %in% c("reference", "contains")) %>%
    intersect_cohorts() %>%
    collect() %>%
    arrange(subject_id)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-10",
    1,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  # starts
  x <- db %>%
    filter(relationship %in% c("reference", "starts")) %>%
    intersect_cohorts() %>%
    collect() %>%
    arrange(subject_id)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-07",
    1,                     2,           "2022-01-05",       "2022-01-07") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  # equals
  x <- db %>%
    filter(relationship %in% c("reference", "equals")) %>%
    intersect_cohorts() %>%
    collect() %>%
    arrange(subject_id)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-10",
    1,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  DBI::dbRemoveTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))
}

test_that("intersect - duckdb", {
  con <- DBI::dbConnect(duckdb::duckdb())
  write_schema <- "main"
  test_intesect(con, write_schema)
  DBI::dbDisconnect(con, shutdown = TRUE)
})

