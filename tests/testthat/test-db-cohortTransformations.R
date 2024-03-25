test_cohort_collapse <- function(con, write_schema) {

  # Nuria's examples
  cohort_input <- tibble::tribble(
  ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
                      1,           3,       "2000-01-11",     "2000-02-21",
                      1,           3,       "2000-01-11",     "2000-02-21",
                      1,           3,       "2000-02-21",     "2000-04-04",
                      1,           3,       "2000-04-04",     "2000-05-15",
                      1,           3,       "2000-05-16",     "2000-06-26",
                      1,           3,       "2000-07-18",     "2000-08-28",
                      1,           3,       "2000-08-23",     "2000-10-03",
                      1,           3,       "2000-10-05",     "2000-11-15",
                      1,           3,       "2000-11-10",     "2000-12-21",
                      1,           4,       "2000-01-11",     "2000-12-21",
                      1,           4,       "2000-01-11",     "2000-02-21",
                      1,           4,       "2000-02-21",     "2000-04-04",
                      1,           4,       "2000-04-04",     "2000-05-15",
                      1,           4,       "2000-05-16",     "2000-06-26",
                      1,           4,       "2000-07-18",     "2000-08-28",
                      1,           4,       "2000-08-23",     "2000-10-03",
                      1,           4,       "2000-10-05",     "2000-11-15",
                      1,           4,       "2000-11-10",     "2000-12-21",
                      2,           3,       "2001-08-30",     "2001-09-28",
                      2,           3,       "2001-09-27",     "2001-10-26",
                      2,           3,       "2001-10-22",     "2001-11-20",
                      2,           3,       "2001-11-21",     "2001-12-20",
                      2,           3,       "2001-12-12",     "2002-01-10",
                      2,           3,       "2002-08-27",     "2002-09-25",
                      2,           3,       "2002-09-23",     "2002-10-22",
                      2,           3,       "2002-10-16",     "2002-11-14",
                      2,           3,       "2002-11-19",     "2002-12-18"
  ) %>% dplyr::mutate(dplyr::across(dplyr::matches("_date"), as.Date))

  expected_output <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
                        1,           3,       "2000-01-11",     "2000-05-15",
                        1,           3,       "2000-05-16",     "2000-06-26",
                        1,           3,       "2000-07-18",     "2000-10-03",
                        1,           3,       "2000-10-05",     "2000-12-21",
                        1,           4,       "2000-01-11",     "2000-12-21",
                        2,           3,       "2001-08-30",     "2001-11-20",
                        2,           3,       "2001-11-21",     "2002-01-10",
                        2,           3,       "2002-08-27",     "2002-11-14",
                        2,           3,       "2002-11-19",     "2002-12-18"
    ) %>% dplyr::mutate(dplyr::across(dplyr::matches("_date"), as.Date)) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date)

  if (dbms(con) == "oracle") {

    # TODO there is an issue inserting dates into an Oracle database
    cohort_input_oracle <- cohort_input %>%
      dplyr::mutate(dplyr::across(dplyr::matches("date"), as.character))

    DBI::dbWriteTable(con,
                      inSchema(write_schema, "tmp_cohort_collapse_input0"),
                      cohort_input_oracle,
                      overwrite = TRUE)

    input_db <- dplyr::tbl(con, inSchema(write_schema, "tmp_cohort_collapse_input0", dbms = dbms(con))) %>%
      dplyr::mutate(cohort_start_date = TO_DATE(cohort_start_date, "YYYY-MM-DD"),
                    cohort_end_date = TO_DATE(cohort_end_date, "YYYY-MM-DD")) %>%
      compute_query(name = "tmp_cohort_collapse_input",
                    temporary = FALSE,
                    schema = write_schema,
                    overwrite = TRUE)

    DBI::dbRemoveTable(con, inSchema(write_schema, "tmp_cohort_collapse_input0", dbms = dbms(con)))
  } else {
    DBI::dbWriteTable(con, inSchema(write_schema, "tmp_cohort_collapse_input", dbms = dbms(con)), cohort_input, overwrite = TRUE)
    input_db <- dplyr::tbl(con, inSchema(write_schema, "tmp_cohort_collapse_input", dbms = dbms(con)))
  }

  if (dbms(con) == "snowflake") {
    DBI::dbExecute(con,
                   glue::glue_sql("USE SCHEMA ATLAS.RESULTS"))
  }

  actual_output <- input_db %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::tibble() %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date) %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date)) %>% # oracle returns datetimes
    dplyr::mutate(dplyr::across(dplyr::matches("id"), as.double)) # bigquery returns integers

  expect_identical(actual_output, expected_output)

  DBI::dbRemoveTable(con, inSchema(write_schema, "tmp_cohort_collapse_input", dbms = dbms(con)))


  # test every case (Allen's interval algebra) for two intervals and two people
  intervals <- tibble::tribble(
    ~relationship,  ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    "reference",    1,                     1,           "2022-01-05",       "2022-01-10",
    "precedes",     1,                     1,           "2022-01-01",       "2022-01-03",
    "meets",        1,                     1,           "2022-01-01",       "2022-01-04",
    "overlaps",     1,                     1,           "2022-01-01",       "2022-01-05",
    "finished_by",  1,                     1,           "2022-01-01",       "2022-01-10",
    "contains",     1,                     1,           "2022-01-01",       "2022-01-15",
    "starts",       1,                     1,           "2022-01-05",       "2022-01-07",
    "equals",       1,                     1,           "2022-01-05",       "2022-01-10",
    "reference",    1,                     2,           "2022-01-05",       "2022-01-10",
    "precedes",     1,                     2,           "2022-01-01",       "2022-01-03",
    "meets",        1,                     2,           "2022-01-01",       "2022-01-04",
    "overlaps",     1,                     2,           "2022-01-01",       "2022-01-05",
    "finished_by",  1,                     2,           "2022-01-01",       "2022-01-10",
    "contains",     1,                     2,           "2022-01-01",       "2022-01-15",
    "starts",       1,                     2,           "2022-01-05",       "2022-01-07",
    "equals",       1,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  if (dbms(con) == "oracle") {
    # TODO there is an issue inserting dates into an Oracle database
    df <- intervals %>%
      dplyr::mutate(dplyr::across(dplyr::matches("date"), as.character))

    nm <- unique_table_name()
    DBI::dbWriteTable(con, nm, df, temporary = TRUE)

    db <- tbl(con, nm) %>%
      dplyr::mutate(cohort_start_date = TO_DATE(cohort_start_date, "YYYY-MM-DD")) %>%
      dplyr::mutate(cohort_end_date   = TO_DATE(cohort_end_date, "YYYY-MM-DD")) %>%
      compute_query()
  } else {
    DBI::dbWriteTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)), intervals, overwrite = TRUE)
    db <- dplyr::tbl(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))
  }

  # precedes
  x <- db %>%
    dplyr::filter(relationship %in% c("reference", "precedes")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expected <- db %>%
    dplyr::filter(relationship %in% c("reference", "precedes")) %>%
    dplyr::select(-"relationship") %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)


  expect_equal(colnames(x), c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"))
  expect_equal(x, expected)

  # meets
  x <- db %>%
    dplyr::filter(.data$relationship %in% c("reference", "meets")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expected <- db %>%
    dplyr::filter(relationship %in% c("reference", "meets")) %>%
    dplyr::select(-"relationship") %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expect_equal(x, expected)

  # overlaps
  x <- db %>%
    dplyr::filter(relationship %in% c("reference", "overlaps")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-01",       "2022-01-10",
    1,                     2,           "2022-01-01",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)


  # finished_by
  x <- db %>%
    filter(relationship %in% c("reference", "finished_by")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-01",       "2022-01-10",
    1,                     2,           "2022-01-01",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  # contains
  x <- db %>%
    filter(relationship %in% c("reference", "contains")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-01",       "2022-01-15",
    1,                     2,           "2022-01-01",       "2022-01-15") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  # starts
  x <- db %>%
    filter(relationship %in% c("reference", "starts")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-10",
    1,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  # equals
  x <- db %>%
    filter(relationship %in% c("reference", "equals")) %>%
    dplyr::select(-"relationship") %>%
    cohort_collapse() %>%
    dplyr::collect() %>%
    dplyr::arrange(subject_id)

  expected <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1,                     1,           "2022-01-05",       "2022-01-10",
    1,                     2,           "2022-01-05",       "2022-01-10") %>%
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

  expect_equal(x, expected)

  DBI::dbRemoveTable(con, inSchema(write_schema, "tmp_intervals", dbms = dbms(con)))
}

# dbtype = "duckdb"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - cohort_collapse"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    write_schema <- get_write_schema(dbtype)
    con <- get_connection(dbtype)
    skip_if(any(write_schema == "") || is.null(con))
    test_cohort_collapse(con, write_schema)
    disconnect(con)
  })
}

# TODO pmin and pmax do not work on sqlserver - add issue to dbplyr
