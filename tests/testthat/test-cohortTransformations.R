test_intesect <- function(con, write_schema) {

  ## 1 cohort id, 1 person
  cohort_input <- dplyr::tibble(
    cohort_definition_id = 1,
    subject_id = 3,
    cohort_start_date = as.Date(
      c("2000-01-11", "2000-01-11", "2000-02-21",
        "2000-04-04", "2000-05-16", "2000-07-18",
        "2000-08-23", "2000-10-05", "2000-11-10")
    ),
    cohort_end_date   = as.Date(
      c("2000-02-21", "2000-02-21", "2000-04-04",
        "2000-05-15", "2000-06-26", "2000-08-28",
        "2000-10-03", "2000-11-15", "2000-12-21")
    )
  )

  expected_output <- tibble(
    cohort_definition_id = 1,
    subject_id = 3,
    cohort_start_date = as.Date(c("2000-01-11", "2000-05-16", "2000-07-18",
                                  "2000-10-05")),
    cohort_end_date   = as.Date(c("2000-05-15", "2000-06-26", "2000-10-03",
                                  "2000-12-21"))
  )

  if (dbms(con) == "oracle") {

    # TODO there is an issue inserting dates into an Oracle database
    cohort_input_oracle <- cohort_input %>%
      dplyr::mutate(dplyr::across(dplyr::matches("date"), as.character))

    DBI::dbWriteTable(con, "tmp_cohort_collapse_input", cohort_input_oracle,
                      temporary = TRUE, overwrite = TRUE)

    input_db <- tbl(con, "tmp_cohort_collapse_input") %>%
      mutate(cohort_start_date = TO_DATE(cohort_start_date, "YYYY-MM-DD")) %>%
      mutate(cohort_start_date = TO_DATE(cohort_end_date, "YYYY-MM-DD")) %>%
      compute_query()

  } else {
    DBI::dbWriteTable(con, inSchema(write_schema, "tmp_cohort_collapse_input",
                                    dbms = dbms(con)), cohort_input, overwrite = TRUE)
    input_db <- dplyr::tbl(con, inSchema(write_schema, "tmp_cohort_collapse_input", dbms = dbms(con)))
  }


  cohort_collect(input_db)

  x %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      .data$cohort_definition_id,
      .data$subject_id,
      start_date = dbplyr::win_over(sql("min(cohort_start_date)"), partition = c("cohort_definition_id", "subject_id", "cohort_end_date"),   con = x$src$con),
      end_date   = dbplyr::win_over(sql("max(cohort_end_date)"),   partition = c("cohort_definition_id", "subject_id", "cohort_start_date"), con = x$src$con),
      prev_start = dbplyr::win_over(sql("min(cohort_start_date)"), partition = c("cohort_definition_id", "subject_id"), frame = c(-Inf, -1), order = "cohort_start_date", con = x$src$con),
      prev_end   = dbplyr::win_over(sql("max(cohort_end_date)"),   partition = c("cohort_definition_id", "subject_id"), frame = c(-Inf, -1), order = "cohort_start_date", con = x$src$con)) %>%
    distinct() %>%
    group_by(.data$cohort_definition_id) %>%
    dplyr::mutate(cohort_start_date = dplyr::case_when(
      !is.na(prev_start) & between(start_date, prev_start, prev_end) ~ prev_start,
      TRUE ~ start_date)) %>%
    dplyr::group_by(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date) %>%
    dplyr::summarise(cohort_end_date = max(end_date, na.rm = TRUE), .groups = "drop") %>%
    dplyr::select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %>%
    dplyr::distinct()


  expect_identical(cohort_collapse(cohort_input) %>%
                     dplyr::collect(),
                   expected_output)
}
