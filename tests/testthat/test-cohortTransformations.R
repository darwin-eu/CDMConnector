test_cohort_collapse <- function(con, write_schema) {

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
  ) %>%
   dplyr::union_all(
      dplyr::tibble(
        cohort_definition_id = 1,
        subject_id = 4,
        cohort_start_date = as.Date(
          c("2000-01-11", "2000-01-11", "2000-02-21",
            "2000-04-04", "2000-05-16", "2000-07-18",
            "2000-08-23", "2000-10-05", "2000-11-10")
        ),
        cohort_end_date   = as.Date(
          c("2000-12-21", "2000-02-21", "2000-04-04",
            "2000-05-15", "2000-06-26", "2000-08-28",
            "2000-10-03", "2000-11-15", "2000-12-21")
        )
      )
    ) %>%
   dplyr::union_all(
      dplyr::tibble(
        cohort_definition_id = 2,
        subject_id = 3,
        cohort_start_date = as.Date(
          c("2001-08-30", "2001-09-27", "2001-10-22",
            "2001-11-21", "2001-12-12", "2002-08-27",
            "2002-09-23", "2002-10-16", "2002-11-19")
        ),
        cohort_end_date   = as.Date(
          c("2001-09-28", "2001-10-26", "2001-11-20",
            "2001-12-20", "2002-01-10", "2002-09-25",
            "2002-10-22", "2002-11-14", "2002-12-18")
        )
      )
    )


  expected_output <- tibble(
    cohort_definition_id = 1,
    subject_id = 3,
    cohort_start_date = as.Date(c("2000-01-11", "2000-05-16", "2000-07-18",
                                  "2000-10-05")
    ),
    cohort_end_date   = as.Date(c("2000-05-15", "2000-06-26", "2000-10-03",
                                  "2000-12-21")
    )
  )%>%
   dplyr::union_all(
      dplyr::tibble(
        cohort_definition_id = 1,
        subject_id = 4,
        cohort_start_date = as.Date(
          c("2000-01-11")
        ),
        cohort_end_date   = as.Date(
          c("2000-12-21")
        )
      )
    ) %>%
    dplyr::union_all(
      dplyr::tibble(
        cohort_definition_id = 2,
        subject_id = 3,
        cohort_start_date = as.Date(
          c("2001-08-30", "2001-11-21", "2002-08-27",
            "2002-11-19")
        ),
        cohort_end_date   = as.Date(
          c("2001-11-20", "2002-01-10", "2002-11-14",
            "2002-12-18")
        )
      )
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


  expect_identical(cohort_collapse(input_db) %>%
                     dplyr::arrange(cohort_definition_id, subject_id, cohort_start_date) %>%
                     dplyr::collect(),
                   expected_output %>%
                     dplyr::arrange(cohort_definition_id, subject_id, cohort_start_date))
}
