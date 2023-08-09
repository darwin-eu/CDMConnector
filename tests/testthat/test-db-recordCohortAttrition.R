
test_record_cohort_attrition <- function(con, cdm_schema, write_schema) {
  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema) %>%
    generateConceptCohortSet(conceptSet = list(pharyngitis = 4112343, bronchitis = 260139),
                             name = "new_cohort",
                             overwrite = TRUE)

  oldAttrition <- cohortAttrition(cdm$new_cohort)
  oldCounts <- cohortCount(cdm$new_cohort)
  expect_true(nrow(cohortSet(cdm$new_cohort)) == 2)

  expect_no_error(cdm$new_cohort <- recordCohortAttrition(cdm$new_cohort, reason = "a reason"))
  # running again will produce an error if no new reason is given
  expect_error(cdm$new_cohort <- recordCohortAttrition(cdm$new_cohort))

  expect_s3_class(cohortAttrition(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortCount(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortSet(cdm$new_cohort), "data.frame")
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")
  expect_equal(nrow(cohortAttrition(cdm$new_cohort)), 4)
  expect_equal(cohortCount(cdm$new_cohort) %>% dplyr::arrange(.data$cohort_definition_id),
               oldCounts %>% dplyr::arrange(.data$cohort_definition_id))

  cdm$new_cohort <- cdm$new_cohort %>%
    dplyr::filter(cohort_start_date >= as.Date("2010-01-01")) %>%
    computeQuery(temporary = FALSE,
                 name = "temp_test",
                 schema = attr(cdm, "write_schema"),
                 overwrite = TRUE)

  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")

  expect_no_error({
    cdm$new_cohort <- recordCohortAttrition(
      cohort = cdm$new_cohort,
      reason = "Only events after 2010")
  })

  expect_s3_class(cohortAttrition(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortCount(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortSet(cdm$new_cohort), "data.frame")
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")

  expect_true(nrow(cohortAttrition(cdm$new_cohort)) == 6)
  expect_true(nrow(cohortCount(cdm$new_cohort)) == 2)
  oldCounts <- cohortCount(cdm$new_cohort)

  cdm$new_cohort <- cdm$new_cohort %>%
    dplyr::filter(
      cohort_definition_id != 1 | cohort_start_date <= as.Date("2015-01-01")
    )

  expect_no_error({
    cdm$new_cohort <- recordCohortAttrition(
      cohort = cdm$new_cohort,
      reason = "Only events before 2020",
      cohortId = 1)
  })

  expect_s3_class(cohortAttrition(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortCount(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortSet(cdm$new_cohort), "data.frame")
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")

  expect_true(nrow(cohortAttrition(cdm$new_cohort)) == 7)
  expect_true(nrow(cohortCount(cdm$new_cohort)) == 2)
  expect_true(
    cdm$new_cohort %>%
      cohortAttrition() %>%
      dplyr::filter(.data$cohort_definition_id == 1) %>%
      nrow() == 4
  )

  expect_true(
    cdm$new_cohort %>%
      cohortAttrition() %>%
      dplyr::filter(cohort_definition_id == 2) %>%
      nrow() == 3
  )

  expect_equal(
    cdm$new_cohort %>% cohortCount() %>% dplyr::filter(cohort_definition_id == 2),
    oldCounts %>% dplyr::filter(cohort_definition_id == 2)
  )

  cdm$new_cohort <- cdm$new_cohort %>%
    dplyr::filter(!!datepart("cohort_start_date", "month") == 1)

  expect_no_error({
    cdm$new_cohort <- recordCohortAttrition(
      cohort = cdm$new_cohort,
      reason = "Only January events")
  })

  expect_true(nrow(cohortAttrition(cdm$new_cohort)) == 9)
  expect_true(nrow(cohortCount(cdm$new_cohort)) == 2)

  expect_true(
    cdm$new_cohort %>%
      cohortAttrition() %>%
      dplyr::filter(cohort_definition_id == 1) %>%
      dplyr::pull("reason_id") %>%
      max() == 5
  )

  expect_true(
    cdm$new_cohort %>%
      cohortAttrition() %>%
      dplyr::filter(cohort_definition_id == 2) %>%
      dplyr::pull("reason_id") %>%
      max() == 4
  )
}

# dbtype = "postgres"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - recordCohortAttrition"), {
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_record_cohort_attrition(con, cdm_schema, write_schema)
    disconnect(con)
  })
}
