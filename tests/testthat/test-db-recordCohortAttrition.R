
test_record_cohort_attrition <- function(con, cdm_schema, write_schema) {


  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema) %>%
    generateConceptCohortSet(conceptSet = list(pharyngitis = 4112343),
                             name = "new_cohort",
                             overwrite = TRUE)

  oldAttrition <- cohortAttrition(cdm$new_cohort)
  oldCounts <- cohortCount(cdm$new_cohort)
  expect_true(nrow(cohortSet(cdm$new_cohort)) == 1)

  expect_no_error(cdm$new_cohort <- recordCohortAttrition(cdm$new_cohort, reason = "a reason"))
  # running again will produce an error if no new reason is given
  expect_error(cdm$new_cohort <- updateCohortAttributes(cdm$new_cohort))

  expect_s3_class(cohortAttrition(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortCount(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortSet(cdm$new_cohort), "data.frame")
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")
  expect_equal(nrow(cohortAttrition(cdm$new_cohort)), 2)
  expect_equal(cohortCount(cdm$new_cohort), oldCounts)

  cdm$new_cohort <- cdm$new_cohort %>%
    dplyr::filter(cohort_start_date >= as.Date("2010-01-01"))

  expect_no_error({
    cdm$new_cohort <- recordCohortAttrition(
      cohort = cdm$new_cohort,
      reason = "Only events after 2010")
  })

  expect_s3_class(cohortAttrition(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortCount(cdm$new_cohort), "data.frame")
  expect_s3_class(cohortSet(cdm$new_cohort), "data.frame")
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")

  expect_true(nrow(cohortAttrition(cdm$new_cohort)) == 3)
  expect_true(nrow(cohortCount(cdm$new_cohort)) == 1)
}


# dbtype = "duckdb"
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
