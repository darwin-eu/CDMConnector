test_new_generated_cohort_set <- function(con, cdm_schema, write_schema) {

  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)

  x <- cdm$condition_occurrence %>%
    filter(condition_concept_id == 4112343) %>%
    mutate(cohort_definition_id = 1) %>%
    select(
      cohort_definition_id, subject_id = person_id,
      cohort_start_date = condition_start_date,
      cohort_end_date = condition_start_date
    ) %>%
    distinct()

  expect_no_error(
    cdm$new_cohort <- x %>%
      newGeneratedCohortSet(
        cohortSetRef = tibble(
          cohort_definition_id = 1, cohort_name = "pharyngitis"
        ),
        writeSchema = attr(cdm, "write_schema"),
        overwrite = TRUE
      )
  )
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")
  expect_true(all(
    c(
      "cdm_reference", "cohort_set", "cohort_attrition", "cohort_count",
      "tbl_name"
    ) %in%
    names(attributes(cdm$new_cohort))
  ))

  expect_no_error(
    cdm$new_cohort <- x %>%
      computeQuery() %>%
      newGeneratedCohortSet(
        cohortSetRef = tibble(
          cohort_definition_id = 1, cohort_name = "pharyngitis"
        ),
        writeSchema = attr(cdm, "write_schema"),
        overwrite = TRUE
      )
  )
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")
  expect_true(all(
    c(
      "cdm_reference", "cohort_set", "cohort_attrition", "cohort_count",
      "tbl_name"
    ) %in%
      names(attributes(cdm$new_cohort))
  ))

  expect_no_error(
    cdm$new_cohort <- x %>%
      computeQuery(
        name = "new_cohort", temporary = FALSE,
        schema = attr(cdm, "write_schema"), overwrite = TRUE
      ) %>%
      newGeneratedCohortSet(
        cohortSetRef = tibble(
          cohort_definition_id = 1, cohort_name = "pharyngitis"
        ),
        writeSchema = attr(cdm, "write_schema"),
        overwrite = TRUE
      )
  )
  expect_s3_class(cdm$new_cohort, "GeneratedCohortSet")
  expect_true(all(
    c(
      "cdm_reference", "cohort_set", "cohort_attrition", "cohort_count",
      "tbl_name"
    ) %in%
      names(attributes(cdm$new_cohort))
  ))
  expect_true(attr(cdm$new_cohort, "tbl_name") == "new_cohort")

}


# dbtype = "duckdb"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - recordCohortAttrition"), {
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_new_generated_cohort_set(con, cdm_schema, write_schema)
    #test_new_generated_cohort_set(con, cdm_schema, c(schema = write_schema, prefix = "cdmc_"))
    disconnect(con)
  })
}
