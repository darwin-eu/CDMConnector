# test_that("basic functionality of appendCohortAttrition", {
#   con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#
#   debugonce(generateConceptCohortSet)
#   cdm <- cdm_from_con(con = con, cdm_schema = "main", write_schema = "main")
#   cdm <- cdm_from_con(con = con, cdm_schema = "main", write_schema = "main")
#   cdm <- generateConceptCohortSet(
#     cdm = cdm, conceptSet = list(pharyngitis = 4112343), name = "new_cohort"
#   )
#
#   oldAttrition <- cohortAttrition(cdm$new_cohort)
#   oldCounts <- cohortCount(cdm$new_cohort)
#   attr(cdm$new_cohort, "cohort_attrition") <- NULL
#   attr(cdm$new_cohort, "cohort_count") <- NULL
#
#   expect_no_error(cdm$new_cohort <- appendCohortAttributes(cdm$new_cohort))
#
#   expect_true("GeneratedCohortSet" %in% class(cdm$new_cohort))
#   expect_equal(cohortAttrition(cdm$new_cohort), oldAttrition)
#   expect_equal(cohortCount(cdm$new_cohort), oldCounts)
#
#   cdm$new_cohort <- cdm$new_cohort %>%
#     dplyr::filter(cohort_start_date >= as.Date("2010-01-01"))
#   expect_no_error(cdm$new_cohort <- appendCohortAttributes(
#     cohort = cdm$new_cohort, reason = "Only events after 2010"
#   ))
#
#   expect_true("GeneratedCohortSet" %in% class(cdm$new_cohort))
#   expect_true("data.frame" %in% class(cohortAttrition(cdm$new_cohort)))
#   expect_true("data.frame" %in% class(cohortCount(cdm$new_cohort)))
#   expect_true(nrow(cohortAttrition(cdm$new_cohort)) == 2)
#   expect_true(nrow(cohortCount(cdm$new_cohort)) == 1)
#
#   # newCohortSet <- dplyr::tibble(
#   #   cohort_definition_id = 1, cohort_name = "pharyngitis"
#   # )
#   # expect_no_error(cdm$new_cohort <- appendCohortAttributes(
#   #   cohort = cdm$new_cohort, cohortSet = newCohortSet
#   # ))
#   # expect_identical(newCohortSet, cohortSet(cdm$new_cohort))
#
#   dbDisconnect(con)
# })
