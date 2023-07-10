# test_that("codelist cohorts", {
#
#   skip_if_not_installed("duckdb")
#   skip_if_not(eunomia_is_available())
#
#   con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#   cdm_schema = "main"
#   write_schema = "main"
#
#   cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = c(schema = write_schema, prefix = "tst_"))
#
#   cdm <- generateConceptCohortSet(cdm = cdm,
#                          conceptSet =  list(gibleed = 80809),
#                          name = "gibleed",
#                          overwrite = TRUE)
#
#   expect_true("GeneratedCohortSet" %in% class(cdm$gibleed))
#   expect_true(nrow(cdm$gibleed %>%
#                      dplyr::collect()) >= 1)
#   expect_true(nrow(cohortAttrition(cdm$gibleed)) >= 1)
#   expect_true(nrow(cohortSet(cdm$gibleed)) >= 1)
#   expect_true(nrow(cohortCount(cdm$gibleed)) >= 1)
#
#   # overwrite cohort table
#   cdm <- generateConceptCohortSet(cdm = cdm,
#                                   conceptSet =  list(gibleed = 80809),
#                                   name = "gibleed",
#                                   overwrite = TRUE,
#                                   computeAttrition = TRUE)
#   expect_true(nrow(cdm$gibleed %>%
#                      dplyr::collect()) >= 1)
#   # error if overwrite is false
#   expect_error(generateConceptCohortSet(cdm = cdm,
#                            conceptSet =  list(gibleed = 80809),
#                            name = "gibleed",
#                            overwrite = FALSE,
#                            computeAttrition = TRUE))
#
#   # check we have the people we would expect
#   expect_true(all(sort(cdm$condition_occurrence %>%
#     dplyr::filter(condition_concept_id==80809) %>%
#     dplyr::pull("person_id")) ==
#     sort(cdm$gibleed %>%
#     dplyr::pull("subject_id"))))
#
#
#
#   # concept cohorts based on multiple domains
#   conceptList <- list("a" = c(4336464), # procedure
#                       "b" = c(4112343, # condition
#                               4336464, # procedure
#                               3006322))  # measurement
#   cdm <- generateConceptCohortSet(cdm, conceptSet = conceptList,
#                                            name = "test_cohort_2",
#                                   overwrite = TRUE)
#
#   # check we have the people we would expect
#   # cohort 1
#   expect_true(all(sort(cdm$procedure_occurrence %>%
#                          dplyr::filter(procedure_concept_id==4336464) %>%
#                          dplyr::pull("person_id")) ==
#                     sort(cdm$test_cohort_2 %>%
#                            dplyr::filter(cohort_definition_id == "1") %>%
#                            dplyr::pull("subject_id"))))
#   # cohort 2
#   expect_true(all(sort(unique(c(cdm$condition_occurrence %>%
#                          dplyr::filter(condition_concept_id==4112343 ) %>%
#                          dplyr::pull("person_id"),
#                         cdm$procedure_occurrence %>%
#                          dplyr::filter(procedure_concept_id==4336464) %>%
#                          dplyr::pull("person_id"),
#                   cdm$measurement %>%
#                     dplyr::filter(measurement_concept_id==3006322) %>%
#                     dplyr::pull("person_id"))))==
#                     sort(unique(cdm$test_cohort_2 %>%
#                            dplyr::filter(cohort_definition_id == "2") %>%
#                            dplyr::pull("subject_id")))))
#
# cdmDisconnect(cdm)
# })




