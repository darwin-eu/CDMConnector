
test_generate_concept_cohort_set <- function(con, cdm_schema, write_schema) {
  skip_if_not_installed("CirceR")

  prefix <- paste0("test", as.integer(Sys.time()), "_")

  withr::local_options("CDMConnector.cohort_as_temp" = FALSE)
  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = c(schema = write_schema, prefix = prefix))

  cdm <- generateConceptCohortSet(cdm = cdm,
                                  conceptSet = list(gibleed = 192671),
                                  name = "gibleed",
                                  overwrite = TRUE)

  cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))
  cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2", overwrite = TRUE)

  expect_equal(nrow(dplyr::collect(cdm$gibleed2)), nrow(dplyr::collect(cdm$gibleed)))

  expected <- dplyr::collect(cdm$gibleed2) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  actual <- dplyr::collect(cdm$gibleed) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

  expect_equal(actual, expected)

  if (rlang::is_installed("Capr")) {
    cohort <- Capr::cohort(entry = Capr::entry(Capr::conditionOccurrence(Capr::cs(192671, name = "gibleed"))))
    cdm <- generateCohortSet(cdm, list("gibleed_circe" = cohort), name = "gibleed3")

    expected <- dplyr::collect(cdm$gibleed3) %>%
      dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date)

    expect_equal(actual, expected)
  }


  # check tables we expected have been created
  tables <- paste0(prefix, "gibleed", c("", "_attrition", "_count", "_set"))
  # failing in testthat
  expect_true(all(tables %in%
                    listTables(con = con, schema = write_schema)))

  # check that overwrite works as expected
  expect_no_error({
    cdm <- generateConceptCohortSet(cdm = cdm,
                                    conceptSet =  list(gibleed = 80809),
                                    name = "gibleed",
                                    end = 10,
                                    overwrite = TRUE)
  })

  dif <- cdm$gibleed %>%
    dplyr::mutate(dif = !!datediff("cohort_start_date", "cohort_end_date")) %>%
    dplyr::distinct(.data$dif) %>%
    dplyr::pull()

  expect_equal(dif, 10)

  expect_error({
    generateConceptCohortSet(cdm = cdm,
                             conceptSet =  list(gibleed = 80809),
                             name = "gibleed",
                             overwrite = FALSE)
  })

   # clean up
   CDMConnector::dropTable(cdm, dplyr::contains("gibleed"))
}

# dbtype = "duckdb"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateConceptCohortSet"), {
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_generate_concept_cohort_set(con, cdm_schema, write_schema)
    disconnect(con)
  })
}


# test_capr_concept_cohort <- function(con,
#                                      cdm_schema,
#                                      write_schema) {
#
#   test_prefix_rand <- paste0("test_",
#                              paste0(sample(letters, size = 5, replace = TRUE),
#                                     collapse = ""))
#   withr::local_options("CDMConnector.cohort_as_temp" = FALSE)
#
#   gibleed_cs <- Capr::cs(80809, name = "gibleed")
#   gibleed_desc_cs <- Capr::cs(Capr::descendants(80809), name = "gibleed_desc")
#
#   cdm <- cdm_from_con(con,
#                       cdm_schema = cdm_schema,
#                       write_schema = c(schema = write_schema,
#                                        prefix = test_prefix_rand))
#
#   # single concept set
#   expect_no_error(cdm <- generateConceptCohortSet(cdm = cdm,
#                                                   conceptSet = list("gibleed" = gibleed_cs),
#                                                   name = "gibleed",
#                                                   overwrite = TRUE))
#
#   # list of concept sets
#   expect_no_error(cdm <- generateConceptCohortSet(cdm = cdm,
#                                                   conceptSet = list("gibleed" = gibleed_cs,
#                                                                     "gibleed_desc" = gibleed_desc_cs),
#                                                   name = "gibleed",
#                                                   overwrite = TRUE))
#
#   # clean up
#   CDMConnector::dropTable(cdm, dplyr::contains("gibleed"))
#
# }
#
# test_concept_cohort_mock <- function(con,
#                                      cdm_schema,
#                                      write_schema){
#
#   test_prefix_rand <- paste0("test_",
#                              paste0(sample(letters, size = 5, replace = TRUE),
#                                     collapse = ""))
#
#   # mock cdm
#   # 3 individuals
#   # 1 event outside of observation period
#
#   # person table
#   personTable <- tibble::tibble(
#     person_id = c("1", "2", "3"),
#     gender_concept_id = rep("8507", 3),
#     year_of_birth = 2000,
#     month_of_birth = 01,
#     day_of_birth = 01
#   )
#
#   # obs period table
#   observationPeriodTable <- tibble::tibble(
#     observation_period_id = c("1", "2", "3"),
#     person_id = c("1", "2", "3"),
#     observation_period_start_date = c(as.Date("2000-01-01"),
#                                       as.Date("2000-01-01"),
#                                       as.Date("2010-01-01")),
#     observation_period_end_date = c(as.Date("2015-06-01"),
#                                     as.Date("2015-06-01"),
#                                     as.Date("2015-06-01"))
#   )
#
#   # concept
#   concept <- data.frame(
#     concept_id = 1:3,
#     concept_name = c(
#       "Arthritis",
#       "Adalimumab",
#       "Knee replacement"
#     ),
#     domain_id = c("Condition", "Drug", "Procedure"),
#     vocabulary_id = c("SNOMED", "RxNorm", "CPT4"
#     ),
#     standard_concept =  rep("S", 3),
#     concept_class_id = c("Clinical Finding", "Ingredient", "Procedure"),
#     concept_code = "1234",
#     valid_start_date = NA,
#     valid_end_date = NA,
#     invalid_reason = NA
#   )
#
#   # condition occurrence
#   conditionOccurrence <- dplyr::tibble(
#     condition_occurrence_id = c(1,2),
#     person_id = c(1,3),
#     condition_concept_id = 1,
#     condition_start_date = as.Date("2005-01-01"),
#     condition_end_date = as.Date("2005-01-04")
#   )
#
#   # drug_exposure
#   drugExposure <- data.frame(
#       drug_exposure_id = 1,
#       person_id = 2,
#       drug_concept_id = 2,
#       drug_exposure_start_date = as.Date("2007-01-01"),
#       drug_exposure_end_date = as.Date("2008-01-01"),
#       quantity = 1
#     )
#
#
#
#   # copy to cdm
#   cdm_mock <- list(person = personTable,
#                    observation_period = observationPeriodTable,
#                    condition_occurrence = conditionOccurrence,
#                    drug_exposure = drugExposure,
#                    concept = concept)
#   class(cdm_mock) <-"cdm_reference"
#   attr(cdm_mock, "cdm_version") <- "5.3"
#   attr(cdm_mock,"cdm_name") <- "mock"
#  cdm <-  copyCdmTo(con = con,
#             prefix = test_prefix_rand,
#             cdm = cdm_mock,
#             schema = cdm_schema,
#             overwrite = TRUE)
#  attr(cdm, "write_schema") <- write_schema
#
#  cdm <- generateConceptCohortSet(cdm,overwrite = TRUE,
#                                  name ="arthritis",
#                                  conceptSet = list("arthritis" = 1))
#   # check we ignore events outside of observation time
#   # we shouldn't see person 3 because their record was before their
#   # observation start date
#   expect_true(cdm$arthritis %>% dplyr::pull("subject_id") == 1)
#
#   # check for concept cohort based on multiple domains
#   cdm <- generateConceptCohortSet(cdm,overwrite = TRUE,
#                                   name ="arthritis_adalimumab",
#                                   conceptSet = list("arthritis_adalimumab" = c(1,2)))
#
#   expect_true(all(c(1,2) %in% (cdm$arthritis_adalimumab %>%
#                 dplyr::pull("subject_id"))))
#   expect_true(!all(c(3) %in% (cdm$arthritis_adalimumab %>%
#                                  dplyr::pull("subject_id"))))
#
#   # expected errors
#   # requiring concept from another domain not in the cdm ref
#   expect_error(generateConceptCohortSet(cdm,
#                            overwrite = TRUE,
#                            name ="arthritis_adalimumab",
#                            conceptSet = list("arthritis_adalimumab" = c(1, 2, 3))))
#
#   # clean up
#   CDMConnector::dropTable(cdm, dplyr::contains("arthritis"))
#
# }
#
#
#
# # # test temp tables
# # # test temp tables
# # withr::local_options("CDMConnector.cohort_as_temp" = TRUE)
# #
# # cdm <- generateConceptCohortSet(cdm = cdm,
# #                                 conceptSet = list(gibleed = 80809),
# #                                 name = "gibleed",
# #                                 overwrite = TRUE)
# #
# # # TODO clean up dbplyr tables that are supposed to be temp (intermediate)
# #
# # # tables shouldn´t have been created
# # # failing test
# # # TODO fix this
# # # expect_false(any(tables %in% listTables(con = con, schema = write_schema)))
# # # expect_true(all(tables %in% listTables(con = con, schema = NULL)))
#
#
#
#
# for (dbtype in dbToTest) {
#   test_that(glue::glue("{dbtype} - generateConceptCohortSet"), {
#     con <- get_connection(dbtype)
#     cdm_schema <- get_cdm_schema(dbtype)
#     write_schema <- get_write_schema(dbtype)
#     skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
#     test_concept_cohort_perm(con, cdm_schema, write_schema)
#     test_concept_cohort_mock(con, cdm_schema, write_schema)
#     disconnect(con)
#   })
#
#   test_that(glue::glue("{dbtype} - concept cohort with capr"), {
#     skip_if_not_installed("Capr", minimum_version = "2.0.5")
#     con <- get_connection(dbtype)
#     cdm_schema <- get_cdm_schema(dbtype)
#     write_schema <- get_write_schema(dbtype)
#     skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
#     test_capr_concept_cohort(con,
#                              cdm_schema = cdm_schema,
#                              write_schema = write_schema)
#     disconnect(con)
#   })
# }


