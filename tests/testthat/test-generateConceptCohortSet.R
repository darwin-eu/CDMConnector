dbToTest <- c(
  "duckdb"
  ,"postgres"
  ,"redshift"
  # ,"sqlserver"
  # ,"oracle"
  # ,"snowflake"
  # ,"bigquery"
)

test_concept_cohort_perm <- function(con,
                                cdm_schema,
                                write_schema) {

  test_prefix_rand <- paste0("test_",
                             paste0(sample(letters, size = 5, replace = TRUE),
                                    collapse = ""))
  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = c(schema = write_schema,
                                       prefix = test_prefix_rand))

  # create permanent cohort tables
  attr(cdm, "cohort_as_temp") <- FALSE
  cdm <- generateConceptCohortSet(cdm = cdm,
                                  conceptSet = list(gibleed = 80809),
                                  name = "gibleed",
                                  overwrite = TRUE)

  # check tables we expected have been created
  expect_true(paste0(test_prefix_rand, "gibleed") %in%
                CDMConnector::listTables(con = con, schema = write_schema))
  expect_true(paste0(test_prefix_rand, "gibleed_attrition") %in%
                CDMConnector::listTables(con = con,
                                         schema = write_schema))
  expect_true(paste0(test_prefix_rand, "gibleed_count")  %in%
                CDMConnector::listTables(con = con,
                                         schema = write_schema))
  expect_true(paste0(test_prefix_rand, "gibleed_set")  %in%
                CDMConnector::listTables(con = con,
                                         schema = write_schema))

  # check that overwrite works as expected
  expect_no_error(generateConceptCohortSet(cdm = cdm,
                                    conceptSet =  list(gibleed = 80809),
                                    name = "gibleed",
                                    overwrite = TRUE))
  expect_error(generateConceptCohortSet(cdm = cdm,
                             conceptSet =  list(gibleed = 80809),
                             name = "gibleed",
                             overwrite = FALSE))

  # check we have the people we would expect
  expect_true(all(sort(unique(cdm$condition_occurrence %>%
      dplyr::filter(condition_concept_id==80809) %>%
      dplyr::pull("person_id"))) ==
      sort(unique(cdm$gibleed %>%
      dplyr::pull("subject_id")))))

    # concept cohorts based on multiple domains
      conceptList <- list("a" = c(4336464), # procedure
                          "b" = c(4112343, # condition
                                  4336464, # procedure
                                  3006322))  # measurement
      cdm <- generateConceptCohortSet(cdm, conceptSet = conceptList,
                                               name = "test_cohort_2",
                                      overwrite = TRUE)

      # check we have the people we would expect
      # cohort 1
      expect_true(all(sort(unique(cdm$procedure_occurrence %>%
                             dplyr::filter(procedure_concept_id==4336464) %>%
                             dplyr::pull("person_id"))) ==
                        sort(unique(cdm$test_cohort_2 %>%
                               dplyr::filter(cohort_definition_id == "1") %>%
                               dplyr::pull("subject_id")))))
      # cohort 2
      expect_true(all(sort(unique(c(cdm$condition_occurrence %>%
                             dplyr::filter(condition_concept_id==4112343 ) %>%
                             dplyr::pull("person_id"),
                            cdm$procedure_occurrence %>%
                             dplyr::filter(procedure_concept_id==4336464) %>%
                             dplyr::pull("person_id"),
                      cdm$measurement %>%
                        dplyr::filter(measurement_concept_id==3006322) %>%
                        dplyr::pull("person_id"))))==
                        sort(unique(cdm$test_cohort_2 %>%
                               dplyr::filter(cohort_definition_id == "2") %>%
                               dplyr::pull("subject_id")))))

   # clean up
   CDMConnector::dropTable(cdm = cdm,
                              name = dplyr::starts_with(test_prefix_rand))

    # test temp tables
   attr(cdm, "cohort_as_temp") <- TRUE
   cdm <- generateConceptCohortSet(cdm = cdm,
                                   conceptSet = list(gibleed = 80809),
                                   name = "gibleed",
                                   overwrite = TRUE)
   # tables shouldn´t have been created
   expect_false(paste0(test_prefix_rand, "gibleed") %in%
                 CDMConnector::listTables(con = con, schema = write_schema))
   expect_false(paste0(test_prefix_rand, "gibleed_attrition") %in%
                 CDMConnector::listTables(con = con,
                                          schema = write_schema))
   expect_false(paste0(test_prefix_rand, "gibleed_count")  %in%
                 CDMConnector::listTables(con = con,
                                          schema = write_schema))
   expect_false(paste0(test_prefix_rand, "gibleed_set")  %in%
                 CDMConnector::listTables(con = con,
                                          schema = write_schema))


}


test_capr_concept_cohort <- function(con,
                                     cdm_schema,
                                     write_schema) {

  test_prefix_rand <- paste0("test_",
                             paste0(sample(letters, size = 5, replace = TRUE),
                                    collapse = ""))

  gibleed_cs <- Capr::cs(80809, name = "gibleed")
  gibleed_desc_cs <- Capr::cs(Capr::descendants(80809), name = "gibleed_desc")

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = c(schema = write_schema,
                                       prefix = test_prefix_rand))

  attr(cdm, "cohort_as_temp") <- FALSE

  # single concept set
  expect_no_error(cdm <- generateConceptCohortSet(cdm = cdm,
                                  conceptSet = gibleed_cs,
                                  name = "gibleed",
                                  overwrite = TRUE))

  # list of concept sets
  expect_no_error(cdm <- generateConceptCohortSet(cdm = cdm,
                                  conceptSet = list("gibleed" = gibleed_cs,
                                                    "gibleed_desc" = gibleed_desc_cs),
                                  name = "gibleed",
                                  overwrite = TRUE))

  # clean up
  CDMConnector::dropTable(cdm = cdm,
                          name = dplyr::starts_with(test_prefix_rand))
}


for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet"), {

    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_concept_cohort_perm(con,
                             cdm_schema = cdm_schema,
                             write_schema = write_schema)
    disconnect(con)
  })

  # cohort from capr concept set
  test_that(glue::glue("{dbtype} - generateCohortSet"), {
    skip_if_not_installed("Capr", minimum_version = "2.0.5")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_capr_concept_cohort(con,
                             cdm_schema = cdm_schema,
                             write_schema = write_schema)
    disconnect(con)
  })

}


