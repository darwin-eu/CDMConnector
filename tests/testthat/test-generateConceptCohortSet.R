test_that("working example", {

  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, "main")

  cdm <- generateConceptCohortSet(cdm = cdm,
                         conceptSet =  list(gibleed = 80809),
                         name = "gibleed",
                         overwrite = TRUE,
                         computeAttrition = TRUE)

  expect_true(nrow(cdm$gibleed %>%
    dplyr::collect()) >= 1)

  # check we have the people we would expect
  expect_true(all(sort(cdm$condition_occurrence %>%
    dplyr::filter(condition_concept_id==80809) %>%
    dplyr::pull("person_id")) ==
    sort(cdm$gibleed %>%
    dplyr::pull("subject_id"))))

cdmDisconnect(cdm)
})
