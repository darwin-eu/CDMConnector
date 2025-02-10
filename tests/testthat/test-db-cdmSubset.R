test_cdmSubset <- function(con, cdmSchema, writeSchema) {

  # test cdmSubset, cdmSubsetCohort, cdmSample, cdmFlatten

  cdm <- cdmFromCon(
    con = con, cdmName = "test", cdmSchema = cdmSchema,
    writeSchema = writeSchema
  )

  cohortSet <- readCohortSet(system.file("cohorts3", package = "CDMConnector", mustWork = TRUE)) %>%
    dplyr::filter(cohort_name == "gibleed_all")

  cdm <- generateCohortSet(cdm, cohortSet = cohortSet, name = "cohort")

  cdmSampled1 <- cdmSample(cdm = cdm, n = 100, seed = 123)
  expect_true(cdmSampled1$person |> dplyr::tally() |> dplyr::pull() == 100)
  expect_true("person_sample" %in% names(cdmSampled1))
  expect_false("person_sample" %in% names(cdm))

  cdmSampled2 <- cdmSample(cdm = cdm, n = 200, name = "sample_person") # test name
  expect_true(cdmSampled2$person |> dplyr::tally() |> dplyr::pull() == 200)
  expect_true("sample_person" %in% names(cdmSampled2))
  expect_false("person_sample" %in% names(cdmSampled2))

  cdmSampled3 <- cdmSample(cdm = cdm, n = 100, seed = 123, name = "sample3") # test seed
  expect_true(cdmSampled3$person |> dplyr::tally() |> dplyr::pull() == 100)
  expect_true("sample3" %in% names(cdmSampled3))

  cdmSampled4 <- cdmSample(cdm = cdm, n = 100, seed = 1234, name = "sample4")
  expect_true(cdmSampled4$person |> dplyr::tally() |> dplyr::pull() == 100)
  expect_true("sample4" %in% names(cdmSampled4))

  expect_true(identical(
    cdmSampled1$person |> dplyr::collect() |> dplyr::arrange(.data$person_id),
    cdmSampled3$person |> dplyr::collect() |> dplyr::arrange(.data$person_id)
  ))

  expect_false(identical(
    cdmSampled1$person |> dplyr::collect() |> dplyr::arrange(.data$person_id),
    cdmSampled4$person |> dplyr::collect() |> dplyr::arrange(.data$person_id)
  ))

  idsToSubsetTo <- cdm$person %>%
    dplyr::select(person_id) %>%
    head(10) %>%
    dplyr::pull() %>%
    sort() %>%
    as.integer()

  cdm10 <- cdmSubset(cdm, idsToSubsetTo)

  expect_equal(
    dplyr::tally(cdm10$person) %>%
    dplyr::pull(.data$n) %>%
    as.integer(),
    10L)

  expect_equal(
    cdm10$person %>% dplyr::select(person_id) %>% dplyr::arrange(person_id) %>% dplyr::pull() %>% sort,
    idsToSubsetTo
  )

  # cdmSubsetCohort ----
  cdmGiBleed <- cdmSubsetCohort(cdm, cohortTable = "cohort")

  idsInConditionTable <- cdmGiBleed$condition_occurrence %>%
    dplyr::distinct(.data$person_id) %>%
    dplyr::pull()

  idsInCohort <- cdmGiBleed$cohort %>%
    dplyr::distinct(.data$subject_id) %>%
    dplyr::pull() %>%
    as.integer() # could be int64 type

  expect_true(all(idsInConditionTable %in% idsInCohort))

  # cdmFlatten ----
  flatCdm <- cdmFlatten(cdm10) %>%
    dplyr::collect()

  expect_s3_class(flatCdm, "data.frame")

  if (dbms(con) == "duckdb") {
    expect_true(nrow(flatCdm) > 0)
    expect_true(all(c("drug_exposure", "condition_occurrence") %in% flatCdm$domain))
  }

  flatCdm2 <- cdmFlatten(cdm10,
                         c("condition_occurrence",
                           "drug_exposure",
                           "procedure_occurrence",
                           "measurement",
                           "visit_occurrence",
                           "death",
                           "observation"),
                           includeConceptName = FALSE) %>%
    dplyr::collect()

  expect_s3_class(flatCdm2, "data.frame")

}

dbtype = "postgres"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - sample database"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdmSchema <- get_cdm_schema(dbtype)
    writeSchema <- get_write_schema(dbtype)
    skip_if(any(writeSchema == "") || any(cdmSchema == "") || is.null(con))
    test_cdmSubset(con, cdmSchema, writeSchema)
    disconnect(con)
  })
}



