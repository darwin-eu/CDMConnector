test_generate_concept_cohort_set <- function(con, cdm_schema, write_schema) {
  skip_if_not_installed("CirceR")
  # withr::local_options("CDMConnector.cohort_as_temp" = FALSE) # temp cohort tables are not implemented yet
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = cdm_schema,
    write_schema = write_schema
  )

  # check that we have records
  cdm$condition_occurrence %>%
    dplyr::filter(condition_concept_id == 192671) %>%
    dplyr::count() %>%
    dplyr::pull("n") %>%
    expect_gt(10)

  # default (no descendants) ----
  # debugonce(generateConceptCohortSet)
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list(gibleed = 192671),
    name = "gibleed",
    overwrite = TRUE
  )

  cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector")) %>%
    dplyr::filter(cohort_name %in% c("gibleed_default", "GiBleed_default")) %>%
    dplyr::mutate(cohort_definition_id = 1L)

  stopifnot(nrow(cohort) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2", overwrite = TRUE)

  expected <- dplyr::collect(cdm$gibleed2) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

  actual <- dplyr::collect(cdm$gibleed) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

  expect_true(nrow(expected) > 0)
  expect_true(nrow(actual) == nrow(expected))

  # setdiff(unique(expected$subject_id), unique(actual$subject_id))
  # setdiff(unique(actual$subject_id), unique(expected$subject_id))

  expect_setequal(unique(expected$subject_id), unique(actual$subject_id))

  # remove attributes since they are a bit different
  # TODO should the attributes be the exact same? probably. but it will take time to implement that.
  attr(actual, 'cohort_attrition') <- attr(expected, 'cohort_attrition') <- NULL
  attr(actual, 'cohort_set') <- attr(expected, 'cohort_set') <- NULL
  expect_equal(actual, expected)

  expect_error({
    # should be fail fast case
    generateConceptCohortSet(
      cdm = cdm,
      conceptSet = list(gibleed = 192671),
      name = "gibleed",
      overwrite = FALSE
    )
  })

  # bind both cohorts
  cdm <- bind(cdm$gibleed, cdm$gibleed2, name = "new_gibleed")
  expect_true("new_gibleed" %in% names(cdm))
  expect_true(inherits(x = cdm$new_gibleed, what = "cohort_table"))
  expect_identical(
    settings(cdm$new_gibleed),
    settings(cdm$gibleed) |>
      dplyr::bind_rows(
        settings(cdm$gibleed2) |> dplyr::mutate("cohort_definition_id" = 2L)
      )
  )

  cdm <- generateConceptCohortSet(cdm,
    conceptSet = list(gibleed = 192671), name = "gibleed3",
    requiredObservation = c(2, 2),
    overwrite = TRUE
  )

  cdm <- generateConceptCohortSet(cdm,
    conceptSet = list(gibleed = 192671), name = "gibleed4",
    requiredObservation = c(2, 200),
    overwrite = TRUE
  )

  expect_identical(settings(cdm$gibleed4)$limit, "first")
  expect_identical(settings(cdm$gibleed4)$end, "observation_period_end_date")
  expect_identical(settings(cdm$gibleed4)$prior_observation, 2)
  expect_identical(settings(cdm$gibleed4)$future_observation, 200)

  expect_true({
    cohort_count(cdm$gibleed3)$number_records >= cohort_count(cdm$gibleed4)$number_records
  })

  # default (with descendants) ----
  # if (rlang::is_installed("Capr")) {
  if (FALSE) { # TODO: capr concept generation failing on sql server
    # we need Capr to include descendants
    cdm <- generateConceptCohortSet(
      cdm = cdm,
      conceptSet = list(gibleed = Capr::cs(Capr::descendants(192671), name = "gibleed")),
      name = "gibleed",
      overwrite = TRUE
    )

    cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector")) %>%
      dplyr::filter(cohort_name %in% c("gibleed_default_with_descendants", "GiBleed_default_with_descendants")) %>%
      dplyr::mutate(cohort_definition_id = 1L)

    stopifnot(nrow(cohort) == 1)

    cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2", overwrite = TRUE)

    expected <- dplyr::collect(cdm$gibleed2) %>%
      dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
      dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

    actual <- dplyr::collect(cdm$gibleed) %>%
      dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
      dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

    setdiff(unique(expected$subject_id), unique(actual$subject_id))
    setdiff(unique(actual$subject_id), unique(expected$subject_id))
    expect_true(nrow(expected) > 0)
    expect_true(nrow(actual) == nrow(expected))

    # note cohort table should be the same
    # but some attributes might differ (e.g. cohort attrition)
    expect_setequal(unique(expected$subject_id), unique(actual$subject_id))
    expect_equal(cohortCount(cdm$gibleed),
                 cohortCount(cdm$gibleed2))
  }

  # all occurrences (no descendants) ----
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list(gibleed = 192671),
    name = "gibleed",
    limit = "all",
    overwrite = TRUE
  )

  cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector")) %>%
    dplyr::filter(cohort_name %in% c("gibleed_all", "GiBleed_all")) %>%
    dplyr::mutate(cohort_definition_id = 1L)

  stopifnot(nrow(cohort) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2", overwrite = TRUE)

  expect_equal(
    as.integer(dplyr::pull(dplyr::tally(cdm$gibleed2), "n")),
    as.integer(dplyr::pull(dplyr::tally(cdm$gibleed), "n"))
  )

  expected <- dplyr::collect(cdm$gibleed2) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

  actual <- dplyr::collect(cdm$gibleed) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

  # setdiff(unique(expected$subject_id), unique(actual$subject_id))
  # setdiff(unique(actual$subject_id), unique(expected$subject_id))

  expect_true(nrow(expected) > 0)
  expect_true(nrow(actual) == nrow(expected))

  expect_setequal(unique(expected$subject_id), unique(actual$subject_id))
  attr(actual, 'cohort_attrition') <- attr(expected, 'cohort_attrition') <- NULL
  attr(actual, 'cohort_set') <- attr(expected, 'cohort_set') <- NULL
  expect_equal(actual, expected)

  # all occurrences (no descendants) fixed end date ----
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list(gibleed = 192671),
    name = "gibleed",
    limit = "all",
    end = "observation_period_end_date",
    overwrite = TRUE
  )

  cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector")) %>%
    dplyr::filter(cohort_name %in% c("gibleed_all")) %>%
    dplyr::mutate(cohort_definition_id = 1L)

  stopifnot(nrow(cohort) == 1)

  cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2", overwrite = TRUE)

  expect_equal(
    as.integer(dplyr::pull(dplyr::tally(cdm$gibleed2), "n")),
    as.integer(dplyr::pull(dplyr::tally(cdm$gibleed), "n"))
  )

  expected <- dplyr::collect(cdm$gibleed2) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

  actual <- dplyr::collect(cdm$gibleed) %>%
    dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
    dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)

  # setdiff(unique(expected$subject_id), unique(actual$subject_id))
  # setdiff(unique(actual$subject_id), unique(expected$subject_id))

  expect_true(nrow(expected) > 0)
  expect_true(nrow(actual) == nrow(expected))

  expect_setequal(unique(expected$subject_id), unique(actual$subject_id))
  attr(actual, 'cohort_attrition') <- attr(expected, 'cohort_attrition') <- NULL
  attr(actual, 'cohort_set') <- attr(expected, 'cohort_set') <- NULL
  expect_equal(actual, expected)

  # multiple cohort generation ------
  cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))
  cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2",
                             overwrite = TRUE)

  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list("acetaminophen_1" = 1127433,
                      "acetaminophen_2" = 1127433),
    name = "acetaminophen",
    limit = "all",
    end = "event_end_date",
    overwrite = TRUE
  )
  # should have two identical cohorts
  expect_equal(length(cohortCount(cdm$acetaminophen)  %>%
    dplyr::select("number_records") |>
    dplyr::distinct() |>
    dplyr::pull()), 1)

  # cohort generation with a cohort subset ------
  # create our main cohort of interest

  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list(gibleed_1 = 192671, gibleed_2 = 4112343),
    name = "gibleed_exp",
    overwrite = TRUE
  )

  start_person_count <- cdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  cdm <- generate_concept_cohort_set(cdm = cdm,
                                     name = "gibleed_medications",
                                     concept_set = list("diclofenac" = 1124300,
                                                        "acetaminophen" = 1127433),
                                     subset_cohort = "gibleed_exp",
                                     overwrite = TRUE)
  # we should still have our original cdm
  end_person_count <- cdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(start_person_count == end_person_count)

  expect_true(nrow(cdm$gibleed_medications %>%
    dplyr::select("subject_id") %>%
    dplyr::distinct() %>%
    dplyr::anti_join(cdm$gibleed_exp %>%
                       dplyr::select("subject_id") %>%
                       dplyr::distinct(),
                     by = "subject_id") %>%
    dplyr::collect()) == 0)

  # specifying cohort ids
  cdm <- generate_concept_cohort_set(cdm = cdm,
                                     name = "gibleed_medications2",
                                     concept_set = list("diclofenac" = 1124300,
                                                        "acetaminophen" = 1127433),
                                     subset_cohort = "gibleed_exp",
                                     subset_cohort_id = 1,
                                     overwrite = TRUE)

  expect_true(nrow(cdm$gibleed_medications2 %>%
                     dplyr::select("subject_id") %>%
                     dplyr::distinct() %>%
                     dplyr::anti_join(cdm$gibleed_exp %>%
                                        dplyr::filter(cohort_definition_id == 1L) %>%
                                        dplyr::select("subject_id") %>%
                                        dplyr::distinct(),
                                      by = "subject_id") %>%
                     dplyr::collect()) == 0)
  # expected errors
 expect_error(generate_concept_cohort_set(cdm = cdm,
                              name = "gibleed_medications2",
                              concept_set = list("diclofenac" = 1124300,
                                                 "acetaminophen" = 1127433),
                              subset_cohort = "not_a_table",
                              subset_cohort_id = 1,
                              overwrite = TRUE))

 expect_error(generate_concept_cohort_set(cdm = cdm,
                                     name = "gibleed_medications2",
                                     concept_set = list("diclofenac" = 1124300,
                                                        "acetaminophen" = 1127433),
                                     subset_cohort = "gibleed_exp",
                                     subset_cohort_id = c(99,100,101),
                                     overwrite = TRUE))


  # clean up
  dropTable(cdm, dplyr::contains("gibleed"))
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateConceptCohortSet"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    skip_if_not_installed("CirceR")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_generate_concept_cohort_set(con, cdm_schema, write_schema)
    disconnect(con)
  })
}


test_that("missing domains produce warning", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  ) %>%
    cdm_select_tbl(-drug_exposure)

  expect_warning({
    cdm <- generateConceptCohortSet(cdm, name = "celecoxib",
                                    conceptSet = list(celecoxib = 1118084))
  })

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("Regimen domain does not cause error", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))

  # create a fake concept with domain "Regimen"
  DBI::dbExecute(con, "UPDATE main.concept SET domain_id = 'Regimen' WHERE concept_id = 19129655")
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )

  concept_set <- list(drug_1 = c(1127433, 19129655), drug_2 = 19129655, drug_3 = 1127433)

  expect_no_error({
    cdm <- generateConceptCohortSet(cdm = cdm,
                                    name = "cohort",
                                    conceptSet = concept_set,
                                    overwrite = TRUE)
  })

  expect_s3_class(cdm$cohort, "cohort_table")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("Eunomia", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())

  # edge case with overlaps (issue 420)
  db <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(
    con = db,
    cdm_schema = "main",
    write_schema = "main"
  )

 expect_no_error(cdm <- cdm %>%
    generate_concept_cohort_set(concept_set = list("acetaminophen" = c(1125315,
                                                                       1127078,
                                                                       1127433,
                                                                       40229134,
                                                                       40231925,
                                                                       40162522,
                                                                       19133768)),
                                limit = "all",
                                end = "event_end_date",
                                name = "acetaminophen",
                                overwrite = TRUE))

 # behaviour with concepts not in vocab
 # this works even though 1 is not in the concept table
 expect_no_error(cdm <- generateConceptCohortSet(
   cdm = cdm,
   name = "ankle_sprain",
   conceptSet = list("ankle_sprain" = c(81151, 1)),
   end = "event_end_date",
   limit = "all",
   overwrite = TRUE
 ))
 expect_true(settings(cdm$ankle_sprain) |>
   dplyr::pull("cohort_name") == "ankle_sprain")

 expect_warning(cdm <- generateConceptCohortSet(
   cdm = cdm,
   name = "ankle_sprain",
   conceptSet = list("ankle_sprain" = 1),
   end = "event_end_date",
   limit = "all",
   overwrite = TRUE
 ), "None of the input concept IDs found for the cdm reference")
 expect_true(settings(cdm$ankle_sprain) |>
               dplyr::pull("cohort_name") == "ankle_sprain")

 # we should have ankle_sprain2 as an empty cohort in our set but don't
 expect_no_error(cdm <- generateConceptCohortSet(
   cdm = cdm,
   name = "ankle_sprain",
   conceptSet = list("ankle_sprain" = 81151,
                     "ankle_sprain2" = 1),
   end = "event_end_date",
   limit = "all",
   overwrite = TRUE
 ))
 expect_true(all(sort(settings(cdm$ankle_sprain) |>
               dplyr::pull("cohort_name")) ==
               c("ankle_sprain", "ankle_sprain2")))

})

