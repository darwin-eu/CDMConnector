test_generate_concept_cohort_set <- function(con, cdm_schema, write_schema) {
  skip_if_not_installed("CirceR")
  # withr::local_options("CDMConnector.cohort_as_temp" = FALSE) # temp cohort tables are not implemented yet
  cdm <- cdmFromCon(
    con = con,
    cdmName = "cdm",
    cdmSchema = cdm_schema,
    writeSchema = write_schema
  )

  # check that we have records. Need the eunomia gibleed data for this.
  cdm$condition_occurrence %>%
    dplyr::filter(condition_concept_id == 192671L) %>%
    dplyr::count() %>%
    dplyr::pull("n") %>%
    expect_gt(10)

  # default (no descendants) ----
  # debugonce(generateConceptCohortSet)
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list(gibleed = 192671L),
    name = "gibleed",
    overwrite = TRUE
  )

  cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector")) %>%
    dplyr::filter(cohort_name %in% c("gibleed_default", "GiBleed_default")) %>%
    dplyr::mutate(cohort_definition_id = 1L)

  # TODO add gibleed data to spark test server
  # if (dbms(con) == "spark") cohort$json <- stringr::str_replace_all(cohort$json, "192671", "40481087")[[1]]

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
      conceptSet = list(gibleed = 192671L),
      name = "gibleed",
      overwrite = FALSE
    )
  }, "gibleed already exists in the CDM")

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
    conceptSet = list(gibleed = 192671L), name = "gibleed3",
    requiredObservation = c(2, 2),
    overwrite = TRUE
  )

  cdm <- generateConceptCohortSet(cdm,
    conceptSet = list(gibleed = 192671L), name = "gibleed4",
    requiredObservation = c(2, 200),
    overwrite = TRUE
  )

  expect_identical(settings(cdm$gibleed4)$limit, "first")
  expect_identical(settings(cdm$gibleed4)$end, "observation_period_end_date")
  expect_true(settings(cdm$gibleed4)$prior_observation == 2)
  expect_true(settings(cdm$gibleed4)$future_observation == 200)

  expect_true({
    cohortCount(cdm$gibleed3)$number_records >= cohortCount(cdm$gibleed4)$number_records
  })

  # default (with descendants) ----
  # if (rlang::is_installed("Capr")) {
  # if (FALSE) { # TODO: capr concept generation failing on sql server
  #   # we need Capr to include descendants
  #   cdm <- generateConceptCohortSet(
  #     cdm = cdm,
  #     conceptSet = list(gibleed = Capr::cs(Capr::descendants(192671), name = "gibleed")),
  #     name = "gibleed",
  #     overwrite = TRUE
  #   )
  #
  #   cohort <- readCohortSet(system.file("cohorts3", package = "CDMConnector")) %>%
  #     dplyr::filter(cohort_name %in% c("gibleed_default_with_descendants", "GiBleed_default_with_descendants")) %>%
  #     dplyr::mutate(cohort_definition_id = 1L)
  #
  #   stopifnot(nrow(cohort) == 1)
  #
  #   cdm <- generateCohortSet(cdm, cohortSet = cohort, name = "gibleed2", overwrite = TRUE)
  #
  #   expected <- dplyr::collect(cdm$gibleed2) %>%
  #     dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
  #     dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)
  #
  #   actual <- dplyr::collect(cdm$gibleed) %>%
  #     dplyr::arrange(.data$cohort_definition_id, .data$subject_id, .data$cohort_start_date, .data$cohort_end_date) %>%
  #     dplyr::mutate_if(~ "integer64" %in% class(.), as.integer)
  #
  #   setdiff(unique(expected$subject_id), unique(actual$subject_id))
  #   setdiff(unique(actual$subject_id), unique(expected$subject_id))
  #   expect_true(nrow(expected) > 0)
  #   expect_true(nrow(actual) == nrow(expected))
  #
  #   # note cohort table should be the same
  #   # but some attributes might differ (e.g. cohort attrition)
  #   expect_setequal(unique(expected$subject_id), unique(actual$subject_id))
  #   expect_equal(cohortCount(cdm$gibleed),
  #                cohortCount(cdm$gibleed2))
  # }


  # use omopgenerics conceptSetExpression to include descendants

  # Capr::cs(Capr::descendants(192671), name = "gibleed")
  conceptSet <- omopgenerics::newConceptSetExpression(
    list("gibleed" = dplyr::tibble(
      "concept_id" = 192671,
      "excluded" = FALSE,
      "descendants" = TRUE,
      "mapped" = FALSE
    ))
  )

  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = conceptSet,
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

  # setdiff(unique(expected$subject_id), unique(actual$subject_id))
  # setdiff(unique(actual$subject_id), unique(expected$subject_id))
  expect_equal(sort(unique(expected$subject_id)), sort(unique(actual$subject_id)))
  expect_true(nrow(expected) > 0)
  expect_true(nrow(actual) == nrow(expected))

  # note cohort table should be the same
  # but some attributes might differ (e.g. cohort attrition)
  expect_setequal(unique(expected$subject_id), unique(actual$subject_id))
  expect_equal(cohortCount(cdm$gibleed),
               cohortCount(cdm$gibleed2))


  # all occurrences (no descendants) ----
  cdm <- generateConceptCohortSet(
    cdm = cdm,
    conceptSet = list(gibleed = 192671L),
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
    conceptSet = list(gibleed = 192671L),
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
    conceptSet = list(gibleed_1 = 192671L, gibleed_2 = 4112343),
    name = "gibleed_exp",
    overwrite = TRUE
  )

  start_person_count <- cdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  cdm <- generateConceptCohortSet(cdm = cdm,
                                     name = "gibleed_medications",
                                     conceptSet = list("diclofenac" = 1124300,
                                                        "acetaminophen" = 1127433),
                                     subsetCohort = "gibleed_exp",
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
  cdm <- generateConceptCohortSet(cdm = cdm,
                                  name = "gibleed_medications2",
                                  conceptSet = list("diclofenac" = 1124300,
                                                    "acetaminophen" = 1127433),
                                  subsetCohort = "gibleed_exp",
                                  subsetCohortId = 1,
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
  expect_error(
    generateConceptCohortSet(cdm = cdm,
                             name = "gibleed_medications2",
                             conceptSet = list("diclofenac" = 1124300,
                                               "acetaminophen" = 1127433),
                              subsetCohort = "not_a_table",
                              subsetCohortId = 1,
                              overwrite = TRUE)
  )

 expect_error(
   generateConceptCohortSet(cdm = cdm,
                            name = "gibleed_medications2",
                            conceptSet = list("diclofenac" = 1124300,
                                              "acetaminophen" = 1127433),
                            subsetCohort = "gibleed_exp",
                            subsetCohortId = c(99,100,101), # these cohort ids not in cohort table
                            overwrite = TRUE)
  )

  # clean up
  dropSourceTable(cdm, dplyr::contains("gibleed"))
}

# dbtype = "duckdb"

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
  skip_if_not("duckdb" %in% dbToTest)
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
  ) %>%
    cdmSelect(-drug_exposure)

  expect_warning({
    cdm <- generateConceptCohortSet(cdm, name = "celecoxib",
                                    conceptSet = list(celecoxib = 1118084))
  })

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("Regimen domain does not cause error", {
  skip_on_cran()
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))

  # create a fake concept with domain "Regimen"
  DBI::dbExecute(con, "UPDATE main.concept SET domain_id = 'Regimen' WHERE concept_id = 19129655")
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
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
  skip_if_not("duckdb" %in% dbToTest)
  skip_if_not(eunomiaIsAvailable())

  # edge case with overlaps (issue 420)
  db <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(
    con = db,
    cdmSchema = "main",
    writeSchema = "main"
  )

 expect_no_error(cdm <- cdm %>%
    generateConceptCohortSet(conceptSet = list("acetaminophen" = c(1125315,
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

test_that("invalid cdm records are ignored in generateConceptCohortSet", {
  skip_if_not_installed("duckdb")
  cdm <- cdmFromTables(
    tables = list(
      "person" = tibble(
        person_id = 1L, gender_concept_id = 0L, year_of_birth = 1900L,
        race_concept_id = 0L, ethnicity_concept_id = 0L
      ),
      "observation_period" = tibble(
        observation_period_id = 1L, person_id = 1L,
        observation_period_start_date = as.Date("1900-01-01"),
        observation_period_end_date = as.Date("2000-01-01"),
        period_type_concept_id = 0L
      ),
      "drug_exposure" = tibble(
        drug_exposure_id = 1L, person_id = 1L, drug_concept_id = 1L,
        drug_exposure_start_date = as.Date(c("1950-01-01", "1951-01-01")),
        drug_exposure_end_date = as.Date(c("1945-01-01", "1952-01-01")),
        drug_type_concept_id = 0L
      ),
      "concept" = tibble(
        concept_id = 1L, concept_name = "my_drug", domain_id = "Drug",
        vocabulary_id = "vocab", concept_class_id = "0", concept_code = "0",
        valid_start_date = as.Date("1900-01-01"), valid_end_date = as.Date("2030-01-01"),
        standard_concept = "S",
        invalid_reason = ""
      )
    ),
    cdmName = "test"
  )

  con <- DBI::dbConnect(duckdb::duckdb())
  cdm <- copyCdmTo(con, cdm = cdm, schema = "main")

  cdm <- generateConceptCohortSet(cdm = cdm,
                                  conceptSet = list(custom = 1),
                                  name = "my_cohort",
                                  end = "event_end_date")

  actual <- dplyr::collect(cdm$my_cohort) %>%
    tibble()

  # names(attributes(actual))

  # remove cohort attributes
  attr(actual, "cohort_set") <- NULL
  attr(actual, "cohort_attrition") <- NULL

  expected <- tibble(
    cohort_definition_id = 1L,
    subject_id = 1L,
    cohort_start_date = as.Date("1951-01-01"),
    cohort_end_date = as.Date("1952-01-01"),
  )

  expect_equal(actual, expected)
})


test_that("attrition columns are correct", {
  skip_if_not("duckdb" %in% dbToTest)
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main", "main")

  cdm <- generateConceptCohortSet(cdm,
                                  conceptSet = list(acetaminophen = 1127433),
                                  name = "cohort1")


  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))[1,]

  cdm <- generateCohortSet(cdm,
                           cohortSet = cohort_set,
                           name = "cohort2")

  expected_colnames <- c("cohort_definition_id", "number_records", "number_subjects",
                         "reason_id", "reason", "excluded_records", "excluded_subjects")

  expect_equal(expected_colnames, colnames(attrition(cdm$cohort1)))
  expect_equal(expected_colnames, colnames(attrition(cdm$cohort2)))

  DBI::dbDisconnect(con, shutdown = T)
})


test_that("attrition columns are correct", {
  skip_if_not_installed("Capr")
  skip_if_not("duckdb" %in% dbToTest)
  skip_on_cran()
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main", "main")

  cohort_set <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet(cdm,
                           cohortSet = cohort_set,
                           name = "cohort")

  expected_colnames <- c("cohort_definition_id", "number_records", "number_subjects",
                         "reason_id", "reason", "excluded_records", "excluded_subjects")

  expect_equal(expected_colnames, colnames(attrition(cdm$cohort)))

  DBI::dbDisconnect(con, shutdown = T)
})

