
test_cohort_generation <- function(con, cdm_schema, write_schema) {
  skip_if_not_installed("CirceR")

  cdm <- cdmFromCon(con, cdm_schema, write_schema, cdmName = "test cdm")

  # test read cohort set with a cohortsToCreate.csv
  expect_error(readCohortSet(path = "does_not_exist"))
  expect_error(readCohortSet(path = paste0(tempdir(), "/not_a_dir")))

  # test readCohortSet without cohortsToCreate.csv
  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  expect_s3_class(cohortSet, "CohortSet")

  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           overwrite = TRUE,
                           computeAttrition = TRUE)

  expect_true(methods::is(cdm$chrt0, "cohort_table"))

  expect_error(generateCohortSet(cdm, name = "blah", cohortSet = "not a cohort"))

  # check already exists
  expect_error(generateCohortSet(cdm, cohortSet, name = "chrt0", overwrite = FALSE))
  expect_no_error(cdm <- generateCohortSet(cdm,
                                           cohortSet =  cohortSet,
                                           name = "chrt0",
                                           overwrite = TRUE))

  expect_true("chrt0" %in% tolower(listTables(con, schema = write_schema)))

  expect_true("cohort_table" %in% class(cdm$chrt0))

  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% colnames(df)))

  expect_true(all(c("cohort_set", "cohort_attrition", "cdm_reference") %in% names(attributes(cdm$chrt0))))
  attrition_df <- attrition(cdm$chrt0)
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(settings(cdm$chrt0), "data.frame")
  counts <- cohortCount(cdm$chrt0)
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), 3)

  # cohort table should be lowercase
  expect_error(generateCohortSet(cdm, cohortSet, name = "MYcohorts", overwrite = TRUE))

  # empty data
  expect_error(generateCohortSet(cdm, cohortSet %>% head(0), name = "cohorts", overwrite = TRUE))
  dropTable(cdm, dplyr::starts_with("chrt0_"))
  expect_length(grep("^chrt0_", listTables(con, schema = write_schema)), 0)
}

for (dbtype in dbToTest) {
  # dbtype = "bigquery"
  test_that(glue::glue("{dbtype} - generateCohortSet"), {
    skip_if_not_installed("CirceR")
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_cohort_generation(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}

# can't depend on Capr until it's on CRAN so removing this test
# test_that("Generation from Capr Cohorts", {
#   skip_if_not(eunomiaIsAvailable())
#   skip_if_not_installed("Capr", minimum_version = "2.0.5")
#   skip_if_not_installed("duckdb")
#   skip_if_not_installed("CirceR")
#   skip_if_not("duckdb" %in% dbToTest)
#
#   con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
#   cdm <- cdmFromCon(
#     con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
#   )
#
#   gibleed_cohort_definition <- Capr::cohort(
#     entry = Capr::conditionOccurrence(Capr::cs(Capr::descendants(192671), name = "test")),
#     attrition = Capr::attrition(
#       "no RA" = Capr::withAll(
#         Capr::exactly(0,
#                       Capr::conditionOccurrence(Capr::cs(Capr::descendants(80809), name = "test")),
#                       Capr::duringInterval(Capr::eventStarts(-Inf, Inf))))
#     )
#   )
#
#   cdm <- generateCohortSet(
#     cdm,
#     list(gibleed = gibleed_cohort_definition),
#     name = "gibleed",
#     overwrite = TRUE
#   )
#
#   expect_gt(nrow(dplyr::collect(cdm$gibleed)), 10)
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })

test_that("duckdb - phenotype library generation", {
  skip("manual test")
  skip_if_not_installed("PhenotypeLibrary")

  cohortSet <- PhenotypeLibrary::listPhenotypes() %>%
    dplyr::pull("cohortId") %>%
    PhenotypeLibrary::getPlCohortDefinitionSet()

  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
  )
  expect_error(
    generateCohortSet(cdm, cohortSet, name = "cohort", overwrite = TRUE, computeAttrition = TRUE),
    NA
  )
  DBI::dbDisconnect(con, shutdown = T)
})

test_that("TreatmentPatterns cohort works", {
  skip_if_not_installed("TreatmentPatterns")
  skip_on_cran()
  skip("manual test")
  skip("failing test")
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
  )

  cohortSet <- readCohortSet(
    system.file("exampleCohorts", package = "TreatmentPatterns")
  )

  cdm <- generateCohortSet(
    cdm = cdm,
    cohortSet = cohortSet,
    name = "cohorttable",
    overwrite = TRUE
  )

  expect_s3_class(cdm$cohorttable, "cohort_table")
  DBI::dbDisconnect(con, shutdown = TRUE)
})

# Test from issue https://github.com/darwin-eu-dev/CDMConnector/issues/238
# cdm <- cdm_from_con(con, cdm_schema, write_schema)
#
# gibleed_cohort_definition <- cohort(
#   entry = Capr::conditionOccurrence(cs(descendants(192671), name = "test")),
#   attrition = attrition(
#     "no RA" = withAll(
#       exactly(0,
#               conditionOccurrence(cs(descendants(80809), name = "test")),
#               duringInterval(eventStarts(-Inf, Inf))))
#   )
# )
#
# cdm <- generateCohortSet(
#   cdm,
#   list(gibleed = gibleed_cohort_definition),
#   name = "gibleed",
#   overwrite = TRUE
# )
#
# cdm$gibleed
# cohort_count(cdm$gibleed)


test_that("newGeneratedCohortSet works with prefix", {
  skip_if_not_installed("duckdb")
  skip_if_not("duckdb" %in% dbToTest)
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))

  write_schema <- c(schema = "main", prefix = "test_")
  cdm <- cdmFromCon(con, "main", writeSchema = write_schema, cdmName = "eunomia")

  cdm$cohort <- cdm$condition_era %>%
    head(1) %>%
    dplyr::mutate(cohort_definition_id = 1L) %>%
    dplyr::select(
      cohort_definition_id,
      subject_id = "person_id",
      cohort_start_date = "condition_era_start_date",
      cohort_end_date = "condition_era_end_date"
    ) %>%
    compute(name = "cohort", temporary = FALSE, overwrite = TRUE)

  cdm$cohort <- newCohortTable(cdm$cohort)

  expect_true("cohort" %in% listTables(con, write_schema))
  expect_true("test_cohort" %in% listTables(con, "main"))

  expect_s3_class(cohortCount(cdm$cohort), "data.frame")
  expect_s3_class(settings(cdm$cohort), "data.frame")

  expect_s3_class(attrition(cdm$cohort), "data.frame")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

# issue: https://github.com/darwin-eu-dev/CDMConnector/issues/337

test_that("no error is given if attrition table already exists and overwrite = TRUE", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))
  cdm <- cdmFromCon(
    con = con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main"
  )
  cohortSet <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "test",
                           computeAttrition = TRUE)

  expect_no_error({
    cdm <- generateCohortSet(cdm,
                             cohortSet,
                             name = "test",
                             computeAttrition = FALSE,
                             overwrite = TRUE)
  })

  expect_no_error({
    cdm <- generateCohortSet(cdm,
                             cohortSet,
                             name = "test",
                             computeAttrition = TRUE,
                             overwrite = TRUE)
  })

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("readCohortSet works from working directory", {
  skip("manual test") # seems to work as a standalone test but fails when running testtat

  source_folder <- system.file("cohorts1", package = "CDMConnector", mustWork = TRUE)
  destination_folder <- file.path(tempdir(), "cohorts1")

  if (!dir.exists(destination_folder)) {
    dir.create(destination_folder, recursive = TRUE)
  }

  files <- list.files(source_folder, full.names = TRUE, recursive = TRUE)

  file.copy(from = files,
            to = file.path(destination_folder, basename(files)),
            overwrite = TRUE)

  withr::with_dir(tempdir(), {
    cohortSet <- readCohortSet("cohorts1")
  })
  expect_equal(nrow(cohortSet), 2)
})

test_that("invalid cohort table names give an error", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("duckdb")
  skip_if_not("duckdb" %in% dbToTest)
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main", "main")
  cohortSet <- readCohortSet(system.file("cohorts1", package = "CDMConnector", mustWork = TRUE))
  expect_error(cdm <- generateCohortSet(cdm, cohortSet, name = "4test", overwrite = TRUE))
  expect_error(cdm <- generateCohortSet(cdm, cohortSet, name = "Test", overwrite = TRUE))
  expect_error(cdm <- generateCohortSet(cdm, cohortSet, name = "te$t", overwrite = TRUE))
  cdmDisconnect(cdm)
})


test_that("non-utf8 characters in json files should be handled by readCohortSet without errors", {
  expect_no_error(readCohortSet(system.file("cohortsNonUTF8", package = "CDMConnector", mustWork = TRUE)))
})


test_that("cohort era collapse is recorded in attrition", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm <- cdmFromCon(con, "main", "main")

  # this cohort has a lot of records per person with a large era gap parameter
  cohortSet <- readCohortSet(system.file("cohorts5", package = "CDMConnector"))

  cdm <- generateCohortSet(cdm, cohortSet, name = "cohort")

  actualCohortCount <- cohortCount(cdm$cohort)

  expectedCohortCount <- cdm$cohort %>%
    dplyr::group_by(cohort_definition_id) %>%
    dplyr::summarize(number_records = n(), number_subjects = dplyr::n_distinct(subject_id)) %>%
    dplyr::collect()

  attr(expectedCohortCount, "cohort_set") <- NULL
  attr(expectedCohortCount, "cohort_attrition") <- NULL

  expect_equal(actualCohortCount, expectedCohortCount)

  expect_true("Cohort records collapsed" %in% attrition(cdm$cohort)$reason)

  cdmDisconnect(cdm)
})

