
test_cohort_generation <- function(con, cdm_schema, write_schema) {
  skip_if_not_installed("CirceR")

  cdm <- cdm_from_con(con, cdm_schema, write_schema)

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

  expect_true(methods::is(cdm$chrt0, "GeneratedCohortSet"))

  expect_error(generateCohortSet(cdm, name = "blah", cohortSet = "not a cohort"))

  # check already exists
  expect_error(generateCohortSet(cdm, cohortSet, name = "chrt0", overwrite = FALSE))
  expect_no_error(cdm <- generate_cohort_set(cdm,
                                             cohort_set =  cohortSet,
                                             name = "chrt0",
                                             overwrite = TRUE))

  expect_true("chrt0" %in% tolower(listTables(con, schema = write_schema)))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))

  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% colnames(df)))

  expect_true(all(c("cohort_set", "cohort_attrition", "cdm_reference") %in% names(attributes(cdm$chrt0))))
  expect_warning(cohortAttrition(cdm$chrt0))
  attrition_df <- attrition(cdm$chrt0)
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_warning(cohort_set(cdm$chrt0)) # deprecation warning
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
  test_that(glue::glue("{dbtype} - generateCohortSet"), {
    skip_if_not_installed("CirceR")
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_cohort_generation(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}

test_that("Generation from Capr Cohorts", {
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("Capr", minimum_version = "2.0.5")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")

  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )

  gibleed_cohort_definition <- Capr::cohort(
    entry = Capr::conditionOccurrence(Capr::cs(Capr::descendants(192671), name = "test")),
    attrition = Capr::attrition(
      "no RA" = Capr::withAll(
        Capr::exactly(0,
                      Capr::conditionOccurrence(Capr::cs(Capr::descendants(80809), name = "test")),
                      Capr::duringInterval(Capr::eventStarts(-Inf, Inf))))
    )
  )

  cdm <- generateCohortSet(
    cdm,
    list(gibleed = gibleed_cohort_definition),
    name = "gibleed",
    overwrite = TRUE
  )

  expect_gt(nrow(dplyr::collect(cdm$gibleed)), 10)
  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("duckdb - phenotype library generation", {
  skip("manual test")
  skip_if_not_installed("PhenotypeLibrary")

  cohort_set <- PhenotypeLibrary::listPhenotypes() %>%
    dplyr::pull("cohortId") %>%
    PhenotypeLibrary::getPlCohortDefinitionSet()

  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )
  expect_error(
    generate_cohort_set(cdm, cohort_set, name = "cohort", overwrite = TRUE, compute_attrition = TRUE),
    NA
  )
  DBI::dbDisconnect(con, shutdown = T)
})

test_that("TreatmentPatterns cohort works", {
  skip_if_not_installed("TreatmentPatterns")
  skip_on_cran()
  skip("manual test")
  skip("failing test")
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
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

  expect_s3_class(cdm$cohorttable, "GeneratedCohortSet")
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
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))

  write_schema <- c(schema = "main", prefix = "test_")
  cdm <- cdm_from_con(con, "main", write_schema, cdm_name = "eunomia")

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

  expect_true("cohort" %in% list_tables(con, write_schema))
  expect_true("test_cohort" %in% list_tables(con, "main"))

  expect_s3_class(cohort_count(cdm$cohort), "data.frame")
  expect_warning(cohort_set(cdm$cohort), "deprecated")
  expect_s3_class(settings(cdm$cohort), "data.frame")
  expect_warning(cohort_attrition(cdm$cohort), "deprecated")

  expect_s3_class(attrition(cdm$cohort), "data.frame")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

# issue: https://github.com/darwin-eu-dev/CDMConnector/issues/337

test_that("no error is given if attrition table already exists and overwrite = TRUE", {
  skip_if_not_installed("CirceR")
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
  cdm <- cdm_from_con(
    con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main"
  )
  cohort_set <- read_cohort_set(system.file("cohorts1", package = "CDMConnector"))

  cdm <- generateCohortSet(cdm,
                           cohort_set,
                           name = "test",
                           computeAttrition = TRUE)

  expect_no_error({
    cdm <- generateCohortSet(cdm,
                             cohort_set,
                             name = "test",
                             computeAttrition = FALSE,
                             overwrite = TRUE)
  })

  expect_no_error({
    cdm <- generateCohortSet(cdm,
                             cohort_set,
                             name = "test",
                             computeAttrition = TRUE,
                             overwrite = TRUE)
  })

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("readCohortSet works from working directory", {
  skip("manual test") # seems to work as a standalone test but fails when running testtat
  fs::dir_copy(system.file("cohorts1", package = "CDMConnector", mustWork = TRUE),
               file.path(tempdir(), "cohorts1"),
               overwrite = TRUE)

  withr::with_dir(tempdir(), {
    cohortSet <- readCohortSet("cohorts1")
  })
  expect_equal(nrow(cohortSet), 2)
})
