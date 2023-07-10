

test_cohort_generation <- function(con, cdm_schema, write_schema) {

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  inst_dir <- system.file(package = "CDMConnector", mustWork = TRUE)

  # test read cohort set with a cohortsToCreate.csv
  withr::with_dir(inst_dir, {
    cohortSet <- readCohortSet("cohorts1")
  })
  expect_equal(nrow(cohortSet), 2)

  # test readCohortSet without cohortsToCreate.csv
  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  expect_s3_class(cohortSet, "CohortSet")

  # debugonce(generateCohortSet)
  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           overwrite = TRUE,
                           computeAttrition = TRUE)
  # check already exists
  expect_error(generateCohortSet(cdm, cohortSet, name = "chrt0", overwrite = FALSE))

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  # expect_s3_class(dplyr::collect(attrition(cdm$chrt0)), "data.frame")
  expect_true(all(c("cohort_set", "cohort_count", "cohort_attrition") %in% names(attributes(cdm$chrt0))))
  attrition_df <- cohortAttrition(cdm$chrt0)
  expect_s3_class(attrition_df, "data.frame")
  expect_true(nrow(attrition_df) > 0)
  expect_true("excluded_records" %in% names(attrition_df))
  expect_s3_class(cohortSet(cdm$chrt0), "data.frame")
  counts <- cohortCount(cdm$chrt0)
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), 3)

  # cohort table should be lowercase
  expect_error(generateCohortSet(cdm, cohortSet, name = "MYcohorts", overwrite = TRUE))

  # empty data
  expect_error(generateCohortSet(cdm, cohortSet %>% head(0), name = "cohorts", overwrite = TRUE))
  drop_table(cdm, starts_with("chrt0_"))
  expect_length(grep("^chrt0_", listTables(con, schema = write_schema)), 0)
}

dbToTest <- c(
  "duckdb"
  # ,"postgres"
  # ,"redshift"
  # ,"sqlserver"
  # ,"oracle"  # requires development version of dbplyr
  # ,"snowflake" invalid identifier 'COHORT_DEFINITION_ID'
  # ,"bigquery" Type not found: VARCHAR at [4:10] [invalidQuery]
)

# dbtype = "duckdb"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet"), {
    if (dbtype != "duckdb") skip_on_ci()
    skip_if_not_installed("CirceR")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_cohort_generation(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}


test_that("duckdb cohort generation", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  skip("failing test")

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  write_schema <- "main"
  cdm_schema <- "main"

  cdm <- cdm_from_con(con,
                      cdm_schema = cdm_schema,
                      write_schema = write_schema)

  inst_dir <- system.file(package = "CDMConnector", mustWork = TRUE)

  # test read cohort set with a cohortsToCreate.csv
  withr::with_dir(inst_dir, {
    cohortSet <- readCohortSet("cohorts1")
  })
  expect_equal(nrow(cohortSet), 2)

  # test readCohortSet without cohortsToCreate.csv
  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector", mustWork = TRUE))
  expect_equal(nrow(cohortSet), 3)
  expect_s3_class(cohortSet, "CohortSet")

  # debugonce(generateCohortSet)
  cdm <- generateCohortSet(cdm,
                           cohortSet,
                           name = "chrt0",
                           overwrite = TRUE)
  # check already exists
  expect_error(generateCohortSet(cdm, cohortSet, name = "chrt0", overwrite = FALSE))

  expect_true("chrt0" %in% listTables(con, schema = write_schema))

  expect_true("GeneratedCohortSet" %in% class(cdm$chrt0))
  df <- cdm$chrt0 %>% head() %>% dplyr::collect()
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(df)))

  # expect_s3_class(dplyr::collect(attrition(cdm$chrt0)), "data.frame")
  expect_true(all(c("cohort_set", "cohort_count", "cohort_attrition") %in% names(attributes(cdm$chrt0))))
  expect_s3_class(cohortAttrition(cdm$chrt0), "data.frame")
  expect_s3_class(dplyr::collect(cohortSet(cdm$chrt0)), "data.frame")
  counts <- dplyr::collect(cohortCount(cdm$chrt0))
  expect_s3_class(counts, "data.frame")
  expect_equal(nrow(counts), 3)

  # cohort table should be lowercase
  expect_error(generateCohortSet(cdm, cohortSet, name = "MYcohorts", overwrite = TRUE))

  # drop tables
  DBI::dbRemoveTable(con, DBI::Id(schema = "main", table = "chrt0"))
  expect_false("chrt0" %in% listTables(con, schema = write_schema))

  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_count"))
  DBI::dbRemoveTable(con, DBI::Id(schema = write_schema, table = "chrt0_set"))

  # empty data
  expect_error(generateCohortSet(cdm, cohortSet %>% head(0), name = "cohorts", overwrite = TRUE))

})

# Test readCohortSet ----
test_that("ReadCohortSet gives informative error when pointed to a file", {
  path <- system.file("cohorts1", "deepVeinThrombosis01.json", package = "CDMConnector", mustWork = TRUE)
  expect_error(readCohortSet(path), "not a directory")
})

test_that("Generation from Capr Cohorts", {
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("Capr")
  library(Capr)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  cdm <- CDMConnector::cdm_from_con(
    con = con,
    cdm_schema = "main",
    write_schema = "main"
  )

  gibleed_cohort_definition <- cohort(
    entry = condition(cs(descendants(192671))),
    attrition = attrition(
      "no RA" = withAll(
        exactly(0,
                condition(cs(descendants(80809))),
                duringInterval(eventStarts(-Inf, Inf))))
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

  cohort_set <- PhenotypeLibrary::listPhenotypes() %>%
    dplyr::pull("cohortId") %>%
    PhenotypeLibrary::getPlCohortDefinitionSet()

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, "main", write_schema = "main")
  expect_error(
    generate_cohort_set(cdm, cohort_set, name = "cohort", overwrite = TRUE, compute_attrition = TRUE),
    NA
  )
  DBI::dbDisconnect(con, shutdown = T)
})



