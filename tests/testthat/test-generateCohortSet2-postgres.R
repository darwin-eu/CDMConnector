# Live PostgreSQL tests for generateCohortSet2
# Tests against a real PostgreSQL CDM database using both DatabaseConnector and RPostgres

# ---- Test 1: DatabaseConnector ----

test_that("generateCohortSet2 works with DatabaseConnector (PostgreSQL)", {
  skip_if_not_installed("DatabaseConnector")
  skip_if_not_installed("PhenotypeLibrary")
  skip_if_not_installed("snakecase")
  skip_if_not_installed("readr")
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "", "CDM5_POSTGRESQL_SERVER not set")

  library(DatabaseConnector)

  cdmSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  writeSchema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")

  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
  )

  con <- connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(con), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = cdmSchema, writeSchema = writeSchema)

  cohorts <- readr::read_csv(
    system.file("Cohorts.csv", package = "PhenotypeLibrary"),
    show_col_types = FALSE,
    guess_max = Inf
  ) %>%
    dplyr::transmute(
      cohort_definition_id = as.integer(cohortId),
      cohort_name = snakecase::to_snake_case(cohortNameFormatted),
      cohort_name_snakecase = cohort_name
    )

  cohortSet <- readCohortSet(system.file("cohorts", package = "PhenotypeLibrary")) %>%
    dplyr::select(-cohort_name, -cohort_name_snakecase) %>%
    dplyr::left_join(cohorts, by = "cohort_definition_id") %>%
    dplyr::select(cohort_definition_id, cohort_name, cohort, json, cohort_name_snakecase)

  set.seed(1)
  cohortSet <- cohortSet[sample(1:nrow(cohortSet), 10, replace = FALSE), ]

  cdm <- generateCohortSet2(cdm, cohortSet = cohortSet, name = "cohort")

  expect_true("cohort" %in% names(cdm))
  result <- dplyr::collect(cdm$cohort)
  expect_true(is.data.frame(result))
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(result)))
})


# ---- Test 2: RPostgres ----

test_that("generateCohortSet2 works with RPostgres (PostgreSQL)", {
  skip_if_not_installed("RPostgres")
  skip_if_not_installed("PhenotypeLibrary")
  skip_if_not_installed("snakecase")
  skip_if_not_installed("readr")
  skip_if(Sys.getenv("CDM5_POSTGRESQL_HOST") == "", "CDM5_POSTGRESQL_HOST not set")

  cdmSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  writeSchema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")

  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
    host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  cdm <- cdmFromCon(con, cdmSchema = cdmSchema, writeSchema = writeSchema)

  cohorts <- readr::read_csv(
    system.file("Cohorts.csv", package = "PhenotypeLibrary"),
    show_col_types = FALSE,
    guess_max = Inf
  ) %>%
    dplyr::transmute(
      cohort_definition_id = as.integer(cohortId),
      cohort_name = snakecase::to_snake_case(cohortNameFormatted),
      cohort_name_snakecase = cohort_name
    )

  cohortSet <- readCohortSet(system.file("cohorts", package = "PhenotypeLibrary")) %>%
    dplyr::select(-cohort_name, -cohort_name_snakecase) %>%
    dplyr::left_join(cohorts, by = "cohort_definition_id") %>%
    dplyr::select(cohort_definition_id, cohort_name, cohort, json, cohort_name_snakecase)

  set.seed(1)
  cohortSet <- cohortSet[sample(1:nrow(cohortSet), 10, replace = FALSE), ]

  cdm <- generateCohortSet2(cdm, cohortSet = cohortSet, name = "cohort")

  expect_true("cohort" %in% names(cdm))
  result <- dplyr::collect(cdm$cohort)
  expect_true(is.data.frame(result))
  expect_true(all(c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date") %in% names(result)))
})
