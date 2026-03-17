test_that("internal cohort era helpers collapse intervals within gap", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())

  con <- local_eunomia_con()
  cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "test")

  cohort_name <- uniqueTableName()
  cohort_input <- dplyr::tibble(
    cohort_definition_id = c(1L, 1L, 1L, 1L, 1L),
    subject_id = c(1L, 1L, 1L, 2L, 2L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-01-12", "2020-01-25", "2020-02-01", "2020-02-08")),
    cohort_end_date = as.Date(c("2020-01-10", "2020-01-20", "2020-01-30", "2020-02-05", "2020-02-10"))
  )
  cdm <- insertTable(cdm = cdm, name = cohort_name, table = cohort_input, overwrite = TRUE)

  expected <- dplyr::tibble(
    cohort_definition_id = c(1L, 1L, 1L, 1L),
    subject_id = c(1L, 1L, 2L, 2L),
    cohort_start_date = as.Date(c("2020-01-01", "2020-01-25", "2020-02-01", "2020-02-08")),
    cohort_end_date = as.Date(c("2020-01-20", "2020-01-30", "2020-02-05", "2020-02-10"))
  )

  actual <- CDMConnector:::cohortErafy(cdm[[cohort_name]], gap = 2L) %>%
    dplyr::collect() %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date)

  alias_actual <- CDMConnector:::cohort_erafy(cdm[[cohort_name]], gap = 2L) %>%
    dplyr::collect() %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date)

  expect_equal(actual, expected)
  expect_equal(alias_actual, expected)
})

test_that("internal cohort_pad_end extends intervals and removes invalid rows", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())

  con <- local_eunomia_con()
  cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "test")

  cohort_name <- uniqueTableName()
  cohort_input <- dplyr::tibble(
    cohort_definition_id = c(1L, 1L),
    subject_id = c(1L, 1L),
    cohort_start_date = as.Date(c("2020-03-01", "2020-03-20")),
    cohort_end_date = as.Date(c("2020-03-10", "2020-03-20"))
  )
  cdm <- insertTable(cdm = cdm, name = cohort_name, table = cohort_input, overwrite = TRUE)

  expected <- dplyr::tibble(
    cohort_definition_id = c(1L, 1L),
    subject_id = c(1L, 1L),
    cohort_start_date = as.Date(c("2020-03-01", "2020-03-20")),
    cohort_end_date = as.Date(c("2020-03-12", "2020-03-22"))
  )

  actual <- CDMConnector:::cohort_pad_end(cdm[[cohort_name]], days = 2L) %>%
    dplyr::collect() %>%
    dplyr::arrange(.data$subject_id, .data$cohort_start_date)

  expect_equal(actual, expected)
  expect_error(
    CDMConnector:::cohort_pad_end(cdm[[cohort_name]], days = -1L, from = "start"),
    "cohort_end_date cannot be before cohort_start_date!"
  )
})
