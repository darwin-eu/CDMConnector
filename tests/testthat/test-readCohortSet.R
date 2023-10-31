
test_that("From csv camelCase", {
  skip("failing test")
  pathCsv <- system.file(package = "CDMConnector", "cohorts1")
  data <- readCohortSet(pathCsv)

  expect_in(
    data$cohort_definition_id,
    c(1, 2)
  )

  expect_in(
    data$cohort_name,
    c("cerebralVenousSinusThrombosis01", "deepVeinThrombosis01")
  )

  expect_identical(nrow(data), 2L)
  expect_identical(ncol(data), 4L)
})

test_that("From csv snake_case", {
  skip("failing test")
  pathCsv <- system.file(package = "CDMConnector", "cohorts1")
  data <- read_cohort_set(pathCsv)

  expect_in(
    data$cohort_definition_id,
    c(1, 2)
  )

  expect_in(
    data$cohort_name,
    c("cerebralVenousSinusThrombosis01", "deepVeinThrombosis01")
  )

  expect_identical(nrow(data), 2L)
  expect_identical(ncol(data), 4L)
})

test_that("From json camelCase", {
  pathJson <- system.file(package = "CDMConnector", "cohorts2")
  data <- readCohortSet(pathJson)

  expect_in(
    data$cohort_definition_id,
    c(1L, 2L, 3L)
  )

  # Unsure what dictates the order
  expect_in(
    data$cohort_name,
    c("cerebralVenousSinusThrombosis01", "deepVeinThrombosis01", "GIBleed_male")
  )

  expect_identical(nrow(data), 3L)
  expect_identical(ncol(data), 4L)
})

test_that("From json snake_case", {
  pathJson <- system.file(package = "CDMConnector", "cohorts2")
  data <- read_cohort_set(pathJson)

  expect_in(
    data$cohort_definition_id,
    c(1L, 2L, 3L)
  )

  expect_in(
    data$cohort_name,
    c("cerebralVenousSinusThrombosis01", "deepVeinThrombosis01", "GIBleed_male")
  )

  expect_identical(nrow(data), 3L)
  expect_identical(ncol(data), 4L)
})
