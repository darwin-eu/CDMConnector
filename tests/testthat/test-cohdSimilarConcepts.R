test_that("cohdSimilarConcepts rejects invalid conceptId", {
  expect_error(cohdSimilarConcepts("not_a_number"), "not TRUE")
  expect_error(cohdSimilarConcepts(NA), "not TRUE")
  expect_error(cohdSimilarConcepts(character(0)), "not TRUE")
})

test_that("cohdSimilarConcepts accepts valid conceptId (numeric or digit string)", {
  skip_on_cran()
  # Use invalid URL so we don't hit real API; we only check that no input error
  out <- cohdSimilarConcepts(
    201826L,
    baseUrl = "https://invalid-unreachable.example.com/api",
    timeoutSec = 2
  )
  expect_null(out)
})

test_that("cohdSimilarConcepts returns NULL when API is unreachable", {
  skip_on_cran()
  expect_message(
    out <- cohdSimilarConcepts(
      201826,
      baseUrl = "https://invalid-unreachable.example.com/api",
      timeoutSec = 1
    ),
    "COHD API"
  )
  expect_null(out)
})

test_that("cohdSimilarConcepts accepts conceptId as character digits", {
  skip_on_cran()
  out <- cohdSimilarConcepts(
    "201826",
    baseUrl = "https://invalid-unreachable.example.com/api",
    timeoutSec = 1
  )
  expect_null(out)
})

test_that("cohdSimilarConcepts respects topN and datasetId", {
  skip_on_cran()
  skip_if_offline()
  # Only run if COHD API is actually available
  out <- cohdSimilarConcepts(201826, datasetId = 1, topN = 5, timeoutSec = 10)
  if (is.null(out)) {
    skip("COHD API not available")
  }
  expect_s3_class(out, "data.frame")
  expect_true(nrow(out) <= 5)
  expect_true(all(c("concept_id_1", "concept_id_2", "relative_frequency", "other_concept_id") %in% names(out)))
  expect_true(all(!is.na(out$relative_frequency)))
  expect_true(is.numeric(out$relative_frequency))
})
