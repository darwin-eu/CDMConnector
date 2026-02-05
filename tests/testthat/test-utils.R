testthat::test_that("cdmCommentContents returns NULL invisibly when non-interactive", {
  # If you don't have mockery, install it or replace with your preferred mocking approach.
  testthat::skip_if_not_installed("mockery")

  mockery::stub(cdmCommentContents, "interactive", FALSE)

  out <- cdmCommentContents(cdm = list())
  testthat::expect_null(out)
})

testthat::test_that("cdmCommentContents validates personIds type", {
  testthat::skip_if_not_installed("mockery")

  mockery::stub(cdmCommentContents, "interactive", TRUE)
  mockery::stub(cdmCommentContents, "requireNamespace", TRUE)
  mockery::stub(cdmCommentContents, "rstudioapi::isAvailable", TRUE)

  testthat::expect_error(
    cdmCommentContents(cdm = list(), personIds = "not numeric"),
    "`personIds` must be a numeric vector"
  )
})

testthat::test_that("cdmCommentContents formats comment lines and inserts below call line", {
  testthat::skip_if_not_installed("mockery")

  # Capture insertText() calls
  captured <- new.env(parent = emptyenv())
  captured$called <- FALSE
  captured$location <- NULL
  captured$text <- NULL

  # --- Mocks to drive the function down the main happy path ---
  mockery::stub(cdmCommentContents, "interactive", TRUE)
  mockery::stub(cdmCommentContents, "requireNamespace", TRUE)
  mockery::stub(cdmCommentContents, "rstudioapi::isAvailable", TRUE)

  # Fake data returned by the pipeline (cdmFlatten |> collect)
  flat_df <- data.frame(
    person_id   = c(2, 1, 1),
    start_date  = as.Date(c("2020-01-01", "2021-02-01", "2020-02-01")),
    end_date    = as.Date(c("2020-01-10", "2021-02-10", NA)),
    concept_id  = c(100, 200, 201),
    stringsAsFactors = FALSE
  )

  mockery::stub(cdmCommentContents, "CDMConnector::cdmFlatten", function(cdm) flat_df)
  mockery::stub(cdmCommentContents, "dplyr::collect", function(x) x)

  # Let dplyr handle filter/arrange if installed; otherwise stub them too.
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    mockery::stub(cdmCommentContents, "dplyr::filter", function(.data, ...) {
      # minimal filter on person_id for this test
      .data[.data$person_id %in% c(1), , drop = FALSE]
    })
    mockery::stub(cdmCommentContents, "dplyr::arrange", function(.data, ...) .data)
    mockery::stub(cdmCommentContents, "dplyr::desc", function(x) x)
  }

  # Provide a document with a call line above the cursor.
  # Cursor row = 4, call row = 2 -> insert at row 3.
  ctx <- list(
    contents = list(
      "x <- 1",
      "cdmCommentContents(cdm, personIds = 1)",
      "y <- 2",
      "z <- 3"
    ),
    selection = list(list(range = list(start = list(row = 4, column = 1))))
  )
  mockery::stub(cdmCommentContents, "rstudioapi::getActiveDocumentContext", function() ctx)

  # rstudioapi helpers used by the function
  mockery::stub(cdmCommentContents, "rstudioapi::document_position", function(row, column) {
    list(row = row, column = column)
  })
  mockery::stub(cdmCommentContents, "rstudioapi::insertText", function(location, text) {
    captured$called <- TRUE
    captured$location <- location
    captured$text <- text
    invisible(NULL)
  })

  # --- Run ---
  out <- cdmCommentContents(cdm = list(), personIds = 1)

  # --- Expectations ---
  testthat::expect_true(captured$called)

  # Insert happens immediately below the call line (call row 2 -> insert at row 3 col 1)
  testthat::expect_identical(captured$location$row, 3)
  testthat::expect_identical(captured$location$column, 1)

  # Text looks like a comment block with a header + rows and ends with a newline
  testthat::expect_match(captured$text, "^# ")
  testthat::expect_match(captured$text, "person_id\\s+\\|\\s+start_date\\s+\\|\\s+end_date")
  testthat::expect_match(captured$text, "\n$")

  # Should include only person_id == 1 rows (since personIds=1)
  testthat::expect_match(captured$text, "# 1\\s+\\|")
  testthat::expect_false(grepl("# 2\\s+\\|", captured$text, fixed = FALSE))

  # Function returns flat (invisibly), but we can still check the value
  testthat::expect_s3_class(out, "data.frame")
  testthat::expect_true(all(out$person_id == 1))
})

testthat::test_that("cdmCommentContents errors if no call line is found above the cursor", {
  testthat::skip_if_not_installed("mockery")

  mockery::stub(cdmCommentContents, "interactive", TRUE)
  mockery::stub(cdmCommentContents, "requireNamespace", TRUE)
  mockery::stub(cdmCommentContents, "rstudioapi::isAvailable", TRUE)

  flat_df <- data.frame(
    person_id   = 1,
    start_date  = as.Date("2021-01-01"),
    end_date    = as.Date("2021-01-02"),
    stringsAsFactors = FALSE
  )
  mockery::stub(cdmCommentContents, "CDMConnector::cdmFlatten", function(cdm) flat_df)
  mockery::stub(cdmCommentContents, "dplyr::collect", function(x) x)

  # No line contains cdmCommentContents(
  ctx <- list(
    contents = list("a <- 1", "b <- 2", "c <- 3"),
    selection = list(list(range = list(start = list(row = 3, column = 1))))
  )
  mockery::stub(cdmCommentContents, "rstudioapi::getActiveDocumentContext", function() ctx)

  testthat::expect_error(
    cdmCommentContents(cdm = list()),
    "Couldn't find a line containing `cdmCommentContents\\(` above the cursor"
  )
})
