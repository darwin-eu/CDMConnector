# testthat::test_that("cdmCommentContents returns NULL invisibly when non-interactive", {
#   skip_on_cran()
#   skip_on_ci()
#   testthat::local_mocked_bindings(
#     .cdm_comment_interactive = function() FALSE,
#     .package = "CDMConnector"
#   )
#   out <- cdmCommentContents(cdm = list())
#   testthat::expect_null(out)
# })
#
# testthat::test_that("cdmCommentContents validates personIds type", {
#   skip_on_cran()
#   skip_on_ci()
#   testthat::local_mocked_bindings(
#     .cdm_comment_interactive = function() TRUE,
#     .cdm_comment_require_rstudioapi = function() TRUE,
#     .cdm_comment_rstudio_available = function() TRUE,
#     .package = "CDMConnector"
#   )
#   testthat::expect_error(
#     cdmCommentContents(cdm = list(), personIds = "not numeric"),
#     "`personIds` must be a numeric vector"
#   )
# })
#
# testthat::test_that("cdmCommentContents formats comment lines and inserts below call line", {
#   skip_on_cran()
#   skip_on_ci()
#   captured <- new.env(parent = emptyenv())
#   captured$called <- FALSE
#   captured$location <- NULL
#   captured$text <- NULL
#
#   flat_df <- data.frame(
#     person_id   = c(2, 1, 1),
#     start_date  = as.Date(c("2020-01-01", "2021-02-01", "2020-02-01")),
#     end_date    = as.Date(c("2020-01-10", "2021-02-10", NA)),
#     concept_id  = c(100, 200, 201),
#     stringsAsFactors = FALSE
#   )
#
#   ctx <- list(
#     contents = list(
#       "x <- 1",
#       "cdmCommentContents(cdm, personIds = 1)",
#       "y <- 2",
#       "z <- 3"
#     ),
#     selection = list(list(range = list(start = list(row = 4, column = 1))))
#   )
#
#   testthat::local_mocked_bindings(
#     .cdm_comment_interactive = function() TRUE,
#     .cdm_comment_require_rstudioapi = function() TRUE,
#     .cdm_comment_rstudio_available = function() TRUE,
#     .cdm_comment_flatten = function(cdm) flat_df,
#     .cdm_comment_collect = function(x) x,
#     .cdm_comment_get_context = function() ctx,
#     .cdm_comment_doc_position = function(row, column) list(row = row, column = column),
#     .cdm_comment_insert_text = function(location, text) {
#       captured$called <- TRUE
#       captured$location <- location
#       captured$text <- text
#       invisible(NULL)
#     },
#     .package = "CDMConnector"
#   )
#
#   out <- cdmCommentContents(cdm = list(), personIds = 1)
#
#   testthat::expect_true(captured$called)
#   testthat::expect_identical(captured$location$row, 3)
#   testthat::expect_identical(captured$location$column, 1)
#   testthat::expect_match(captured$text, "^# ")
#   testthat::expect_match(captured$text, "person_id\\s+\\|\\s+start_date\\s+\\|\\s+end_date")
#   testthat::expect_match(captured$text, "\n$")
#   testthat::expect_match(captured$text, "# 1\\s+\\|")
#   testthat::expect_false(grepl("# 2\\s+\\|", captured$text, fixed = FALSE))
#   testthat::expect_s3_class(out, "data.frame")
#   testthat::expect_true(all(out$person_id == 1))
# })
#
# testthat::test_that("cdmCommentContents errors if no call line is found above the cursor", {
#   skip_on_cran()
#   skip_on_ci()
#   flat_df <- data.frame(
#     person_id   = 1,
#     start_date  = as.Date("2021-01-01"),
#     end_date    = as.Date("2021-01-02"),
#     stringsAsFactors = FALSE
#   )
#   ctx <- list(
#     contents = list("a <- 1", "b <- 2", "c <- 3"),
#     selection = list(list(range = list(start = list(row = 3, column = 1))))
#   )
#
#   testthat::local_mocked_bindings(
#     .cdm_comment_interactive = function() TRUE,
#     .cdm_comment_require_rstudioapi = function() TRUE,
#     .cdm_comment_rstudio_available = function() TRUE,
#     .cdm_comment_flatten = function(cdm) flat_df,
#     .cdm_comment_collect = function(x) x,
#     .cdm_comment_get_context = function() ctx,
#     .package = "CDMConnector"
#   )
#
#   testthat::expect_error(
#     cdmCommentContents(cdm = st()),
#     "Couldn't find a line containing `cdmCommentContents\\(` above the cursor"
#   )
# })
