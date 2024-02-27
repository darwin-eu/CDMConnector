test_that("cdm_sample works", {
  skip_if_not_installed("duckdb")

  # note that this form of connecting causes the db to be garbage collected when called
  # using the "run tests" button in RStudio IDE `con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())`
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))

  expect_true(DBI::dbIsValid(con))

  cdm <- cdm_from_con(con, "main", "main", cdm_name = "test")

  cdm_sampled <- cdm_sample(cdm, n = 10)

  df <- cdm_sampled$person %>%
    dplyr::tally() %>%
    dplyr::collect()

  expect_equal(as.double(df$n), 10)

  DBI::dbDisconnect(con, shutdown = TRUE)

})

# TODO do we still need this test?

# test_that("cdm_reference assign and unassign", {
#   # if I have a class cdm_reference and I access to one element it will have a
#   # cdm_reference as attribute
#   x <- list(a = 1, b = 2)
#   class(x) <- "cdm_reference"
#   expect_true("cdm_reference" %in% names(attributes(x[["a"]])))
#   expect_true("cdm_reference" %in% names(attributes(x$a)))
#
#   # if I do it for an object that it is not a cdm_reference it wont
#   xu <- unclass(x)
#   expect_false("cdm_reference" %in% names(attributes(xu[["a"]])))
#   expect_false("cdm_reference" %in% names(attributes(xu$a)))
#
#   # I define an element with a cdm reference on it
#   xx <- 3
#   attr(xx, "cdm_reference") <- 4
#   expect_true("cdm_reference" %in% names(attributes(xx)))
#
#   # if I assign this element to a list with no class the attribute will persist
#   xu$c <- xx
#   expect_true("cdm_reference" %in% names(attributes(xu[["c"]])))
#   expect_true("cdm_reference" %in% names(attributes(xu$c)))
#
#   # if I assign to a cdm_reference it wont but it will appear back when I access
#   # to one of the elements
#   x$c <- cars
#   expect_true("cdm_reference" %in% names(attributes(x[["c"]])))
#   expect_true("cdm_reference" %in% names(attributes(x$c)))
#
#   # but if after assigning I remove the class the attribute wont be there
#   # because when I assigned it was eliminated
#   xuu <- unclass(x)
#   expect_false("cdm_reference" %in% names(attributes(xuu[["c"]])))
#   expect_false("cdm_reference" %in% names(attributes(xuu$c)))
# })
