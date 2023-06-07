
test_that("subsetting cdm tables", {
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con)

  # $ always returns a `tbl`
  expect_s3_class(attr(cdm$person, "cdm_reference"), "cdm_reference")

  # double bracket always returns a `tbl`
  expect_s3_class(attr(cdm[["person"]], "cdm_reference"), "cdm_reference")
  expect_s3_class(attr(cdm[[1]], "cdm_reference"), "cdm_reference")

  # single bracket always returns a cdm
  expect_s3_class(cdm[1], "cdm_reference")
  expect_s3_class(cdm[1:3], "cdm_reference")
  expect_s3_class(cdm["person"], "cdm_reference")
  expect_s3_class(cdm[c("person", "concept")], "cdm_reference")


  # dplyr verbs retain cdm_attribute
  cdm2 <- cdm$person %>%
    dplyr::mutate(a = 1) %>%
    dplyr::filter(.data$person_id < 100) %>%
    dplyr::left_join(cdm$death, by = "person_id") %>%
    dplyr::arrange(.data$person_id) %>%
    attr("cdm_reference")

  expect_s3_class(cdm2, "cdm_reference")

  # cdm_reference is not retained after collect
  expect_null(attr(collect(cdm$person), "cdm_reference"))

  # cdm_reference attribute is retained after computeQuery
  cdm2 <- cdm$person %>%
    dplyr::mutate(a = 1) %>%
    dplyr::filter(.data$person_id < 100) %>%
    dplyr::left_join(cdm$death, by = "person_id") %>%
    dplyr::arrange(.data$person_id) %>%
    computeQuery() %>%
    attr("cdm_reference")

  expect_s3_class(cdm2, "cdm_reference")

  DBI::dbDisconnect(con, shutdown = TRUE)
})








