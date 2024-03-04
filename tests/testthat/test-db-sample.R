test_sample <- function(con, cdm_schema, write_schema) {

  cdm <- cdm_from_con(
    con = con, cdm_name = "test", cdm_schema = cdm_schema,
    write_schema = write_schema
  )

  cdmSampled1 <- cdmSample(cdm = cdm, n = 100, seed = 123)
  expect_true(cdmSampled1$person |> dplyr::tally() |> dplyr::pull() == 100)
  expect_true("person_sample" %in% names(cdmSampled1))
  expect_false("person_sample" %in% names(cdm))

  cdmSampled2 <- cdmSample(cdm = cdm, n = 200, name = "sample_person")
  expect_true(cdmSampled2$person |> dplyr::tally() |> dplyr::pull() == 200)
  expect_true("sample_person" %in% names(cdmSampled2))
  expect_false("person_sample" %in% names(cdmSampled2))

  cdmSampled3 <- cdmSample(cdm = cdm, n = 100, seed = 123, name = "sample3")
  expect_true(cdmSampled3$person |> dplyr::tally() |> dplyr::pull() == 100)
  expect_true("sample3" %in% names(cdmSampled3))

  cdmSampled4 <- cdmSample(cdm = cdm, n = 100, seed = 1234, name = "sample4")
  expect_true(cdmSampled4$person |> dplyr::tally() |> dplyr::pull() == 100)
  expect_true("sample4" %in% names(cdmSampled4))

  expect_true(identical(
    cdmSampled1$person |> dplyr::collect() |> dplyr::arrange(.data$person_id),
    cdmSampled3$person |> dplyr::collect() |> dplyr::arrange(.data$person_id)
  ))

  expect_false(identical(
    cdmSampled1$person |> dplyr::collect() |> dplyr::arrange(.data$person_id),
    cdmSampled4$person |> dplyr::collect() |> dplyr::arrange(.data$person_id)
  ))

}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - sample database"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_sample(con, cdm_schema, write_schema)
    disconnect(con)
  })
}
