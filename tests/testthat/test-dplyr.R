test_that("Oracle dplyr works", {
  skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
  skip_on_ci() # need development version of dbplyr

  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")

  cdm <- CDMConnector::cdm_from_con(
    con = con,
    cdm_schema = "CDMV5",
    cdm_tables = c(CDMConnector::tbl_group("default"), -visit_detail) # visit_detail is missing in Oracle test database
  )

  df <- cdm$observation_period %>%
    dplyr::mutate(new_date = !!asDate(observation_period_start_date)) %>% #as.Date translation is incorrect
    head() %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  df <- cdm$person %>%
    # dplyr::left_join(cdm$observation_period, by = "person_id") %>% # this fails
    dplyr::left_join(cdm$observation_period, by = "person_id", x_as = "x", y_as = "y") %>%
    head() %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")


  df <- cdm$person %>%
    dplyr::slice_sample(n = 100) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

  cdm$person %>%
    dplyr::select(year_of_birth, month_of_birth, day_of_birth) %>%
    dplyr::mutate(dob = as.Date(paste0(
      .data$year_of_birth, "/",
      .data$month_of_birth, "/",
      .data$day_of_birth
    ))) %>%
    dbplyr::sql_render()

  DBI::dbDisconnect(con)
})




