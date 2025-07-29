

test_summarise_quantile <- function(con, write_schema) {

  eunomia_con <- DBI::dbConnect(duckdb::duckdb(eunomiaDir()))

  eunomia_cdm <- cdmFromCon(
    con = eunomia_con, cdmName = "eunomia", cdmSchema = "main",
    writeSchema = "main"
  ) %>%
    cdmSelect("person")

  person <- dplyr::collect(eunomia_cdm$person)
  DBI::dbDisconnect(eunomia_con, shutdown = TRUE)

  tempname <- paste0("temp", floor(10*as.numeric(Sys.Date()) %% 1e6), "_person")

  DBI::dbWriteTable(con, inSchema(write_schema, tempname, dbms = dbms(con)), person)

  person_ref <- dplyr::tbl(con, inSchema(write_schema, tempname, dbms = dbms(con)))

  # summariseQuantile without group by
  actual <- person_ref %>%
    summariseQuantile(year_of_birth,
                      probs = round(seq(0, 1, 0.05), 2),
                      nameSuffix = "quant") %>%
    dplyr::collect() %>%
    tidyr::gather() %>%
    dplyr::pull(value) %>%
    sort()

  expected <- quantile(person$year_of_birth, round(seq(0, 1, 0.05), 2), type = 1) %>%
    unname() %>%
    sort()

  expect_equal(actual, expected)

# summariseQuantile with group by
  actual <- person_ref %>%
    dplyr::group_by(gender_concept_id) %>%
    summariseQuantile(x = year_of_birth,
                    probs = 0.5) %>%
    dplyr::collect() %>%
    dplyr::arrange(gender_concept_id) %>%
    dplyr::pull("p50_value")

  expected <- person %>%
    dplyr::group_by(gender_concept_id) %>%
    dplyr::summarise(p50_value = quantile(year_of_birth, 0.5)) %>%
    dplyr::arrange(gender_concept_id) %>%
    dplyr::pull("p50_value") %>%
    unname()

  expect_equal(actual, expected)
  DBI::dbRemoveTable(con, inSchema(write_schema, tempname, dbms = dbms(con)))

  tempname <- paste0("temp", floor(10*as.numeric(Sys.Date()) %% 1e6))
  DBI::dbWriteTable(con, inSchema(write_schema, tempname, dbms = dbms(con)), mtcars)
  mtcars_tbl <- dplyr::tbl(con, inSchema(write_schema, tempname, dbms = dbms(con)))

  # test summariseQuantile2
  result <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
    summariseQuantile2("mpg", probs = c(0, 0.2, 0.4, 0.6, 0.8, 1),  nameSuffix = "quant") %>%
    dplyr::collect()

  expected <- dplyr::tibble(
    cyl = c(8, 4, 6),
    q00_quant = c(10.4, 21.4, 17.8),
    q20_quant = c(13.3, 22.8, 18.1),
    q40_quant = c(15, 24.4, 19.2),
    q60_quant = c(15.5, 27.3, 21),
    q80_quant = c(17.3, 30.4, 21),
    q100_quant = c(19.2, 33.9, 21.4))

  expect_equal(dplyr::arrange(result, .data$cyl), dplyr::arrange(expected, .data$cyl))

  # multiple columns
  result <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
    summariseQuantile2(c("mpg", "hp", "wt"), probs = c(0.2, 0.8),  nameSuffix = "{x}_quant") %>%
    dplyr::collect()

  expected <- dplyr::tibble(
    cyl = c(8, 4, 6),
    q20_mpg_quant = c(13.3, 22.8, 18.1),
    q80_mpg_quant = c(17.3, 30.4, 21),
    q20_hp_quant = c(175, 65, 110),
    q80_hp_quant = c(245, 97, 123),
    q20_wt_quant = c(3.44, 1.835, 2.77),
    q80_wt_quant = c(5.25, 2.78, 3.44))

  expect_equal(dplyr::arrange(result, .data$cyl), dplyr::arrange(expected, .data$cyl))

  DBI::dbRemoveTable(con, inSchema(write_schema, tempname, dbms = dbms(con)))
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - summariseQuantile"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    prefix <- paste0("test", as.integer(Sys.time()), "_")
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || is.null(con))
    test_summarise_quantile(con, write_schema)
    disconnect(con)
  })
}

