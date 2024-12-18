

test_summarise_quantile <- function(con,
                                    write_schema) {

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

