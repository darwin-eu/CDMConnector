

test_summarise_quantile <- function(con,
                                    write_schema) {

  eunomia_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  eunomia_cdm <- cdm_from_con(eunomia_con, cdm_schema = "main") %>%
    cdm_select_tbl("person")

  person <- dplyr::collect(eunomia_cdm$person)

  cdm <- copyCdmTo(con = con,
                   cdm = eunomia_cdm,
                   schema = write_schema,
                   overwrite = TRUE)

  DBI::dbDisconnect(eunomia_con, shutdown = TRUE)



  # summariseQuantile without group by
  actual <- cdm$person %>%
    summarise_quantile(year_of_birth,
                       probs = round(seq(0, 1, 0.05), 2),
                       name_suffix = "quant") %>%
    dplyr::collect() %>%
    tidyr::gather() %>%
    dplyr::pull(value) %>%
    sort()

  expected <- quantile(person$year_of_birth, round(seq(0, 1, 0.05), 2), type = 1) %>%
    unname() %>%
    sort()

  expect_equal(actual, expected)

# summariseQuantile with group by
  actual <- cdm$person %>%
    dplyr::group_by(gender_concept_id) %>%
    summarise_quantile(x = year_of_birth,
                    probs = 0.5) %>%
    dplyr::collect() %>%
    dplyr::arrange(gender_concept_id) %>%
    dplyr::pull("p50_value")

  expected <-  person %>%
    dplyr::group_by(gender_concept_id) %>%
    dplyr::summarise(p50_value = quantile(year_of_birth, 0.5)) %>%
    dplyr::arrange(gender_concept_id) %>%
    dplyr::pull("p50_value") %>%
    unname()

  expect_equal(actual, expected)
  DBI::dbRemoveTable(con, inSchema(write_schema, "person", dbms = dbms(con)))
}

# dbtype = "snowflake"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - summariseQuantile"), {
    con <- get_connection(dbtype)
    prefix <- paste0("test", as.integer(Sys.time()), "_")
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || is.null(con))
    test_summarise_quantile(con, write_schema)
    disconnect(con)
  })
}

