# DuckDB with multiple catalogs: main DB + ATTACH read-only CDM
# Tests cdmFromCon with cdmSchema/writeSchema using catalog.schema (e.g. cdm.main, results.main)

test_that("DuckDB multiple catalogs: listTables and cdmFromCon with catalog.schema", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())
  skip_if_not("duckdb" %in% dbToTest)

  # Writable results database (temp file)
  results_db <- tempfile(fileext = ".duckdb")
  on.exit(unlink(results_db, force = TRUE), add = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = results_db)

  # Attach Eunomia CDM as read-only catalog "cdm"
  cdm_path <- eunomiaDir()
  DBI::dbExecute(con, paste0("ATTACH '", cdm_path, "' AS cdm (READ_ONLY)"))

  # Multiple catalogs: main (default) and cdm
  catalogs <- DBI::dbGetQuery(con, "SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name;")
  expect_true("cdm" %in% catalogs$catalog_name)

  # listTables with catalog.schema (string "cdm.main" or c("cdm", "main"))
  cdm_tables <- listTables(con, schema = "cdm.main")
  expect_true("person" %in% tolower(cdm_tables))
  expect_true("concept" %in% tolower(cdm_tables))
  cdm_tables2 <- listTables(con, schema = c(catalog = "cdm", schema = "main"))
  expect_equal(sort(tolower(cdm_tables)), sort(tolower(cdm_tables2)))

  # cdmFromCon with "catalog.schema" form
  cdm <- cdmFromCon(
    con = con,
    cdmSchema = "cdm.main",
    writeSchema = "main",
    cdmName = "SK OMOP",
    cdmVersion = "5.3"
  )
  expect_s3_class(cdm, "cdm_reference")
  expect_true("person" %in% names(cdm))
  expect_true("concept" %in% names(cdm))

  # Read from CDM
  person_head <- dplyr::collect(head(cdm$person, 2))
  expect_s3_class(person_head, "data.frame")
  expect_gte(ncol(person_head), 1)

  # Write to write schema (main)
  small <- cdm$person %>%
    head(1) %>%
    dplyr::compute(name = "test_multi_catalog", temporary = FALSE, overwrite = TRUE)
  expect_s3_class(dplyr::collect(small), "data.frame")
  expect_true("test_multi_catalog" %in% listTables(con, schema = "main"))

  DBI::dbRemoveTable(con, "test_multi_catalog")
  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("DuckDB multiple catalogs: cdmFromCon with named c(catalog=, schema=)", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomiaIsAvailable())
  skip_if_not("duckdb" %in% dbToTest)

  results_db <- tempfile(fileext = ".duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = results_db)

  cdm_path <- eunomiaDir()
  DBI::dbExecute(con, paste0("ATTACH '", cdm_path, "' AS cdm (READ_ONLY)"))

  # Write schema: single "main" for default catalog's main schema (primary connection)
  cdm <- cdmFromCon(
    con = con,
    cdmSchema = c(catalog = "cdm", schema = "main"),
    writeSchema = "main",
    cdmName = "SK OMOP",
    cdmVersion = "5.3"
  )
  expect_s3_class(cdm, "cdm_reference")
  expect_true("person" %in% names(cdm))

  df <- dplyr::collect(head(cdm$concept, 3))
  expect_s3_class(df, "data.frame")
  DBI::dbDisconnect(con, shutdown = TRUE)
  unlink(results_db, force = TRUE)
})
