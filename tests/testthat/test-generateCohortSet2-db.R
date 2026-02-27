# Live database tests for generateCohortSet2 (optimizer.R pipeline)
#
# Tests against real database backends: PostgreSQL, SQL Server, Redshift,
# Snowflake, Spark/Databricks, BigQuery (plus DuckDB as local baseline).
# Uses get_connection / get_cdm_schema / get_write_schema from setup.R.
# Each test uses a GUID write-prefix for collision-free table naming.

# Helper: generate a short GUID-based prefix (max ~24 chars to stay within
# identifier length limits on all backends).
guid_prefix <- function() {
  # 8 hex chars from a UUID-style random value + "x" leader for valid SQL ident
  hex <- paste0(
    format(as.hexmode(sample.int(.Machine$integer.max, 1)), width = 8),
    format(as.hexmode(sample.int(.Machine$integer.max, 1)), width = 8)
  )
  paste0("x", substr(hex, 1, 15), "_")
}

# ---- Basic cohort generation across all backends ----

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 basic"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")

    prefix <- guid_prefix()
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    on.exit(disconnect(con), add = TRUE)

    cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

    cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort")

    expect_true("cohort" %in% names(cdm))
    result <- dplyr::collect(cdm$cohort)
    expect_true(is.data.frame(result))
    expect_true(nrow(result) > 0)
    expect_true(all(c("cohort_definition_id", "subject_id",
                       "cohort_start_date", "cohort_end_date") %in% names(result)))

    # Verify settings, attrition, and counts are populated
    s <- omopgenerics::settings(cdm$cohort)
    expect_true(is.data.frame(s))
    expect_equal(nrow(s), nrow(cs))

    att <- omopgenerics::attrition(cdm$cohort)
    expect_true(is.data.frame(att))
    expect_true(nrow(att) > 0)

    cnt <- omopgenerics::cohortCount(cdm$cohort)
    expect_true(is.data.frame(cnt))
    expect_true(nrow(cnt) > 0)

    # Cleanup output tables
    result_schema_str <- CDMConnector:::normalize_schema_str(write_schema)
    for (tbl_name in c("cohort", "cohort_set", "cohort_attrition",
                       "cohort_codelist", "inclusion_events", "inclusion_stats")) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}

# ---- Attrition with inclusion rules ----

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 attrition"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")

    prefix <- guid_prefix()
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    on.exit(disconnect(con), add = TRUE)

    cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

    cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort",
                               computeAttrition = TRUE)

    att <- omopgenerics::attrition(cdm$cohort)
    expect_true(is.data.frame(att))
    expect_true(nrow(att) > 0)
    expect_true(all(c("cohort_definition_id", "number_records", "number_subjects",
                       "reason_id", "reason") %in% names(att)))

    # GIBleed_male has 2 inclusion rules (Male, 30 days prior observation)
    s <- omopgenerics::settings(cdm$cohort)
    gibleed_id <- s$cohort_definition_id[s$cohort_name == "gibleed_male"]
    if (length(gibleed_id) == 1) {
      att_gb <- att[att$cohort_definition_id == gibleed_id, ]
      expect_true(nrow(att_gb) >= 3)
      expect_equal(att_gb$reason[1], "Qualifying initial records")
    }

    # Cleanup
    for (tbl_name in c("cohort", "cohort_set", "cohort_attrition",
                       "cohort_codelist", "inclusion_events", "inclusion_stats")) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}

# ---- Overwrite behavior ----

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 overwrite"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")

    prefix <- guid_prefix()
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    on.exit(disconnect(con), add = TRUE)

    cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

    cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

    # First run
    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort", overwrite = TRUE)
    expect_true("cohort" %in% names(cdm))
    first_result <- dplyr::collect(cdm$cohort)

    # Second run with overwrite=TRUE should succeed
    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort", overwrite = TRUE)
    expect_true("cohort" %in% names(cdm))
    second_result <- dplyr::collect(cdm$cohort)
    expect_equal(nrow(first_result), nrow(second_result))

    # overwrite=FALSE should error
    expect_error(
      generateCohortSet2(cdm, cohortSet = cs, name = "cohort", overwrite = FALSE),
      "already exists"
    )

    # Cleanup
    for (tbl_name in c("cohort", "cohort_set", "cohort_attrition",
                       "cohort_codelist", "inclusion_events", "inclusion_stats")) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}

# ---- PhenotypeLibrary cohorts (larger batch, diverse concept domains) ----

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 PhenotypeLibrary batch"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    skip_if_not_installed("PhenotypeLibrary")
    skip_if_not_installed("snakecase")
    skip_if_not_installed("readr")

    prefix <- guid_prefix()
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    on.exit(disconnect(con), add = TRUE)

    cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

    cohorts <- readr::read_csv(
      system.file("Cohorts.csv", package = "PhenotypeLibrary"),
      show_col_types = FALSE,
      guess_max = Inf
    ) %>%
      dplyr::transmute(
        cohort_definition_id = as.integer(cohortId),
        cohort_name = snakecase::to_snake_case(cohortNameFormatted),
        cohort_name_snakecase = cohort_name
      )

    cohortSet <- readCohortSet(system.file("cohorts", package = "PhenotypeLibrary")) %>%
      dplyr::select(-cohort_name, -cohort_name_snakecase) %>%
      dplyr::left_join(cohorts, by = "cohort_definition_id") %>%
      dplyr::select(cohort_definition_id, cohort_name, cohort, json, cohort_name_snakecase)

    set.seed(1)
    cohortSet <- cohortSet[sample(seq_len(nrow(cohortSet)), 10, replace = FALSE), ]

    cdm <- generateCohortSet2(cdm, cohortSet = cohortSet, name = "cohort")

    expect_true("cohort" %in% names(cdm))
    result <- dplyr::collect(cdm$cohort)
    expect_true(is.data.frame(result))
    expect_true(all(c("cohort_definition_id", "subject_id",
                       "cohort_start_date", "cohort_end_date") %in% names(result)))

    s <- omopgenerics::settings(cdm$cohort)
    expect_equal(nrow(s), 10)

    # Cleanup
    for (tbl_name in c("cohort", "cohort_set", "cohort_attrition",
                       "cohort_codelist", "inclusion_events", "inclusion_stats")) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}

# ---- computeAttrition=FALSE ----

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 computeAttrition=FALSE"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")

    prefix <- guid_prefix()
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    on.exit(disconnect(con), add = TRUE)

    cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

    cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort",
                               computeAttrition = FALSE)

    expect_true("cohort" %in% names(cdm))
    result <- dplyr::collect(cdm$cohort)
    expect_true(is.data.frame(result))
    expect_true(nrow(result) > 0)

    # With computeAttrition=FALSE, attrition should have only 1 row per cohort
    att <- omopgenerics::attrition(cdm$cohort)
    expect_true(is.data.frame(att))
    ids <- unique(att$cohort_definition_id)
    for (id in ids) {
      expect_equal(nrow(att[att$cohort_definition_id == id, ]), 1)
    }

    # Cleanup
    for (tbl_name in c("cohort", "cohort_set", "cohort_attrition",
                       "cohort_codelist", "inclusion_events", "inclusion_stats")) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}

# ---- CDM subset support (cdmSubset + generateCohortSet2) ----
# Only on DuckDB for now — cdmSubset requires writeSchema for temp tables
# and dplyr filter subqueries may have dialect-specific issues on some backends.

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 with cdmSubset"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")

    prefix <- guid_prefix()
    con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    on.exit(disconnect(con), add = TRUE)

    cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

    # Get a few person IDs to subset to
    some_persons <- cdm$person %>%
      dplyr::select("person_id") %>%
      utils::head(5) %>%
      dplyr::pull("person_id")

    skip_if(length(some_persons) < 5, "Not enough persons in CDM for subset test")

    cdm_sub <- cdmSubset(cdm, personId = some_persons)

    # Verify subset worked
    n_person <- cdm_sub$person %>% dplyr::tally() %>% dplyr::pull("n")
    expect_true(n_person <= 5)

    cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))

    cdm_sub <- generateCohortSet2(cdm_sub, cohortSet = cs, name = "cohort")

    expect_true("cohort" %in% names(cdm_sub))
    result <- dplyr::collect(cdm_sub$cohort)
    expect_true(is.data.frame(result))
    expect_true(all(c("cohort_definition_id", "subject_id",
                       "cohort_start_date", "cohort_end_date") %in% names(result)))

    # All results must be within the subset
    if (nrow(result) > 0) {
      expect_true(all(result$subject_id %in% some_persons))
    }

    # Cleanup
    for (tbl_name in c("cohort", "cohort_set", "cohort_attrition",
                       "cohort_codelist", "inclusion_events", "inclusion_stats")) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}
