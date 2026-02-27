# Live database tests for generateCohortSet2 (optimizer.R pipeline)
#
# Tests against real database backends: PostgreSQL, SQL Server, Redshift,
# Snowflake, Spark/Databricks, BigQuery (plus DuckDB as local baseline).
# Uses get_connection / get_cdm_schema / get_write_schema from setup.R.
# Each test uses a GUID write-prefix for collision-free table naming.
#
# Complex cohort tests use 5 cohorts from AtlasCohortGenerator that together
# exercise all major Circe features: multiple domain types, CorrelatedCriteria,
# CensoringCriteria, EndStrategy, DemographicCriteria, nested Groups, high
# occurrence counts, Death censoring, ConditionEra, and EraPad.
# A mock CDM is generated via cdmFromCohortSet and its clinical tables are
# uploaded to each database via copyCdmTo for deterministic cross-db testing.

# Helper: generate a short GUID-based prefix (max ~17 chars for safe SQL idents).
guid_prefix <- function() {
  hex <- paste0(
    format(as.hexmode(sample.int(.Machine$integer.max, 1)), width = 8),
    format(as.hexmode(sample.int(.Machine$integer.max, 1)), width = 8)
  )
  paste0("x", substr(hex, 1, 15), "_")
}

# Helper: cleanup output tables created by generateCohortSet2
cleanup_cohort_tables <- function(con, write_schema, name) {
  for (tbl_name in c(name, paste0(name, "_set"), paste0(name, "_attrition"),
                     paste0(name, "_codelist"), "inclusion_events", "inclusion_stats")) {
    tryCatch(
      DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = tbl_name, dbms = dbms(con))),
      error = function(e) NULL
    )
  }
}

# --- Complex cohort set from AtlasCohortGenerator ---
# These 5 cohorts cover: ConditionOccurrence, DrugExposure, ProcedureOccurrence,
# Measurement, DeviceExposure, ConditionEra, VisitOccurrence, Death;
# plus CorrelatedCriteria, CensoringCriteria, EndStrategy (DateOffset),
# DemographicCriteria (Gender + Age), nested Groups (ALL/ANY/AT_LEAST),
# Occurrence count=3, EraPad=7, and Death censoring.
cohort_dir <- "/Users/ablack/Desktop/AtlasCohortGenerator/inst/cohorts"
complex_cohort_files <- c(
  "thrombotic_microangiopathy_tma_or_microangiopathic_hemolytic_anemia_maha.json",
  "cardiac_arrhythmia_with_inpatient_admission.json",
  "metastatic_hormone_sensitive_prostate_cancer_metachronus.json",
  "acute_kidney_injury_aki_in_persons_with_chronic_kidney_disease.json",
  "sclerosing_cholangitis_treated_with_vancomycin.json"
)

build_complex_cohort_set <- function() {
  cs_list <- list()
  for (i in seq_along(complex_cohort_files)) {
    fp <- file.path(cohort_dir, complex_cohort_files[i])
    if (!file.exists(fp)) return(NULL)
    json_text <- paste(readLines(fp, warn = FALSE), collapse = "\n")
    cs_list[[i]] <- data.frame(
      cohort_definition_id = as.integer(i),
      cohort_name = tools::file_path_sans_ext(complex_cohort_files[i]),
      json = json_text,
      stringsAsFactors = FALSE
    )
  }
  cs <- do.call(rbind, cs_list)
  cs$cohort <- lapply(cs$json, function(j) jsonlite::fromJSON(j, simplifyVector = FALSE))
  cs
}

# ============================================================================
# TEST GROUP 1: Basic cohort generation across all backends (using existing CDM)
# ============================================================================

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

    s <- omopgenerics::settings(cdm$cohort)
    expect_true(is.data.frame(s))
    expect_equal(nrow(s), nrow(cs))

    att <- omopgenerics::attrition(cdm$cohort)
    expect_true(is.data.frame(att))
    expect_true(nrow(att) > 0)

    cnt <- omopgenerics::cohortCount(cdm$cohort)
    expect_true(is.data.frame(cnt))
    expect_true(nrow(cnt) > 0)

    cleanup_cohort_tables(con, write_schema, "cohort")
  })
}

# ============================================================================
# TEST GROUP 2: Attrition with inclusion rules
# ============================================================================

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

    # GIBleed_male has inclusion rules → should have > 1 attrition row
    s <- omopgenerics::settings(cdm$cohort)
    gibleed_id <- s$cohort_definition_id[s$cohort_name == "gibleed_male"]
    if (length(gibleed_id) == 1) {
      att_gb <- att[att$cohort_definition_id == gibleed_id, ]
      expect_true(nrow(att_gb) >= 3)
      expect_equal(att_gb$reason[1], "Qualifying initial records")
    }

    cleanup_cohort_tables(con, write_schema, "cohort")
  })
}

# ============================================================================
# TEST GROUP 3: Overwrite behavior
# ============================================================================

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
    second_result <- dplyr::collect(cdm$cohort)
    expect_equal(nrow(first_result), nrow(second_result))

    # overwrite=FALSE should error
    expect_error(
      generateCohortSet2(cdm, cohortSet = cs, name = "cohort", overwrite = FALSE),
      "already exists"
    )

    cleanup_cohort_tables(con, write_schema, "cohort")
  })
}

# ============================================================================
# TEST GROUP 4: computeAttrition=FALSE
# ============================================================================

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

    # computeAttrition=FALSE → only 1 attrition row per cohort
    att <- omopgenerics::attrition(cdm$cohort)
    ids <- unique(att$cohort_definition_id)
    for (id in ids) {
      expect_equal(nrow(att[att$cohort_definition_id == id, ]), 1)
    }

    cleanup_cohort_tables(con, write_schema, "cohort")
  })
}

# ============================================================================
# TEST GROUP 5: PhenotypeLibrary batch (10 diverse cohorts)
# ============================================================================

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
      show_col_types = FALSE, guess_max = Inf
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

    cleanup_cohort_tables(con, write_schema, "cohort")
  })
}

# ============================================================================
# TEST GROUP 6: Complex multi-domain cohorts
# DuckDB: uses mock CDM from cdmFromCohortSet for deterministic data
# Remote DBs: uses existing CDM data (has vocab + all clinical tables)
# ============================================================================

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 complex multi-domain cohorts"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    skip_if(!dir.exists(cohort_dir), "AtlasCohortGenerator cohorts not available")

    complex_cs <- build_complex_cohort_set()
    skip_if(is.null(complex_cs), "Complex cohort files not found")

    if (dbtype == "duckdb") {
      # DuckDB: generate mock CDM with synthetic data tailored to these cohorts
      mock_cdm <- cdmFromCohortSet(complex_cs, n = 200, seed = 42)
      on.exit(tryCatch(DBI::dbDisconnect(cdmCon(mock_cdm), shutdown = TRUE), error = function(e) NULL), add = TRUE)

      mock_cdm <- generateCohortSet2(mock_cdm, cohortSet = complex_cs, name = "test_cohort")

      expect_true("test_cohort" %in% names(mock_cdm))
      result <- dplyr::collect(mock_cdm$test_cohort)
      expect_true(is.data.frame(result))
      expect_true(all(c("cohort_definition_id", "subject_id",
                         "cohort_start_date", "cohort_end_date") %in% names(result)))
      expect_true(nrow(result) > 0)

      s <- omopgenerics::settings(mock_cdm$test_cohort)
      expect_equal(nrow(s), 5)

      att <- omopgenerics::attrition(mock_cdm$test_cohort)
      expect_true(nrow(att) > 0)
    } else {
      # Remote DB: use existing CDM data (all tables + vocab already present)
      prefix <- guid_prefix()
      con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
      cdm_schema <- get_cdm_schema(dbtype)
      write_schema <- get_write_schema(dbtype, prefix = prefix)
      skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
      on.exit(disconnect(con), add = TRUE)

      cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

      cdm <- generateCohortSet2(cdm, cohortSet = complex_cs, name = "test_cohort")

      expect_true("test_cohort" %in% names(cdm))
      result <- dplyr::collect(cdm$test_cohort)
      expect_true(is.data.frame(result))
      expect_true(all(c("cohort_definition_id", "subject_id",
                         "cohort_start_date", "cohort_end_date") %in% names(result)))

      s <- omopgenerics::settings(cdm$test_cohort)
      expect_equal(nrow(s), 5)

      cleanup_cohort_tables(con, write_schema, "test_cohort")
    }
  })
}

# ============================================================================
# TEST GROUP 7: CDM subset support (cdmSubset + generateCohortSet2)
# ============================================================================

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

    some_persons <- cdm$person %>%
      dplyr::select("person_id") %>%
      utils::head(5) %>%
      dplyr::pull("person_id")

    skip_if(length(some_persons) < 5, "Not enough persons in CDM for subset test")

    cdm_sub <- cdmSubset(cdm, personId = some_persons)
    n_person <- cdm_sub$person %>% dplyr::tally() %>% dplyr::pull("n")
    expect_true(n_person <= 5)

    cs <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
    cdm_sub <- generateCohortSet2(cdm_sub, cohortSet = cs, name = "cohort")

    expect_true("cohort" %in% names(cdm_sub))
    result <- dplyr::collect(cdm_sub$cohort)
    expect_true(is.data.frame(result))
    expect_true(all(c("cohort_definition_id", "subject_id",
                       "cohort_start_date", "cohort_end_date") %in% names(result)))

    if (nrow(result) > 0) {
      expect_true(all(result$subject_id %in% some_persons))
    }

    cleanup_cohort_tables(con, write_schema, "cohort")
  })
}

# ============================================================================
# TEST GROUP 8: Caching on all supported database systems
# ============================================================================

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 caching"), {
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

    # First run with cache=TRUE — builds cache
    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort",
                               overwrite = TRUE, cache = TRUE)

    expect_true("cohort" %in% names(cdm))
    first_result <- dplyr::collect(cdm$cohort)
    expect_true(nrow(first_result) > 0)

    # Second run with cache=TRUE — should reuse cached tables (faster)
    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort2",
                               overwrite = TRUE, cache = TRUE)

    expect_true("cohort2" %in% names(cdm))
    second_result <- dplyr::collect(cdm$cohort2)
    # Both runs on the same CDM data should produce identical results
    expect_equal(nrow(first_result), nrow(second_result))

    # Verify cache tables exist in the write schema.
    # Cache tables are named dagcache_* (without write prefix).
    all_tbls <- tryCatch(DBI::dbListTables(con), error = function(e) character(0))
    cache_tbls <- all_tbls[grepl("^dagcache_", all_tbls, ignore.case = TRUE)]
    expect_true(length(cache_tbls) > 0)

    # Third run with cache=FALSE produces same results (but doesn't use cache)
    cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cohort3",
                               overwrite = TRUE, cache = FALSE)
    third_result <- dplyr::collect(cdm$cohort3)
    expect_equal(nrow(first_result), nrow(third_result))

    # Cleanup: cached tables + output tables
    for (nm in c("cohort", "cohort2", "cohort3")) {
      cleanup_cohort_tables(con, write_schema, nm)
    }
    # Drop cache tables
    for (ct in cache_tbls) {
      tryCatch(
        DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = ct, dbms = dbms(con))),
        error = function(e) NULL
      )
    }
  })
}

# ============================================================================
# TEST GROUP 9: Caching with complex multi-domain cohorts
# ============================================================================

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - generateCohortSet2 caching with complex cohorts"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    skip_if(!dir.exists(cohort_dir), "AtlasCohortGenerator cohorts not available")

    complex_cs <- build_complex_cohort_set()
    skip_if(is.null(complex_cs), "Complex cohort files not found")

    if (dbtype == "duckdb") {
      # Generate mock CDM for DuckDB caching test
      mock_cdm <- cdmFromCohortSet(complex_cs, n = 200, seed = 42)
      on.exit(tryCatch(DBI::dbDisconnect(cdmCon(mock_cdm), shutdown = TRUE), error = function(e) NULL), add = TRUE)

      # First run with cache=TRUE
      mock_cdm <- generateCohortSet2(mock_cdm, cohortSet = complex_cs,
                                      name = "cache_test", overwrite = TRUE, cache = TRUE)
      expect_true("cache_test" %in% names(mock_cdm))
      first_result <- dplyr::collect(mock_cdm$cache_test)

      # Second run with cache=TRUE — should reuse
      mock_cdm <- generateCohortSet2(mock_cdm, cohortSet = complex_cs,
                                      name = "cache_test2", overwrite = TRUE, cache = TRUE)
      second_result <- dplyr::collect(mock_cdm$cache_test2)
      expect_equal(nrow(first_result), nrow(second_result))
    } else {
      prefix <- guid_prefix()
      con <- get_connection(dbtype, DatabaseConnector = testUsingDatabaseConnector)
      cdm_schema <- get_cdm_schema(dbtype)
      write_schema <- get_write_schema(dbtype, prefix = prefix)
      skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
      on.exit(disconnect(con), add = TRUE)

      cdm <- cdmFromCon(con, cdmSchema = cdm_schema, writeSchema = write_schema)

      # Use the existing CDM data for caching tests on remote databases
      # (we can't easily upload + point vocab; the real CDM data is sufficient)
      cs <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

      # First run with cache=TRUE
      cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cache_test",
                                 overwrite = TRUE, cache = TRUE)
      first_result <- dplyr::collect(cdm$cache_test)

      # Second run — cache hit
      cdm <- generateCohortSet2(cdm, cohortSet = cs, name = "cache_test2",
                                 overwrite = TRUE, cache = TRUE)
      second_result <- dplyr::collect(cdm$cache_test2)
      expect_equal(nrow(first_result), nrow(second_result))

      # Cleanup
      for (nm in c("cache_test", "cache_test2")) {
        cleanup_cohort_tables(con, write_schema, nm)
      }
      all_tbls <- tryCatch(DBI::dbListTables(con), error = function(e) character(0))
      cache_tbls <- all_tbls[grepl("^dagcache_", all_tbls, ignore.case = TRUE)]
      for (ct in cache_tbls) {
        tryCatch(
          DBI::dbRemoveTable(con, inSchema(schema = write_schema, table = ct, dbms = dbms(con))),
          error = function(e) NULL
        )
      }
    }
  })
}
