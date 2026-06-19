# Benchmark + correctness: insertTable vs insertTableSpark on Databricks/Spark
#
# Compares the default insertTable (INSERT INTO ... SELECT ... UNION ALL,
# batch size 1000) against insertTableSpark (INSERT INTO ... VALUES (...),
# batch size 5000) on a Spark connection.
#
# Two parts:
#   1. Correctness: confirms both paths produce tables with the right
#      *column types* (DATE stays DATE, TIMESTAMP stays TIMESTAMP — see #618),
#      the right row counts, and round-trip the same values. Also covers the
#      empty (0-row) cohort-table case from #618.
#   2. Benchmark: inserts synthetic tables of a few sizes and reports
#      wall-clock seconds and rows/sec.
#
# Requirements: env vars DATABRICKS_HTTPPATH and DATABRICKS_CATALOG, plus
# a configured odbc::databricks() profile. A scratch schema is created
# and dropped at the end.
#
# Run with: Rscript extras/benchmark_insertTableSpark.R

devtools::load_all()
library(DBI)
library(dplyr)

# ---- connection ---------------------------------------------------------

con <- DBI::dbConnect(
  odbc::databricks(),
  httpPath       = Sys.getenv("DATABRICKS_HTTPPATH"),
  useNativeQuery = FALSE,
  bigint         = "numeric"
)
stopifnot(CDMConnector::dbms(con) == "spark")

catalog <- Sys.getenv("DATABRICKS_CATALOG", "hive_metastore")
bench_schema <- c(catalog = catalog, schema = "cdmconnector_bench")

DBI::dbExecute(con, paste0(
  "CREATE SCHEMA IF NOT EXISTS ", paste(bench_schema, collapse = ".")
))

src <- CDMConnector::dbSource(con, writeSchema = bench_schema)

# ---- synthetic data -----------------------------------------------------

make_table <- function(nrows, seed = 1) {
  set.seed(seed)
  tibble::tibble(
    person_id        = seq_len(nrows),
    gender_concept_id = sample(c(8507L, 8532L), nrows, replace = TRUE),
    year_of_birth    = sample(1940:2010, nrows, replace = TRUE),
    birth_date       = as.Date("1970-01-01") + sample(0:20000, nrows, replace = TRUE),
    observation_ts   = as.POSIXct("2020-01-01") + sample(0:1e7, nrows, replace = TRUE),
    note             = stringi::stri_rand_strings(nrows, length = 12),
    measurement      = round(rnorm(nrows, mean = 100, sd = 15), 2)
  )
}

# ---- correctness checks -------------------------------------------------
#
# Insert a table with each column type via both paths, read it back, and
# confirm the column types and values survive the round trip. The key
# regression this guards against is #618: dates being stored as character
# columns because they were formatted to strings before the table was
# created.

read_back <- function(name) {
  dplyr::tbl(src = con, CDMConnector::inSchema(bench_schema, name, dbms = "spark")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::collect()
}

check_types_and_values <- function(name, expected) {
  got <- read_back(name)
  got <- got[order(got$person_id), names(expected), drop = FALSE]
  expected <- expected[order(expected$person_id), , drop = FALSE]

  type_ok <- vapply(names(expected), function(col) {
    identical(class(got[[col]])[1], class(expected[[col]])[1])
  }, logical(1))

  if (!all(type_ok)) {
    bad <- names(expected)[!type_ok]
    for (col in bad) {
      message(sprintf("    BAD TYPE  %-18s expected %s, got %s",
                      col, class(expected[[col]])[1], class(got[[col]])[1]))
    }
    stop("column type mismatch in ", name)
  }

  if (nrow(expected) > 0) {
    # dates compared as character to avoid tz/precision noise
    norm <- function(df) {
      df$birth_date     <- format(df$birth_date, "%Y-%m-%d")
      df$observation_ts <- format(df$observation_ts, "%Y-%m-%d %H:%M:%S")
      df
    }
    stopifnot(isTRUE(all.equal(norm(expected), norm(got), check.attributes = FALSE)))
  }
  message(sprintf("    OK  %-14s  %d rows, %d cols, types preserved",
                  name, nrow(got), ncol(got)))
}

message("\n=== correctness ===")
for (n in c(0L, 250L)) {
  message(sprintf("  --- %d-row table ---", n))
  expected <- make_table(n)

  try(DBI::dbRemoveTable(con, CDMConnector::inSchema(bench_schema, "chk_default", dbms = "spark")), silent = TRUE)
  try(DBI::dbRemoveTable(con, CDMConnector::inSchema(bench_schema, "chk_bulk",    dbms = "spark")), silent = TRUE)

  CDMConnector::insertTable(cdm = src, name = "chk_default", table = expected, overwrite = TRUE)
  CDMConnector::insertTableSpark(cdm = src, name = "chk_bulk", table = expected, overwrite = TRUE)

  check_types_and_values("chk_default", expected)
  check_types_and_values("chk_bulk", expected)
}
message("  correctness checks passed")

# ---- newCohortTable column types ---------------------------------------
#
# The original #618 report came through omopgenerics::emptyCohortTable /
# newCohortTable: on Spark the cohort_start_date / cohort_end_date columns
# came back as character. This insert -> newCohortTable round trip is the
# exact path that failed, so assert the cohort columns keep their types
# (integer ids, Date start/end) for both an empty and a populated cohort.

make_cohort <- function(nrows, seed = 1) {
  set.seed(seed)
  tibble::tibble(
    cohort_definition_id = rep(1L, nrows),
    subject_id           = seq_len(nrows),
    cohort_start_date    = as.Date("2010-01-01") + sample(0:3000, nrows, replace = TRUE),
    cohort_end_date      = as.Date("2015-01-01") + sample(0:3000, nrows, replace = TRUE)
  )
}

# newCohortTable validates against a cdm_reference, so build a minimal one
# (empty person + observation_period) on the bench connection and attach the
# cohort to it. .softValidation skips the observation-period content check
# (our synthetic subjects have no observation periods) while still enforcing
# the column types we care about.
build_min_cdm <- function() {
  person <- tibble::tibble(
    person_id = integer(), gender_concept_id = integer(),
    year_of_birth = integer(), race_concept_id = integer(),
    ethnicity_concept_id = integer()
  )
  observation_period <- tibble::tibble(
    observation_period_id = integer(), person_id = integer(),
    observation_period_start_date = as.Date(character()),
    observation_period_end_date = as.Date(character()),
    period_type_concept_id = integer()
  )
  p <- CDMConnector::insertTable(src, "person", person, overwrite = TRUE)
  o <- CDMConnector::insertTable(src, "observation_period", observation_period, overwrite = TRUE)
  omopgenerics::newCdmReference(
    tables = list(person = p, observation_period = o),
    cdmName = "bench", .softValidation = TRUE
  )
}

check_cohort_table_types <- function(name, nrows) {
  cohort <- make_cohort(nrows)
  try(DBI::dbRemoveTable(con, CDMConnector::inSchema(bench_schema, name, dbms = "spark")), silent = TRUE)

  cdm <- build_min_cdm()
  cdm[[name]] <- CDMConnector::insertTable(cdm = src, name = name, table = cohort, overwrite = TRUE)
  cdm[[name]] <- omopgenerics::newCohortTable(cdm[[name]], .softValidation = TRUE)
  got <- dplyr::collect(cdm[[name]])

  expected_types <- c(
    cohort_definition_id = "integer",
    subject_id           = "integer",
    cohort_start_date    = "Date",
    cohort_end_date      = "Date"
  )
  for (col in names(expected_types)) {
    actual <- class(got[[col]])[1]
    # integer ids may come back as numeric from Spark; only dates are strict
    ok <- if (expected_types[[col]] == "Date") {
      inherits(got[[col]], "Date")
    } else {
      is.numeric(got[[col]])
    }
    if (!ok) {
      stop(sprintf("newCohortTable %s: column %s is %s, expected %s",
                   name, col, actual, expected_types[[col]]))
    }
  }
  message(sprintf("    OK  %-18s  %d rows, cohort dates kept as Date", name, nrow(got)))
}

message("\n=== newCohortTable column types ===")
check_cohort_table_types("chk_cohort_empty", 0L)
check_cohort_table_types("chk_cohort_full", 100L)
message("  newCohortTable type checks passed")

# Sizes to benchmark. Add/remove as desired.
sizes <- c(100L, 1000L, 5000L, 20000L)

timeit <- function(expr) {
  t0 <- Sys.time()
  force(expr)
  as.numeric(difftime(Sys.time(), t0, units = "secs"))
}

results <- list()

for (n in sizes) {
  message("\n=== ", format(n, big.mark = ","), " rows ===")
  tbl <- make_table(n)

  # warm-up: ensure both target tables don't exist
  try(DBI::dbRemoveTable(con, CDMConnector::inSchema(bench_schema, "bench_default", dbms = "spark")), silent = TRUE)
  try(DBI::dbRemoveTable(con, CDMConnector::inSchema(bench_schema, "bench_bulk",    dbms = "spark")), silent = TRUE)

  # default path (UNION ALL, batch 1000, via .dbWriteTableSafe)
  t_default <- timeit({
    CDMConnector::insertTable(
      cdm       = src,
      name      = "bench_default",
      table     = tbl,
      overwrite = TRUE
    )
  })
  message(sprintf("  insertTable        : %7.2fs  (%6.0f rows/s)",
                  t_default, n / t_default))

  # bulk VALUES path (batch 5000)
  t_bulk <- timeit({
    CDMConnector::insertTableSpark(
      cdm       = src,
      name      = "bench_bulk",
      table     = tbl,
      overwrite = TRUE,
      batchSize = 5000L
    )
  })
  message(sprintf("  insertTableSpark   : %7.2fs  (%6.0f rows/s)",
                  t_bulk, n / t_bulk))

  # sanity: row counts match
  n_default <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM %s.bench_default",
    paste(bench_schema, collapse = ".")
  ))$n
  n_bulk <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM %s.bench_bulk",
    paste(bench_schema, collapse = ".")
  ))$n
  stopifnot(n_default == n, n_bulk == n)

  results[[length(results) + 1L]] <- tibble::tibble(
    n_rows       = n,
    insertTable_s     = t_default,
    insertTableSpark_s = t_bulk,
    speedup           = t_default / t_bulk
  )
}

summary_df <- dplyr::bind_rows(results)
message("\n=== summary ===")
print(summary_df, n = Inf)

# ---- cleanup ------------------------------------------------------------

DBI::dbExecute(con, paste0(
  "DROP SCHEMA IF EXISTS ", paste(bench_schema, collapse = "."), " CASCADE"
))
DBI::dbDisconnect(con)
