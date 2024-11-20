# manual reverse dependency tests


# check that the eunomia data is the same

library(CDMConnector)
con1 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm1 <- cdmFromCon(con1, "main", "main")

con <- DBI::dbConnect(
  odbc::databricks(),
  httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
  useNativeQuery = FALSE,
  bigint = "numeric"
)

cdm_schema <- Sys.getenv("DATABRICKS_CDM_SCHEMA")
write_schema <- Sys.getenv("DATABRICKS_SCRATCH_SCHEMA")


library(CDMConnector)

cdm <- cdmFromCon(con,
                  cdmSchema = cdm_schema,
                  writeSchema = write_schema)

cdm

cdm1


library(dplyr)

for (n in names(cdm)) {

  cli::cat_line(paste("checking row counts of table", n))
  n0 <- tally(cdm[[n]]) %>% pull(n)
  n1 <- tally(cdm1[[n]]) %>% pull(n)

  if (n0 != n1) {
    cli::cli_warn(glue::glue("remote cdm table {n} should have {n0} rows but has {n1} instead!"))
  }
}


comparisions <- list()
for (n in names(cdm)) {
  cli::cat_line(paste("checking table", n))
  df0 <- collect(cdm[[n]])
  df1 <- collect(cdm1[[n]])

  comparisions[[n]] <- waldo::compare(df0, df1)
}


comparisions[[1]]

names(comparisions)

comparisions[["person"]]
comparisions[["observation_period"]]

p0 <- collect(cdm$person) %>% arrange(person_id)
p1 <- collect(cdm1$person) %>% arrange(person_id)

waldo::compare(p0, p1)


p0 <- collect(cdm$observation_period) %>% arrange(person_id, observation_period_start_date)
p1 <- collect(cdm1$observation_period) %>% arrange(person_id, observation_period_start_date)

waldo::compare(p0, p1)


# debugonce(IncidencePrevalence::generateDenominatorCohortSet)
cdm <- IncidencePrevalence::generateDenominatorCohortSet(
  cdm = cdm,
  name = "denom"
)


cdm1 <- IncidencePrevalence::generateDenominatorCohortSet(
  cdm = cdm1,
  name = "denom"
)

tally(cdm$denom)
tally(cdm1$denom)

cdm$condition_occurrence %>%
  PatientProfiles::addAge(indexDate = "condition_start_date",
                          useTempTables = FALSE)



select(condition_start_date) %>%
  mutate(next_day = clock::add_days(condition_start_date, 1L))



denom <- fetchSingleTargetDenominatorCohortSet(
  cdm = cdm, name = name,
  intermediateTable = paste0(intermediateTable, i),
  popSpecs = denominatorSet[[i]])


DBI::dbDisconnect(con)






testIncidencePrevalence <- function(con, cdm_schema, write_schema) {

  cdm <- cdmFromCon(
    con = con,
    cdmSchema = cdm_schema,
    writeSchema = write_schema
  )

  DBI::dbIsValid(con)

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
  exportFolder <- here::here("work", "export")

  prefix <- paste0("tmp", as.integer(Sys.time()) %% 1e5, "_")
  cohortTableName <- paste0(prefix, "cohort")
  cdm <- CDMConnector::generateCohortSet(
    cdm,
    cohortSet = cohortSet,
    name = cohortTableName
  )

  # debugonce(IncidencePrevalence::generateDenominatorCohortSet)

  cdm <- IncidencePrevalence::generateDenominatorCohortSet(
    cdm = cdm,
    name = "denominator",
    # ageGroup = list(c(0,17), c(18,64), c(65,199)),
    # sex = c("Male", "Female", "Both"),
    daysPriorObservation = 0
  )


  library(CDMConnector)
  con2 <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
  cdm2 <- cdmFromCon(con2, "main", "main")


    cdm2 <- IncidencePrevalence::generateDenominatorCohortSet(
      cdm = cdm2,
      name = "denominator",
      # ageGroup = list(c(0,17), c(18,64), c(65,199)),
      # sex = c("Male", "Female", "Both"),
      daysPriorObservation = 0
    )

  cdm2$denominator



  cdm$observation_period %>%
    PatientProfiles::addAge()

  DBI::dbWriteTable(con, "temptable11", cars, temporary = T)

}

dbtype = "spark"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - IncidencePrevalence"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    if (dbtype %in% c("spark", "sqlserver", "snowflake", "bigquery")) skip("failing test")
    skip_if(get_write_schema(dbtype) == "")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    testIncidencePrevalence(con, cdm_schema, write_schema)
    disconnect(con)
  })
}

