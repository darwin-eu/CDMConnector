library(CDMConnector)

cohortSet <- readCohortSet("~/Desktop/AtlasCohortGenerator/inst/cohorts")


cdm <- cdmFromCohortSet(cohortSet, n = 100000L, duckdbPath = "~/Desktop/kitchen-sink.duckdb", verbose = T)

