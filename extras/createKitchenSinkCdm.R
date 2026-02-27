library(CDMConnector)

cohortSet <- readCohortSet("~/Desktop/AtlasCohortGenerator/inst/cohorts")

unlink("~/Desktop/kitchen-sink.duckdb")
cdm <- cdmFromCohortSet(cohortSet, n = 100000L, duckdbPath = "~/Desktop/kitchen-sink.duckdb", verbose = T)
