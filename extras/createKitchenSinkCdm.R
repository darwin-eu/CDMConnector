library(CDMConnector)

cohortSet <- readCohortSet("~/Desktop/AtlasCohortGenerator/inst/cohorts")

unlink("~/Desktop/kitchen-sink.duckdb")
cdm <- cdmFromCohortSet(cohortSet, n = 100000L, duckdbPath = "~/Desktop/kitchen-sink.duckdb", verbose = T)


con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/kitchen-sink.duckdb")
cdm <- cdmFromCon(con, "main", "main", .softValidation = T, cohortTables = "cohort")
library(dplyr)
cdm$person %>%
  tally()

settings(cdm$cohort)




a = cohortCount(cdm$cohort)


cdm <- generateCohortSet2(cdm, cohortSet, name = "cohort2")


DBI::dbDisconnect(con)

