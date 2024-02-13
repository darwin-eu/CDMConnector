# manual reverse dependency tests

# test_that("revdep-generateDenominatorCoÒhortSet", {
#   con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
#   cdm <- cdm_from_con(con, "main", "main")
#
#   cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
#   exportFolder <- here::here("work", "export")
#
#   prefix <-  paste0("tmp", stringr::str_remove_all(uuid::UUIDgenerate(), '-'))
#   cohortTableName <- paste0(prefix, "cohort")
#   cdm <- CDMConnector::generateCohortSet(
#     cdm,
#     cohortSet = cohortSet,
#     name = cohortTableName
#   )
#
#   expect_no_error({
#     cdm <- IncidencePrevalence::generateDenominatorCohortSet(
#       cdm = cdm,
#       name = "denominator",
#       ageGroup = list(c(0,17), c(18,64), c(65,199)),
#       # ageGroup = purrr::map(0:10, ~c(.*10, (.+1)*10-1)),
#       sex = c("Male", "Female", "Both"),
#       daysPriorObservation = 180
#     )
#   })
#
#   DBI::dbDisconnect(con)
# })

