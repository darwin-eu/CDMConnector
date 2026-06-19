library(CDMConnector)
library(dplyr, warn.conflicts = FALSE)
library(Capr)

# downloadEunomiaData(
#   pathToData = "/Users/ginberg/Data/eunomia", # change to the location you want to save the data
#   overwrite = TRUE
# )

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
cdm <- CDMConnector::cdm_from_con(
  con = con,
  cdm_schema = "main",
  write_schema = "main"
)

gibleed_cohort_definition <- cohort(
  entry = condition(cs(descendants(192671))),
  attrition = attrition(
    "no RA" = withAll(
      exactly(0,
              condition(cs(descendants(80809))),
              duringInterval(eventStarts(-Inf, Inf))))
  )
)

cdm <- generateCohortSet(
  cdm,
  list(gibleed = gibleed_cohort_definition),
  name = "gibleed",
  computeAttrition = TRUE,
  overwrite = TRUE
)
cdm$gibleed

DBI::dbDisconnect(con, shutdown = TRUE)

sessionInfo()
