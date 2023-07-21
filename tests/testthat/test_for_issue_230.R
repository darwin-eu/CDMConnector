#
# library(CDMConnector)
# library(dplyr, warn.conflicts = F)
#
# con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir("synthea-hiv-10k"))
# cdm <- cdm_from_con(con, "main", "main")
#
# cdm <- generateConceptCohortSet(cdm, 40213227, name = "cohort_tbl", overwrite = T)
#
# attr(cdm$cohort_tbl, "tbl_name")
#
#
#
# x <- cdm$cohort_tbl %>%
#   filter(cohort_start_date >= as.Date("2020-01-01")) %>%
#   computeQuery(
#     name = "interest_cohort",
#     temporary = getOption("cohort_as_temp", FALSE),
#     schema = attr(cdm, "write_schema"),
#     overwrite = TRUE
#   )
#
# cohortSet(x)
# appendCohortAttributes(x, attritionReason = "After 2020-01-01")
#
# DBI::dbDisconnect(con, shutdown = T)
