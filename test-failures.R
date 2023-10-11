#
# ── Failed tests ───────────────────────────────────────────────────────────────
# Error (test-db-generateConceptCohortSet.R:222:5): duckdb - generateConceptCohortSet
# Error in `generateConceptCohortSet(cdm, cohortSet = cohort, name = "gibleed5",
#                                    requiredObservation = c(2, 2), overwrite = TRUE)`: unused argument (cohortSet = cohort)
# Backtrace:
#   ▆
# 1. └─CDMConnector (local) test_generate_concept_cohort_set(con, cdm_schema, write_schema) at test-db-generateConceptCohortSet.R:222:4
#
# Error (test-db-generateConceptCohortSet.R:222:5): postgres - generateConceptCohortSet
# Error in `generateConceptCohortSet(cdm, cohortSet = cohort, name = "gibleed5",
#                                    requiredObservation = c(2, 2), overwrite = TRUE)`: unused argument (cohortSet = cohort)
# Backtrace:
#   ▆
# 1. └─CDMConnector (local) test_generate_concept_cohort_set(con, cdm_schema, write_schema) at test-db-generateConceptCohortSet.R:222:4
#
# Error (test-db-generateConceptCohortSet.R:222:5): redshift - generateConceptCohortSet
# Error in `generateConceptCohortSet(cdm, cohortSet = cohort, name = "gibleed5",
#                                    requiredObservation = c(2, 2), overwrite = TRUE)`: unused argument (cohortSet = cohort)
# Backtrace:
#   ▆
# 1. └─CDMConnector (local) test_generate_concept_cohort_set(con, cdm_schema, write_schema) at test-db-generateConceptCohortSet.R:222:4
#
# Error (test-db-generateConceptCohortSet.R:222:5): snowflake - generateConceptCohortSet
# Error in `generateConceptCohortSet(cdm, cohortSet = cohort, name = "gibleed5",
#                                    requiredObservation = c(2, 2), overwrite = TRUE)`: unused argument (cohortSet = cohort)
# Backtrace:
#   ▆
# 1. └─CDMConnector (local) test_generate_concept_cohort_set(con, cdm_schema, write_schema) at test-db-generateConceptCohortSet.R:222:4
#
# Error (test-db-generateConceptCohortSet.R:233:3): missing domains produce warning
# Error in `UseMethod("inner_join")`: no applicable method for 'inner_join' applied to an object of class "NULL"
# Backtrace:
#   ▆
# 1. ├─testthat::expect_warning(...) at test-db-generateConceptCohortSet.R:233:2
# 2. │ └─testthat:::expect_condition_matching(...)
# 3. │   └─testthat:::quasi_capture(...)
# 4. │     ├─testthat (local) .capture(...)
# 5. │     │ └─base::withCallingHandlers(...)
# 6. │     └─rlang::eval_bare(quo_get_expr(.quo), quo_get_env(.quo))
# 7. ├─CDMConnector::generateConceptCohortSet(cdm, conceptSet = list(celecoxib = 1118084)) at test-db-generateConceptCohortSet.R:234:4
# 8. │ └─... %>% ... at CDMConnector/R/generateConceptCohortSet.R:243:2
# 9. ├─CDMConnector::computeQuery(...)
# 10. │ └─base::is.data.frame(x) at CDMConnector/R/compute.R:186:2
# 11. ├─dplyr::mutate(...) at CDMConnector/R/compute.R:186:2
# 12. ├─CDMConnector:::cohort_collapse(.)
# 13. │ ├─checkmate::assert_true(methods::is(x, "tbl_dbi")) at CDMConnector/R/cohortTransformations.R:6:2
# 14. │ │ └─checkmate::checkTRUE(x, na.ok)
# 15. │ │   └─base::isTRUE(x)
# 16. │ └─methods::is(x, "tbl_dbi") at CDMConnector/R/cohortTransformations.R:6:2
# 17. ├─dplyr::select(...)
# 18. ├─dplyr::mutate(...)
# 19. ├─dplyr::filter(...)
# 20. └─dplyr::inner_join(., obs_period, by = "subject_id")
#
# [ FAIL 5 | WARN 0 | SKIP 7 | PASS 771 ]
# # fixed in v1.1.3
#
#
