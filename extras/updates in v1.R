
# CDMConnector updates --- v1.0

library(CDMConnector)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())

cdm <- cdmFromCon(con, cdmSchema = "main")

############## selection of cdm tables #######################
# The cdm_tables argument has been removed from cdmFromCon
# Now you will get all existing cdm tables. If a table is missing no error is given.

# To select specific cdm tables
cdm %>%
  cdmSelectTbl(person, observation_period)

cdm %>%
  cdmSelectTbl("person", "observation_period")

cdm %>%
  cdmSelectTbl(tblGroup("vocab"))

cdm %>%
  cdmSelectTbl(starts_with("concept"))

# ... etc. Similar to dplyr::select but for the cdm

############## getting the cdm from a cdm table #######################

# Suppose you have a function that takes in a query but you also need access to the whole cdm
# We have seen some people write functions that accept both the table and the cdm

add_birthyear <- function(tbl, cdm) {
  tbl %>%
    left_join(select(cdm$person, person_id, year_of_birth), by = "person_id")
}

cdm$visit_occurrence %>%
  add_birthyear(cdm) %>%
  count(visit_concept_id, year_of_birth)


# Instead of doing this we can now get the cdm from the table using the "cdm_reference" attribute

add_birthyear <- function(tbl) {
  cdm <- attr(tbl, "cdm_reference")
  if (is.null(cdm)) rlang::abort("cdm_reference attribute not found!")

  tbl %>%
    left_join(select(cdm$person, person_id, year_of_birth), by = "person_id")
}


cdm$visit_occurrence %>%
  select(person_id, visit_concept_id) %>%
  add_birthyear() %>%
  count(visit_concept_id, year_of_birth)

# So in summary cdm tables know about the cdm object that they came from

# The cdm_reference attribute is preserved by dplyr verbs and computeQuery

cdm$visit_occurrence %>%
  select(person_id, visit_concept_id) %>%
  computeQuery() %>%
  add_birthyear() %>%
  count(visit_concept_id, year_of_birth)

# One caveat.... If you collect then the attribute is lost

cdm$visit_occurrence %>%
  select(person_id, visit_concept_id) %>%
  collect() %>%
  add_birthyear() %>%
  count(visit_concept_id, year_of_birth)


############## Intersect and Union operations for cohort tables #######################

# Suppose you have a cohort table with multiple cohorts and would like to perform an intersect or union operation

cohort <- tibble::tribble(
  ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
  1,                     1,           "2022-01-01",       "2023-01-01",
  2,                     1,           "2022-05-01",       "2023-06-01",
  1,                     2,           "2020-01-01",       "2022-01-01",
  2,                     2,           "2023-01-01",       "2023-02-01",
  2,                     3,           "2023-01-01",       "2023-02-01") %>%
  dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))

DBI::dbWriteTable(con, "cohort", cohort, overwrite = TRUE)


cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cohortTables = "cohort")

cdm$cohort %>%
  intersectCohorts(cohortDefinitionId = 99)

cdm$cohort %>%
  unionCohorts(cohortDefinitionId = 99)

# Please test this and let me know if you find edge cases that give incorrect results :)

# Also you can now rely on two cohort attributes for a cohort table.
cohortCount(cdm$cohort)
omopgenerics::settings(cdm$cohort)

# However cohort_attrition might be null
omopgenerics::attrition(cdm$cohort)

# Both camel case and snake case styles are supported for the new functions.
cdm$cohort %>%
  intersectCohorts(cohort_definition_id = 99) %>%
  dplyr::show_query()

cdm$cohort %>%
  unionCohorts(cohort_definition_id = 99)

# Next up
# - Snowflake support
# - New Databricks test server
# Should we require cdm_name?





