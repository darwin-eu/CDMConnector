# Ad-hoc test: build a CDM with the omock R package and run generateCohortSet.
#
# Install omock from OHDSI universe if needed:
#   install.packages("omock", repos = c("https://ohdsi.r-universe.dev", "https://cloud.r-project.org"))
#
# Run from package root or after loading CDMConnector:
#   source("extras/omock_generateCohortSet.R")

library(omock)
library(dplyr)
library(CDMConnector)

cdm <- mockCdmReference(cdmName = "omock test") %>%
  mockVocabularyTables(vocabularySet = "mock") |>
  mockPerson(nPerson = 100) |>
  mockObservationPeriod() |>
  mockConditionOccurrence(recordPerson = 2, seed = 42) |>
  mockConcepts(conceptSet = 192671L, domain = "Condition", seed = 1)

# Add concept 192671 to concept table if not present, and add condition rows
concept_tbl <- cdm$concept |> dplyr::collect()
if (!192671L %in% concept_tbl$concept_id) {
  new_concept <- dplyr::tibble(
    concept_id = 192671L,
    concept_name = "Gastrointestinal hemorrhage",
    domain_id = "Condition",
    vocabulary_id = "SNOMED",
    concept_class_id = "Clinical Finding",
    standard_concept = "S",
    concept_code = "74474003",
    valid_start_date = as.Date("1970-01-01"),
    valid_end_date = as.Date("2099-12-31"),
    invalid_reason = NA_character_
  )
  cdm$concept <- dplyr::bind_rows(concept_tbl, new_concept)
}
# Add condition_occurrence rows with concept 192671
obs <- cdm$observation_period
persons_use <- head(unique(obs$person_id), 10)
obs_use <- obs |> dplyr::filter(.data$person_id %in% .env$persons_use)
co_tbl <- cdm$condition_occurrence
max_co_id <- if (nrow(co_tbl) > 0) max(co_tbl$condition_occurrence_id, na.rm = TRUE) else 0
extra_co <- obs_use |>
  dplyr::mutate(
    condition_occurrence_id = .env$max_co_id + dplyr::row_number(),
    condition_concept_id = 192671L,
    condition_start_date = .data$observation_period_start_date + 1L,
    condition_end_date = .data$observation_period_start_date + 7L,
    condition_type_concept_id = 32020L,
    visit_occurrence_id = NA_integer_
  ) |>
  dplyr::select(
    "condition_occurrence_id", "person_id", "condition_concept_id",
    "condition_start_date", "condition_end_date", "condition_type_concept_id",
    "visit_occurrence_id"
  )
co_tbl <- dplyr::bind_rows(co_tbl, extra_co)
cdm$condition_occurrence <- co_tbl

message("Mock CDM tables: ", paste(names(cdm), collapse = ", "))
message("Person nrow: ", nrow(dplyr::collect(cdm$person)))
message("Condition_occurrence nrow: ", nrow(cdm$condition_occurrence))

# ------------------------------------------------------------------------------
# 2. Load example cohort set and run generateCohortSet (local CDM path)
# ------------------------------------------------------------------------------
cohort_set <- readCohortSet(
  system.file("example_cohorts", package = "CDMConnector", mustWork = TRUE)
)
message("Cohort set: ", nrow(cohort_set), " cohort(s)")

debugonce(generateCohortSet)
debugonce(generateCohortSetLocal)
cdm <- generateCohortSet(
  cdm = cdm,
  cohortSet = cohort_set,
  name = "gibleed",
  computeAttrition = TRUE,
  overwrite = TRUE
)

# ------------------------------------------------------------------------------
# 3. Inspect result
# ------------------------------------------------------------------------------
stopifnot("gibleed" %in% names(cdm))
stopifnot(inherits(cdm$gibleed, "cohort_table"))

cohort_count <- cohortCount(cdm$gibleed)
message("Cohort counts:")
print(cohort_count)

message("Cohort table (first rows):")
print(head(cdm$gibleed, 5))

message("Done: omock CDM + generateCohortSet ran successfully.")
