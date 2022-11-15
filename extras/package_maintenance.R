# Create internal package data

library(dplyr)
cdm_fields_src_54 <- readr::read_csv(system.file(file.path("csv", "OMOP_CDMv5.4_Field_Level.csv"), package = "CommonDataModel"))
cdm_tables_src_54 <- readr::read_csv(system.file(file.path("csv", "OMOP_CDMv5.4_Table_Level.csv"), package = "CommonDataModel"))
cdm_fields_src_53 <- readr::read_csv(system.file(file.path("csv", "OMOP_CDMv5.3_Field_Level.csv"), package = "CommonDataModel"))
cdm_tables_src_53 <- readr::read_csv(system.file(file.path("csv", "OMOP_CDMv5.3_Table_Level.csv"), package = "CommonDataModel"))


# define table groupings
vocab_tables <- c("concept", "vocabulary", "domain", "concept_class", "concept_relationship",
                  "relationship", "concept_synonym", "concept_ancestor", "source_to_concept_map",
                  "drug_strength")

# vocab_tables <- c("concept", "concept_relationship", "concept_synonym", "concept_ancestor")

default_tables <- c("person", "observation_period", "visit_occurrence", "visit_detail",
  "condition_occurrence", "drug_exposure", "procedure_occurrence",
  "measurement", "observation", "death",
  "location", "care_site", "provider", "drug_era", "dose_era",
  "condition_era", "concept", "vocabulary",  "concept_relationship", "concept_ancestor",
  "concept_synonym", "drug_strength")


clinical_tables <- c("person", "observation_period", "visit_occurrence", "visit_detail",
  "condition_occurrence", "drug_exposure", "procedure_occurrence",
  "device_exposure", "measurement", "observation", "death", "note",
  "note_nlp", "specimen", "fact_relationship", "episode", "episode_event")

derived_tables <- c("condition_era", "drug_era", "dose_era")


# cdm_fields$cdmTableName %>% unique() %>% tolower() %>% dput
# all_tables <- c("person", "observation_period", "visit_occurrence", "visit_detail",
#   "condition_occurrence", "drug_exposure", "procedure_occurrence",
#   "device_exposure", "measurement", "observation", "death", "note",
#   "note_nlp", "specimen", "fact_relationship", "location", "care_site",
#   "provider", "payer_plan_period", "cost", "drug_era", "dose_era",
#   "condition_era", "episode", "episode_event", "metadata", "cdm_source",
#   "concept", "vocabulary", "domain", "concept_class", "concept_relationship",
#   "relationship", "concept_synonym", "concept_ancestor", "source_to_concept_map",
#   "drug_strength", "cohort", "cohort_definition")

# CDM 5.3 and 5.4 specs

spec_cdm_table_54 <-  cdm_tables_src_54 %>%
  select(1:4) %>%
  mutate(across(everything(), tolower)) %>%
  mutate(isRequired = case_when(isRequired == "yes" ~ TRUE, isRequired == "no" ~ FALSE)) %>%
  mutate(group_vocab = cdmTableName %in% vocab_tables,
         group_all = TRUE,
         group_clinical = cdmTableName %in% clinical_tables,
         group_derived = cdmTableName %in% derived_tables,
         group_default = cdmTableName %in% default_tables)



spec_cdm_field_54 <-  cdm_fields_src_54 %>%
  select(1:4) %>%
  mutate(across(everything(), tolower)) %>%
  mutate(isRequired = case_when(isRequired == "yes" ~ TRUE, isRequired == "no" ~ FALSE))


spec_cdm_table_53 <-  cdm_tables_src_53 %>%
  select(1:4) %>%
  mutate(across(everything(), tolower)) %>%
  mutate(isRequired = case_when(isRequired == "yes" ~ TRUE, isRequired == "no" ~ FALSE)) %>%
  mutate(group_vocab = cdmTableName %in% vocab_tables,
         group_all = TRUE,
         group_clinical = cdmTableName %in% clinical_tables,
         group_derived = cdmTableName %in% derived_tables,
         group_default = cdmTableName %in% default_tables)

spec_cdm_field_53 <-  cdm_fields_src_53 %>%
  select(1:4) %>%
  mutate(across(everything(), tolower)) %>%
  mutate(isRequired = case_when(isRequired == "yes" ~ TRUE, isRequired == "no" ~ FALSE))

spec_cdm_table <- list("5.4" = spec_cdm_table_54, "5.3" = spec_cdm_table_53)
spec_cdm_field <- list("5.4" = spec_cdm_field_54, "5.3" = spec_cdm_field_53)


readr::write_csv(spec_cdm_field_54, file.path("inst", "csv", "OMOP_CDMv5.4_Field_Level.csv"))
readr::write_csv(spec_cdm_table_54, file.path("inst", "csv", "OMOP_CDMv5.4_Table_Level.csv"))

readr::write_csv(spec_cdm_field_53, file.path("inst", "csv", "OMOP_CDMv5.3_Field_Level.csv"))
readr::write_csv(spec_cdm_table_53, file.path("inst", "csv", "OMOP_CDMv5.3_Table_Level.csv"))

usethis::use_data(spec_cdm_field, spec_cdm_table, internal = TRUE, overwrite = TRUE)


