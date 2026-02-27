# Tests for R/circe.R SQL builder functions - more domain builders

# --- build_death_sql ---

test_that("build_death_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("DEATH", result))
  expect_true(grepl("person_id", result))
  expect_true(grepl("death_date", result))
})

test_that("build_death_sql handles Age filter", {
  criteria <- list(CodesetId = 0, Age = list(Op = "gte", Value = 65))
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("build_death_sql handles DeathType", {
  criteria <- list(CodesetId = 0, DeathType = list(list(CONCEPT_ID = 38003569)))
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("death_type_concept_id", result))
  expect_true(grepl("38003569", result))
})

test_that("build_death_sql handles OccurrenceStartDate", {
  criteria <- list(CodesetId = 0, OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

# --- build_observation_period_sql ---

test_that("build_observation_period_sql generates valid SQL", {
  criteria <- list()
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("OBSERVATION_PERIOD", result))
  expect_true(grepl("person_id", result))
})

test_that("build_observation_period_sql handles First flag", {
  criteria <- list(First = TRUE)
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

test_that("build_observation_period_sql handles UserDefinedPeriod", {
  criteria <- list(UserDefinedPeriod = list(startDate = "2020-01-01", endDate = "2020-12-31"))
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("DATEFROMPARTS", result))
})

# --- build_location_region_sql ---

test_that("build_location_region_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_location_region_sql(criteria)
  expect_true(grepl("LOCATION", result))
  expect_true(grepl("region_concept_id", result))
})

test_that("build_location_region_sql handles NULL CodesetId", {
  criteria <- list()
  result <- CDMConnector:::build_location_region_sql(criteria)
  expect_true(grepl("LOCATION", result))
})

# --- build_drug_exposure_sql ---

test_that("build_drug_exposure_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("DRUG_EXPOSURE", result))
  expect_true(grepl("person_id", result))
  expect_true(grepl("start_date", result))
})

test_that("build_drug_exposure_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("ordinal", result))
  expect_true(grepl("row_number", result))
})

test_that("build_drug_exposure_sql handles Refills filter", {
  criteria <- list(CodesetId = 0, Refills = list(Op = "gte", Value = 2))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("refills", result))
})

test_that("build_drug_exposure_sql handles Quantity filter", {
  criteria <- list(CodesetId = 0, Quantity = list(Op = "eq", Value = 30))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("quantity", result))
})

test_that("build_drug_exposure_sql handles DaysSupply filter", {
  criteria <- list(CodesetId = 0, DaysSupply = list(Op = "gt", Value = 30))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("days_supply", result))
})

test_that("build_drug_exposure_sql handles DrugType filter", {
  criteria <- list(CodesetId = 0, DrugType = list(list(CONCEPT_ID = 38000175)))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("drug_type_concept_id", result))
})

test_that("build_drug_exposure_sql handles RouteConcept", {
  criteria <- list(CodesetId = 0, RouteConcept = list(list(CONCEPT_ID = 4132161)))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("route_concept_id", result))
})

# --- build_visit_occurrence_sql ---

test_that("build_visit_occurrence_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
  expect_true(grepl("person_id", result))
})

test_that("build_visit_occurrence_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("ordinal", result))
})

test_that("build_visit_occurrence_sql handles VisitLength", {
  criteria <- list(CodesetId = 0, VisitLength = list(Op = "gte", Value = 3))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("DATEDIFF", result))
})

test_that("build_visit_occurrence_sql handles VisitType filter", {
  criteria <- list(CodesetId = 0, VisitType = list(list(CONCEPT_ID = 44818518)))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("visit_type_concept_id", result))
})

# --- build_procedure_occurrence_sql ---

test_that("build_procedure_occurrence_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("PROCEDURE_OCCURRENCE", result))
  expect_true(grepl("person_id", result))
})

test_that("build_procedure_occurrence_sql handles ProcedureType", {
  criteria <- list(CodesetId = 0, ProcedureType = list(list(CONCEPT_ID = 38000275)))
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("procedure_type_concept_id", result))
})

test_that("build_procedure_occurrence_sql handles Quantity", {
  criteria <- list(CodesetId = 0, Quantity = list(Op = "eq", Value = 1))
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("quantity", result))
})

test_that("build_procedure_occurrence_sql handles Modifier", {
  criteria <- list(CodesetId = 0, Modifier = list(list(CONCEPT_ID = 44818668)))
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("modifier_concept_id", result))
})

# --- build_measurement_sql ---

test_that("build_measurement_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("MEASUREMENT", result))
  expect_true(grepl("person_id", result))
})

test_that("build_measurement_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("ordinal", result))
})

test_that("build_measurement_sql handles ValueAsNumber", {
  criteria <- list(CodesetId = 0, ValueAsNumber = list(Op = "gt", Value = 100))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("value_as_number", result))
})

test_that("build_measurement_sql handles MeasurementType", {
  criteria <- list(CodesetId = 0, MeasurementType = list(list(CONCEPT_ID = 44818702)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("measurement_type_concept_id", result))
})

test_that("build_measurement_sql handles Unit filter", {
  criteria <- list(CodesetId = 0, Unit = list(list(CONCEPT_ID = 8876)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("unit_concept_id", result))
})

test_that("build_measurement_sql handles ValueAsConcept", {
  criteria <- list(CodesetId = 0, ValueAsConcept = list(list(CONCEPT_ID = 45877985)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("value_as_concept_id", result))
})

test_that("build_measurement_sql handles Operator filter", {
  criteria <- list(CodesetId = 0, Operator = list(list(CONCEPT_ID = 4172703)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("operator_concept_id", result))
})

test_that("build_measurement_sql handles RangeLow/RangeHigh", {
  criteria <- list(CodesetId = 0, RangeLow = list(Op = "gt", Value = 0), RangeHigh = list(Op = "lt", Value = 200))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("range_low", result))
  expect_true(grepl("range_high", result))
})

test_that("build_measurement_sql handles RangeLowRatio", {
  criteria <- list(CodesetId = 0, RangeLowRatio = list(Op = "gt", Value = 0.5))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("NULLIF.*range_low", result))
})

test_that("build_measurement_sql handles RangeHighRatio", {
  criteria <- list(CodesetId = 0, RangeHighRatio = list(Op = "lt", Value = 1.5))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("NULLIF.*range_high", result))
})

test_that("build_measurement_sql handles Abnormal", {
  criteria <- list(CodesetId = 0, Abnormal = TRUE)
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("range_low", result))
  expect_true(grepl("range_high", result))
  expect_true(grepl("4155142", result))
})

test_that("build_measurement_sql handles DateAdjustment", {
  criteria <- list(CodesetId = 0, DateAdjustment = list(startOffset = 1, endOffset = -1, StartWith = "start_date", EndWith = "start_date"))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("DATEADD", result))
})

# --- build_observation_sql ---

test_that("build_observation_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("OBSERVATION", result))
  expect_true(grepl("person_id", result))
})

test_that("build_observation_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("ordinal", result))
})

test_that("build_observation_sql handles ValueAsString", {
  criteria <- list(CodesetId = 0, ValueAsString = "positive")
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("value_as_string", result))
  expect_true(grepl("LIKE", result))
})

test_that("build_observation_sql handles OccurrenceStartDate", {
  criteria <- list(CodesetId = 0, OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("DATEFROMPARTS", result))
})

# --- build_device_exposure_sql ---

test_that("build_device_exposure_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("DEVICE_EXPOSURE", result))
  expect_true(grepl("person_id", result))
})

test_that("build_device_exposure_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("ordinal", result))
})

test_that("build_device_exposure_sql handles Quantity", {
  criteria <- list(CodesetId = 0, Quantity = list(Op = "eq", Value = 1))
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("quantity", result))
})

test_that("build_device_exposure_sql handles DeviceType", {
  criteria <- list(CodesetId = 0, DeviceType = list(list(CONCEPT_ID = 44818707)))
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("device_type_concept_id", result))
})

test_that("build_device_exposure_sql handles DateAdjustment", {
  criteria <- list(CodesetId = 0, DateAdjustment = list(startOffset = 0, endOffset = 0, StartWith = "start_date", EndWith = "start_date"))
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("DATEADD", result))
})

# --- build_specimen_sql ---

test_that("build_specimen_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_specimen_sql(criteria)
  expect_true(grepl("SPECIMEN", result))
  expect_true(grepl("person_id", result))
})

test_that("build_specimen_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_specimen_sql(criteria)
  expect_true(grepl("ordinal", result))
})

# --- build_condition_era_sql ---

test_that("build_condition_era_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_condition_era_sql(criteria)
  expect_true(grepl("CONDITION_ERA", result))
  expect_true(grepl("person_id", result))
})

test_that("build_condition_era_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_condition_era_sql(criteria)
  expect_true(grepl("ordinal", result))
})

# --- build_drug_era_sql ---

test_that("build_drug_era_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_drug_era_sql(criteria)
  expect_true(grepl("DRUG_ERA", result))
  expect_true(grepl("person_id", result))
})

test_that("build_drug_era_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_drug_era_sql(criteria)
  expect_true(grepl("ordinal", result))
})

# --- build_dose_era_sql ---

test_that("build_dose_era_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_dose_era_sql(criteria)
  expect_true(grepl("DOSE_ERA", result))
  expect_true(grepl("person_id", result))
})

test_that("build_dose_era_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_dose_era_sql(criteria)
  expect_true(grepl("ordinal", result))
})

# --- build_visit_detail_sql ---

test_that("build_visit_detail_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_visit_detail_sql(criteria)
  expect_true(grepl("VISIT_DETAIL", result))
  expect_true(grepl("person_id", result))
})

test_that("build_visit_detail_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_visit_detail_sql(criteria)
  expect_true(grepl("ordinal", result))
})

# --- build_payer_plan_period_sql ---

test_that("build_payer_plan_period_sql generates valid SQL", {
  criteria <- list()
  result <- CDMConnector:::build_payer_plan_period_sql(criteria)
  expect_true(grepl("PAYER_PLAN_PERIOD", result))
  expect_true(grepl("person_id", result))
})

test_that("build_payer_plan_period_sql handles First flag", {
  criteria <- list(First = TRUE)
  result <- CDMConnector:::build_payer_plan_period_sql(criteria)
  expect_true(grepl("ordinal", result))
})

# --- get_criteria_sql (dispatch) ---

test_that("get_criteria_sql dispatches to ConditionOccurrence", {
  criteria <- list(ConditionOccurrence = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
})

test_that("get_criteria_sql dispatches to DrugExposure", {
  criteria <- list(DrugExposure = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("DRUG_EXPOSURE", result))
})

test_that("get_criteria_sql dispatches to Observation", {
  criteria <- list(Observation = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("OBSERVATION", result))
})

test_that("get_criteria_sql dispatches to Measurement", {
  criteria <- list(Measurement = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("MEASUREMENT", result))
})

test_that("get_criteria_sql dispatches to Death", {
  criteria <- list(Death = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("DEATH", result))
})

test_that("get_criteria_sql dispatches to DeviceExposure", {
  criteria <- list(DeviceExposure = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("DEVICE_EXPOSURE", result))
})

test_that("get_criteria_sql dispatches to Specimen", {
  criteria <- list(Specimen = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("SPECIMEN", result))
})

test_that("get_criteria_sql dispatches to ConditionEra", {
  criteria <- list(ConditionEra = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("CONDITION_ERA", result))
})

test_that("get_criteria_sql dispatches to DrugEra", {
  criteria <- list(DrugEra = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("DRUG_ERA", result))
})

test_that("get_criteria_sql dispatches to DoseEra", {
  criteria <- list(DoseEra = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("DOSE_ERA", result))
})

test_that("get_criteria_sql dispatches to VisitDetail", {
  criteria <- list(VisitDetail = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("VISIT_DETAIL", result))
})

test_that("get_criteria_sql dispatches to VisitOccurrence", {
  criteria <- list(VisitOccurrence = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

test_that("get_criteria_sql dispatches to ProcedureOccurrence", {
  criteria <- list(ProcedureOccurrence = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("PROCEDURE_OCCURRENCE", result))
})

test_that("get_criteria_sql dispatches to ObservationPeriod", {
  criteria <- list(ObservationPeriod = list())
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("OBSERVATION_PERIOD", result))
})

test_that("get_criteria_sql dispatches to PayerPlanPeriod", {
  criteria <- list(PayerPlanPeriod = list())
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("PAYER_PLAN_PERIOD", result))
})

test_that("get_criteria_sql dispatches to LocationRegion", {
  criteria <- list(LocationRegion = list(CodesetId = 0))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("LOCATION", result))
})

test_that("get_criteria_sql errors on unknown type", {
  expect_error(CDMConnector:::get_criteria_sql(list(unknown_type = list())), "Unknown")
})

# --- get_occurrence_operator ---

test_that("get_occurrence_operator maps types correctly", {
  expect_equal(CDMConnector:::get_occurrence_operator(0), "=")
  expect_equal(CDMConnector:::get_occurrence_operator(1), "<=")
  expect_equal(CDMConnector:::get_occurrence_operator(2), ">=")
})

# --- get_domain_concept_col ---

test_that("get_domain_concept_col returns correct columns", {
  expect_equal(CDMConnector:::get_domain_concept_col(list(ConditionOccurrence = list())), "condition_concept_id")
  expect_equal(CDMConnector:::get_domain_concept_col(list(DrugExposure = list())), "drug_concept_id")
  expect_equal(CDMConnector:::get_domain_concept_col(list(Measurement = list())), "measurement_concept_id")
  expect_equal(CDMConnector:::get_domain_concept_col(list(Observation = list())), "observation_concept_id")
  expect_equal(CDMConnector:::get_domain_concept_col(list(ProcedureOccurrence = list())), "procedure_concept_id")
})

test_that("get_domain_concept_col returns NULL for unknown type", {
  expect_null(CDMConnector:::get_domain_concept_col(list(Unknown = list())))
})
