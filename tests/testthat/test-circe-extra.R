# Additional tests for R/circe.R — coverage for utility functions

# --- get_concept_ids ---

test_that("get_concept_ids extracts IDs from concept list", {
  concepts <- list(
    list(CONCEPT_ID = 1234),
    list(CONCEPT_ID = 5678)
  )
  result <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(result, c(1234L, 5678L))
})

test_that("get_concept_ids handles mixed case keys", {
  concepts <- list(
    list(conceptId = 100),
    list(ConceptId = 200)
  )
  result <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(result, c(100L, 200L))
})

test_that("get_concept_ids returns empty for NULL input", {
  expect_equal(CDMConnector:::get_concept_ids(NULL), integer(0))
})

test_that("get_concept_ids returns empty for empty list", {
  expect_equal(CDMConnector:::get_concept_ids(list()), integer(0))
})

test_that("get_concept_ids skips atomic values", {
  concepts <- list(42, list(CONCEPT_ID = 100))
  result <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(result, 100L)
})

# --- split_in_clause ---

test_that("split_in_clause handles single value", {
  result <- CDMConnector:::split_in_clause("col", 42)
  expect_true(grepl("col in \\(42\\)", result))
})

test_that("split_in_clause handles empty values", {
  result <- CDMConnector:::split_in_clause("col", integer(0))
  expect_equal(result, "NULL")
})

test_that("split_in_clause handles NULL values", {
  result <- CDMConnector:::split_in_clause("col", NULL)
  expect_equal(result, "NULL")
})

test_that("split_in_clause splits large lists", {
  result <- CDMConnector:::split_in_clause("col", 1:5, max_length = 2)
  expect_true(grepl(" or ", result))
})

test_that("split_in_clause doesn't split small lists", {
  result <- CDMConnector:::split_in_clause("col", 1:3, max_length = 1000)
  expect_false(grepl(" or ", result))
})

# --- get_operator ---

test_that("get_operator maps all operator types", {
  expect_equal(CDMConnector:::get_operator("lt"), "<")
  expect_equal(CDMConnector:::get_operator("lte"), "<=")
  expect_equal(CDMConnector:::get_operator("eq"), "=")
  expect_equal(CDMConnector:::get_operator("!eq"), "<>")
  expect_equal(CDMConnector:::get_operator("gt"), ">")
  expect_equal(CDMConnector:::get_operator("gte"), ">=")
})

test_that("get_operator is case insensitive", {
  expect_equal(CDMConnector:::get_operator("LT"), "<")
  expect_equal(CDMConnector:::get_operator("GTE"), ">=")
})

test_that("get_operator errors on unknown operator", {
  expect_error(CDMConnector:::get_operator("xyz"), "Unknown operator")
})

# --- date_string_to_sql ---

test_that("date_string_to_sql generates DATEFROMPARTS", {
  result <- CDMConnector:::date_string_to_sql("2020-06-15")
  expect_equal(result, "DATEFROMPARTS(2020, 6, 15)")
})

test_that("date_string_to_sql handles January 1st", {
  result <- CDMConnector:::date_string_to_sql("2000-01-01")
  expect_equal(result, "DATEFROMPARTS(2000, 1, 1)")
})

test_that("date_string_to_sql errors on invalid format", {
  expect_error(CDMConnector:::date_string_to_sql("2020/06/15"), "Invalid date format")
  expect_error(CDMConnector:::date_string_to_sql("20200615"), "Invalid date format")
})

# --- build_date_range_clause ---

test_that("build_date_range_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_date_range_clause("start_date", NULL))
})

test_that("build_date_range_clause handles single operator", {
  dr <- list(Op = "gt", Value = "2020-01-01")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl("start_date > ", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_date_range_clause handles between operator", {
  dr <- list(Op = "bt", Value = "2020-01-01", Extent = "2020-12-31")
  result <- CDMConnector:::build_date_range_clause("d", dr)
  expect_true(grepl("d >= ", result))
  expect_true(grepl("d <= ", result))
})

test_that("build_date_range_clause handles not-between operator", {
  dr <- list(Op = "!bt", Value = "2020-01-01", Extent = "2020-12-31")
  result <- CDMConnector:::build_date_range_clause("d", dr)
  expect_true(grepl("^not ", result))
})

test_that("build_date_range_clause returns NULL when value is missing", {
  dr <- list(Op = "gt")
  expect_null(CDMConnector:::build_date_range_clause("d", dr))
})

# --- build_numeric_range_clause ---

test_that("build_numeric_range_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_numeric_range_clause("col", NULL))
})

test_that("build_numeric_range_clause handles simple operator", {
  nr <- list(Op = "gte", Value = 10)
  result <- CDMConnector:::build_numeric_range_clause("col", nr)
  expect_true(grepl("col >= 10", result))
})

test_that("build_numeric_range_clause handles between", {
  nr <- list(Op = "bt", Value = 5, Extent = 10)
  result <- CDMConnector:::build_numeric_range_clause("col", nr)
  expect_true(grepl("col >= 5", result))
  expect_true(grepl("col <= 10", result))
})

test_that("build_numeric_range_clause handles format string", {
  nr <- list(Op = "eq", Value = 3.14)
  result <- CDMConnector:::build_numeric_range_clause("col", nr, format = "%.2f")
  expect_true(grepl("3.14", result))
})

test_that("build_numeric_range_clause handles not-between with format", {
  nr <- list(Op = "!bt", Value = 1.5, Extent = 9.5)
  result <- CDMConnector:::build_numeric_range_clause("col", nr, format = "%.1f")
  expect_true(grepl("^not ", result))
  expect_true(grepl("1.5", result))
  expect_true(grepl("9.5", result))
})

# --- build_text_filter_clause ---

test_that("build_text_filter_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_text_filter_clause(NULL, "col"))
})

test_that("build_text_filter_clause handles simple string", {
  result <- CDMConnector:::build_text_filter_clause("hello", "col")
  expect_equal(result, "col LIKE '%hello%'")
})

test_that("build_text_filter_clause escapes single quotes", {
  result <- CDMConnector:::build_text_filter_clause("it's", "col")
  expect_true(grepl("it''s", result))
})

test_that("build_text_filter_clause handles list with eq operator", {
  tf <- list(Text = "test", Op = "eq")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_equal(result, "col = 'test'")
})

test_that("build_text_filter_clause handles startsWith", {
  tf <- list(Text = "abc", Op = "startsWith")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_equal(result, "col LIKE 'abc%'")
})

test_that("build_text_filter_clause handles endsWith", {
  tf <- list(Text = "xyz", Op = "endsWith")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_equal(result, "col LIKE '%xyz'")
})

test_that("build_text_filter_clause handles contains", {
  tf <- list(Text = "mid", Op = "contains")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_equal(result, "col LIKE '%mid%'")
})

test_that("build_text_filter_clause handles !contains", {
  tf <- list(Text = "no", Op = "!contains")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_equal(result, "col NOT LIKE '%no%'")
})

test_that("build_text_filter_clause handles !eq", {
  tf <- list(Text = "bad", Op = "!eq")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_equal(result, "col <> 'bad'")
})

# --- scalar ---

test_that("scalar returns NULL for NULL input", {
  expect_null(CDMConnector:::scalar(NULL))
})

test_that("scalar returns first element from vector", {
  expect_equal(CDMConnector:::scalar(c(1, 2, 3)), 1)
})

test_that("scalar returns element from single-value", {
  expect_equal(CDMConnector:::scalar(42), 42)
})

test_that("scalar returns NULL for empty vector", {
  expect_null(CDMConnector:::scalar(character(0)))
})

# --- get_occurrence_operator ---

test_that("get_occurrence_operator maps all types", {
  expect_equal(CDMConnector:::get_occurrence_operator(0), "=")
  expect_equal(CDMConnector:::get_occurrence_operator(1), "<=")
  expect_equal(CDMConnector:::get_occurrence_operator(2), ">=")
})

test_that("get_occurrence_operator handles string input", {
  expect_equal(CDMConnector:::get_occurrence_operator("0"), "=")
  expect_equal(CDMConnector:::get_occurrence_operator("1"), "<=")
  expect_equal(CDMConnector:::get_occurrence_operator("2"), ">=")
})

test_that("get_occurrence_operator defaults for unknown type", {
  expect_equal(CDMConnector:::get_occurrence_operator(99), "=")
})

# --- extract_criteria ---

test_that("extract_criteria extracts ConditionOccurrence", {
  item <- list(ConditionOccurrence = list(CodesetId = 1))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ConditionOccurrence")
})

test_that("extract_criteria extracts DrugExposure", {
  item <- list(DrugExposure = list(CodesetId = 2))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DrugExposure")
})

test_that("extract_criteria extracts nested Criteria", {
  item <- list(Criteria = list(Measurement = list(CodesetId = 3)))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Measurement")
})

test_that("extract_criteria returns NULL for non-list", {
  expect_null(CDMConnector:::extract_criteria(42))
  expect_null(CDMConnector:::extract_criteria("string"))
})

test_that("extract_criteria returns NULL for empty list", {
  expect_null(CDMConnector:::extract_criteria(list()))
})

test_that("extract_criteria adds First=FALSE default", {
  item <- list(VisitOccurrence = list(CodesetId = 5))
  result <- CDMConnector:::extract_criteria(item)
  expect_false(result$data$First)
})

test_that("extract_criteria handles all domain types", {
  types <- c("ConditionOccurrence", "DrugExposure", "ProcedureOccurrence",
    "VisitOccurrence", "Observation", "Measurement", "DeviceExposure", "Specimen",
    "Death", "VisitDetail", "ObservationPeriod", "PayerPlanPeriod",
    "ConditionEra", "DrugEra", "DoseEra")
  for (t in types) {
    item <- setNames(list(list()), t)
    result <- CDMConnector:::extract_criteria(item)
    expect_equal(result$type, t, info = paste("Type:", t))
  }
})

# --- is_group_empty ---

test_that("is_group_empty returns TRUE for empty group", {
  expect_true(CDMConnector:::is_group_empty(list()))
})

test_that("is_group_empty returns FALSE for group with criteria", {
  group <- list(CriteriaList = list(list(x = 1)))
  expect_false(CDMConnector:::is_group_empty(group))
})

test_that("is_group_empty returns FALSE for group with subgroups", {
  group <- list(Groups = list(list(x = 1)))
  expect_false(CDMConnector:::is_group_empty(group))
})

test_that("is_group_empty returns FALSE for group with demographics", {
  group <- list(DemographicCriteriaList = list(list(x = 1)))
  expect_false(CDMConnector:::is_group_empty(group))
})

# --- get_date_adjustment_expression ---

test_that("get_date_adjustment_expression returns DATEADD expression", {
  da <- list(startOffset = 0, endOffset = 0)
  result <- CDMConnector:::get_date_adjustment_expression(da, "start_date", "end_date")
  expect_true(grepl("DATEADD", result))
  expect_true(grepl("start_date", result))
  expect_true(grepl("end_date", result))
})

test_that("get_date_adjustment_expression handles offsets", {
  da <- list(StartOffset = 5, EndOffset = -3)
  result <- CDMConnector:::get_date_adjustment_expression(da, "s", "e")
  expect_true(grepl("5", result))
  expect_true(grepl("-3", result))
})

# --- cohort_expression_from_json ---

test_that("cohort_expression_from_json parses valid JSON", {
  json_path <- system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector")
  json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
  result <- CDMConnector:::cohort_expression_from_json(json_str)
  expect_type(result, "list")
  expect_true("PrimaryCriteria" %in% names(result) || "primaryCriteria" %in% names(result))
})

# --- cohortExpressionFromJson ---

test_that("cohortExpressionFromJson reads from file path", {
  json_path <- system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector")
  result <- CDMConnector:::cohortExpressionFromJson(json_path)
  expect_type(result, "list")
})

test_that("cohortExpressionFromJson reads from JSON string", {
  json_path <- system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector")
  json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
  result <- CDMConnector:::cohortExpressionFromJson(json_str)
  expect_type(result, "list")
})

test_that("cohortExpressionFromJson errors on non-character", {
  expect_error(CDMConnector:::cohortExpressionFromJson(42), "character")
})

# --- buildCohortQuery ---

test_that("buildCohortQuery generates SQL from JSON path", {
  json_path <- system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector")
  result <- CDMConnector:::buildCohortQuery(json_path)
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

test_that("buildCohortQuery generates SQL from parsed expression", {
  json_path <- system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector")
  expr <- CDMConnector:::cohortExpressionFromJson(json_path)
  result <- CDMConnector:::buildCohortQuery(expr)
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

# --- get_primary_criteria helpers ---

test_that("get_primary_criteria extracts from cohort", {
  cohort <- list(PrimaryCriteria = list(CriteriaList = list()))
  result <- CDMConnector:::get_primary_criteria(cohort)
  expect_type(result, "list")
})

test_that("get_concept_sets returns empty for no concept sets", {
  cohort <- list()
  result <- CDMConnector:::get_concept_sets(cohort)
  expect_equal(result, list())
})

test_that("get_observation_window returns defaults", {
  pc <- list()
  result <- CDMConnector:::get_observation_window(pc)
  expect_equal(result$prior_days, 0)
  expect_equal(result$post_days, 0)
})

test_that("get_observation_window extracts values", {
  pc <- list(ObservationWindow = list(PriorDays = 365, PostDays = 30))
  result <- CDMConnector:::get_observation_window(pc)
  expect_equal(result$prior_days, 365)
  expect_equal(result$post_days, 30)
})

test_that("get_primary_limit_type returns default All", {
  pc <- list()
  result <- CDMConnector:::get_primary_limit_type(pc)
  expect_equal(result, "All")
})

test_that("get_primary_limit_type extracts First", {
  pc <- list(PrimaryCriteriaLimit = list(Type = "First"))
  result <- CDMConnector:::get_primary_limit_type(pc)
  expect_equal(result, "First")
})

# --- get_domain_concept_col ---

test_that("get_domain_concept_col returns column for ConditionOccurrence", {
  item <- list(ConditionOccurrence = list())
  result <- CDMConnector:::get_domain_concept_col(item)
  expect_equal(result, "condition_concept_id")
})

test_that("get_domain_concept_col returns column for DrugExposure", {
  item <- list(DrugExposure = list())
  result <- CDMConnector:::get_domain_concept_col(item)
  expect_equal(result, "drug_concept_id")
})

test_that("get_domain_concept_col returns NULL for unknown", {
  result <- CDMConnector:::get_domain_concept_col(42)
  expect_null(result)
})

# --- build_concept_set_expression_query ---

test_that("build_concept_set_expression_query handles empty expression", {
  expr <- list(items = list())
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("0=1", result))
})

test_that("build_concept_set_expression_query builds SQL for simple inclusion", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("1234", result))
  expect_true(grepl("concept_id", result))
})

test_that("build_concept_set_expression_query handles descendants", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 999), isExcluded = FALSE, includeDescendants = TRUE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("concept_ancestor", result, ignore.case = TRUE))
})

test_that("build_concept_set_expression_query handles exclusion", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = TRUE, includeDescendants = FALSE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("100", result))
  expect_true(grepl("200", result))
  # Exclusion should produce separate include/exclude parts
  expect_true(nchar(result) > 50)
})

# --- build_condition_occurrence_sql ---

test_that("build_condition_occurrence_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
  expect_true(grepl("condition_occurrence_id", result))
})

test_that("build_condition_occurrence_sql with First flag", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
  expect_true(grepl("row_number", result))
})

test_that("build_condition_occurrence_sql with date range", {
  criteria <- list(
    CodesetId = 1,
    OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01")
  )
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
})

test_that("build_condition_occurrence_sql with age filter", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "gte", Value = 18)
  )
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("build_condition_occurrence_sql with DateAdjustment", {
  criteria <- list(
    CodesetId = 1,
    DateAdjustment = list(
      StartWith = "start_date", EndWith = "end_date",
      StartOffset = 0, EndOffset = 0
    )
  )
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
})

# --- build_death_sql ---

test_that("build_death_sql generates basic SQL", {
  criteria <- list()
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("DEATH", result))
  expect_true(grepl("death_date", result))
})

test_that("build_death_sql with age filter", {
  criteria <- list(Age = list(Op = "gte", Value = 65))
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("PERSON", result))
})

# --- build_drug_exposure_sql ---

test_that("build_drug_exposure_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("DRUG_EXPOSURE", result))
  expect_true(grepl("drug_exposure_id", result))
})

test_that("build_drug_exposure_sql with First flag", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

test_that("build_drug_exposure_sql with DaysSupply filter", {
  criteria <- list(
    CodesetId = 1,
    DaysSupply = list(Op = "gte", Value = 30)
  )
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("days_supply", result))
})

test_that("build_drug_exposure_sql with Quantity filter", {
  criteria <- list(
    CodesetId = 1,
    Quantity = list(Op = "gte", Value = 1)
  )
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("quantity", result))
})

test_that("build_drug_exposure_sql with DateAdjustment", {
  criteria <- list(
    CodesetId = 1,
    DateAdjustment = list(
      StartWith = "start_date", EndWith = "end_date",
      StartOffset = 5, EndOffset = -5
    )
  )
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("DATEADD", result))
})

# --- build_procedure_occurrence_sql ---

test_that("build_procedure_occurrence_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("PROCEDURE_OCCURRENCE", result))
  expect_true(grepl("procedure_occurrence_id", result))
})

test_that("build_procedure_occurrence_sql with Quantity", {
  criteria <- list(CodesetId = 1, Quantity = list(Op = "gte", Value = 1))
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("quantity", result))
})

# --- build_measurement_sql ---

test_that("build_measurement_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("MEASUREMENT", result))
  expect_true(grepl("measurement_id", result))
})

test_that("build_measurement_sql with value range", {
  criteria <- list(
    CodesetId = 1,
    ValueAsNumber = list(Op = "gte", Value = 100)
  )
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("value_as_number", result))
})

test_that("build_measurement_sql with RangeHigh filter", {
  criteria <- list(
    CodesetId = 1,
    RangeHigh = list(Op = "lt", Value = 200)
  )
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("range_high", result))
})

# --- build_observation_sql ---

test_that("build_observation_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("OBSERVATION", result))
  expect_true(grepl("observation_id", result))
})

# --- build_visit_occurrence_sql ---

test_that("build_visit_occurrence_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
  expect_true(grepl("visit_occurrence_id", result))
})

test_that("build_visit_occurrence_sql with VisitLength", {
  criteria <- list(CodesetId = 1, VisitLength = list(Op = "gte", Value = 3))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("DATEDIFF", result))
})

# --- build_observation_period_sql ---

test_that("build_observation_period_sql generates basic SQL", {
  criteria <- list()
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("OBSERVATION_PERIOD", result))
  expect_true(grepl("observation_period_id", result))
})

test_that("build_observation_period_sql with First flag", {
  criteria <- list(First = TRUE)
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

# --- build_device_exposure_sql ---

test_that("build_device_exposure_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("DEVICE_EXPOSURE", result))
  expect_true(grepl("device_exposure_id", result))
})

# --- build_location_region_sql ---

test_that("build_location_region_sql generates SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_location_region_sql(criteria)
  expect_true(grepl("LOCATION", result))
  expect_true(grepl("region_concept_id", result))
})

# --- build_specimen_sql ---

test_that("build_specimen_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_specimen_sql(criteria)
  expect_true(grepl("SPECIMEN", result))
  expect_true(grepl("specimen_id", result))
})

# --- build_condition_era_sql ---

test_that("build_condition_era_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_condition_era_sql(criteria)
  expect_true(grepl("CONDITION_ERA", result))
})

test_that("build_condition_era_sql with EraLength criteria runs without error", {
  criteria <- list(CodesetId = 1, EraLength = list(Op = "gte", Value = 30))
  result <- CDMConnector:::build_condition_era_sql(criteria)
  expect_true(grepl("CONDITION_ERA", result))
})

# --- build_drug_era_sql ---

test_that("build_drug_era_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_drug_era_sql(criteria)
  expect_true(grepl("DRUG_ERA", result))
})

test_that("build_drug_era_sql with EraLength criteria runs without error", {
  criteria <- list(CodesetId = 1, EraLength = list(Op = "gte", Value = 30))
  result <- CDMConnector:::build_drug_era_sql(criteria)
  expect_true(grepl("DRUG_ERA", result))
})

# --- build_dose_era_sql ---

test_that("build_dose_era_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_dose_era_sql(criteria)
  expect_true(grepl("DOSE_ERA", result))
})

# --- build_visit_detail_sql ---

test_that("build_visit_detail_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_visit_detail_sql(criteria)
  expect_true(grepl("VISIT_DETAIL", result, ignore.case = TRUE))
})

# --- build_payer_plan_period_sql ---

test_that("build_payer_plan_period_sql generates basic SQL", {
  criteria <- list()
  result <- CDMConnector:::build_payer_plan_period_sql(criteria)
  expect_true(grepl("PAYER_PLAN_PERIOD", result))
})

# --- get_codeset_clause / get_codeset_join_expression ---

test_that("get_codeset_clause with source concept", {
  criteria <- list(CodesetId = 1, ConditionSourceConcept = 1)
  result <- CDMConnector:::get_codeset_clause(criteria, "co.condition_concept_id", "co.condition_source_concept_id")
  expect_true(grepl("source_concept_id", result) || grepl("concept_id", result))
})

test_that("get_codeset_join_expression with NULL codeset_id", {
  result <- CDMConnector:::get_codeset_join_expression(NULL, "co.condition_concept_id", NULL, NULL)
  expect_equal(result, "")
})

test_that("get_codeset_in_expression basic", {
  result <- CDMConnector:::get_codeset_in_expression(5, "C.concept_id", FALSE)
  expect_true(grepl("concept_id", result))
  expect_true(grepl("codeset_id = 5", result))
})

test_that("get_codeset_in_expression with exclusion", {
  result <- CDMConnector:::get_codeset_in_expression(5, "C.concept_id", TRUE)
  # Exclusion produces different SQL than non-exclusion
  result_no_excl <- CDMConnector:::get_codeset_in_expression(5, "C.concept_id", FALSE)
  expect_true(result != result_no_excl)
})

# --- add_provider_specialty_filter ---

test_that("add_provider_specialty_filter returns empty when no specialty", {
  result <- CDMConnector:::add_provider_specialty_filter(list(), "@cdm", "co")
  expect_null(result$select_col)
  expect_null(result$join_sql)
  expect_equal(length(result$where_parts), 0)
})

test_that("add_provider_specialty_filter with ProviderSpecialty", {
  criteria <- list(ProviderSpecialty = list(list(CONCEPT_ID = 1234)))
  result <- CDMConnector:::add_provider_specialty_filter(criteria, "@cdm", "co")
  expect_true(!is.null(result$join_sql))
  expect_true(grepl("PROVIDER", result$join_sql))
})

# date/numeric range clauses, mapped concepts in codeset query, etc.

# --- normalize_cohort_keys ---

test_that("normalize_cohort_keys handles deep nesting up to depth limit", {
  deep <- list(a = list(b = list(c = list(d = 1))))
  result <- CDMConnector:::normalize_cohort_keys(deep)
  expect_true(is.list(result))
  expect_equal(result$a$b$c$d, 1)
})

test_that("normalize_cohort_keys returns non-list unchanged", {
  expect_equal(CDMConnector:::normalize_cohort_keys(42), 42)
  expect_equal(CDMConnector:::normalize_cohort_keys("text"), "text")
})

test_that("normalize_cohort_keys handles empty list", {
  result <- CDMConnector:::normalize_cohort_keys(list())
  expect_equal(length(result), 0)
})

test_that("normalize_cohort_keys handles data.frame", {
  df <- data.frame(x = 1)
  result <- CDMConnector:::normalize_cohort_keys(df)
  expect_s3_class(result, "data.frame")
})

# --- get_concept_sets ---

test_that("get_concept_sets returns concept sets from cohort", {
  cohort <- list(ConceptSets = list(list(id = 0, name = "CS0")))
  result <- CDMConnector:::get_concept_sets(cohort)
  expect_equal(length(result), 1)
  expect_equal(result[[1]]$id, 0)
})

test_that("get_concept_sets returns empty list when missing", {
  result <- CDMConnector:::get_concept_sets(list())
  expect_equal(length(result), 0)
})

# --- get_primary_limit_type ---

test_that("get_primary_limit_type returns First", {
  pc <- list(PrimaryCriteriaLimit = list(Type = "First"))
  expect_equal(CDMConnector:::get_primary_limit_type(pc), "First")
})

test_that("get_primary_limit_type returns All by default", {
  expect_equal(CDMConnector:::get_primary_limit_type(list()), "All")
})

test_that("get_primary_limit_type handles NULL type", {
  pc <- list(PrimaryCriteriaLimit = list())
  expect_equal(CDMConnector:::get_primary_limit_type(pc), "All")
})

# --- get_criteria_list ---

test_that("get_criteria_list returns criteria from primary criteria", {
  pc <- list(CriteriaList = list(list(x = 1), list(y = 2)))
  result <- CDMConnector:::get_criteria_list(pc)
  expect_equal(length(result), 2)
})

test_that("get_criteria_list returns empty list when missing", {
  result <- CDMConnector:::get_criteria_list(list())
  expect_equal(length(result), 0)
})

# --- build_date_range_clause ---

test_that("build_date_range_clause handles between (bt) operator", {
  dr <- list(Op = "bt", Value = "2020-01-01", Extent = "2020-12-31")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl(">=", result))
  expect_true(grepl("<=", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_date_range_clause handles not between (!bt) operator", {
  dr <- list(Op = "!bt", Value = "2020-01-01", Extent = "2020-12-31")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl("^not ", result))
})

test_that("build_date_range_clause handles gt operator", {
  dr <- list(Op = "gt", Value = "2020-06-15")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl(">", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_date_range_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_date_range_clause("col", NULL))
})

test_that("build_date_range_clause returns NULL when Value missing for bt", {
  expect_null(CDMConnector:::build_date_range_clause("col", list(Op = "bt")))
})

# --- build_numeric_range_clause ---

test_that("build_numeric_range_clause handles between (bt) operator", {
  nr <- list(Op = "bt", Value = 10, Extent = 50)
  result <- CDMConnector:::build_numeric_range_clause("age", nr)
  expect_true(grepl(">=", result))
  expect_true(grepl("<=", result))
})

test_that("build_numeric_range_clause handles not between (!bt) operator", {
  nr <- list(Op = "!bt", Value = 10, Extent = 50)
  result <- CDMConnector:::build_numeric_range_clause("age", nr)
  expect_true(grepl("^not ", result))
})

test_that("build_numeric_range_clause handles bt with format", {
  nr <- list(Op = "bt", Value = 10.5, Extent = 20.3)
  result <- CDMConnector:::build_numeric_range_clause("value_as_number", nr, format = "%.2f")
  expect_true(grepl("10.50", result))
  expect_true(grepl("20.30", result))
})

test_that("build_numeric_range_clause handles simple op with format", {
  nr <- list(Op = "gte", Value = 100.5)
  result <- CDMConnector:::build_numeric_range_clause("value_as_number", nr, format = "%.1f")
  expect_true(grepl("100.5", result))
  expect_true(grepl(">=", result))
})

test_that("build_numeric_range_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_numeric_range_clause("col", NULL))
})

test_that("build_numeric_range_clause returns NULL when Value missing for simple op", {
  expect_null(CDMConnector:::build_numeric_range_clause("col", list(Op = "gt")))
})

# --- build_text_filter_clause ---

test_that("build_text_filter_clause handles plain string", {
  result <- CDMConnector:::build_text_filter_clause("hello", "src_value")
  expect_true(grepl("LIKE", result))
  expect_true(grepl("hello", result))
})

test_that("build_text_filter_clause handles eq op", {
  tf <- list(Text = "exact", Op = "eq")
  result <- CDMConnector:::build_text_filter_clause(tf, "src_value")
  expect_true(grepl("= 'exact'", result))
})

test_that("build_text_filter_clause handles startsWith", {
  tf <- list(Text = "prefix", Op = "startsWith")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("LIKE 'prefix%'", result))
})

test_that("build_text_filter_clause handles endsWith", {
  tf <- list(Text = "suffix", Op = "endsWith")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("LIKE '%suffix'", result))
})

test_that("build_text_filter_clause handles !eq", {
  tf <- list(Text = "nope", Op = "!eq")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("<>", result))
})

test_that("build_text_filter_clause handles !contains", {
  tf <- list(Text = "bad", Op = "!contains")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("NOT LIKE", result))
})

test_that("build_text_filter_clause handles contains", {
  tf <- list(Text = "partial", Op = "contains")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("LIKE '%partial%'", result))
})

test_that("build_text_filter_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_text_filter_clause(NULL, "col"))
})

test_that("build_text_filter_clause returns NULL when Text is missing", {
  expect_null(CDMConnector:::build_text_filter_clause(list(Op = "eq"), "col"))
})

test_that("build_text_filter_clause escapes single quotes", {
  result <- CDMConnector:::build_text_filter_clause("it's", "col")
  expect_true(grepl("it''s", result))
})

# --- date_string_to_sql ---

test_that("date_string_to_sql converts date string to DATEFROMPARTS", {
  result <- CDMConnector:::date_string_to_sql("2020-06-15")
  expect_equal(result, "DATEFROMPARTS(2020, 6, 15)")
})

test_that("date_string_to_sql errors on invalid format", {
  expect_error(CDMConnector:::date_string_to_sql("2020/06/15"), "Invalid date format")
})

# --- build_concept_set_expression_query with mapped concepts ---

test_that("build_concept_set_expression_query handles mapped concepts", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = TRUE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = FALSE, includeDescendants = TRUE, includeMapped = TRUE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr, "@vocab")
  expect_true(grepl("concept_relationship", result))
  expect_true(grepl("Maps to", result))
})

test_that("build_concept_set_expression_query handles exclude concepts via LEFT JOIN", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE, includeDescendants = TRUE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = TRUE, includeDescendants = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr, "@vocab")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("is null", result))
})

test_that("build_concept_set_expression_query handles exclude with descendants", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = TRUE, includeDescendants = TRUE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr, "@vocab")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("CONCEPT_ANCESTOR", result))
})

test_that("build_concept_set_expression_query handles exclude with mapped concepts", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = TRUE, includeMapped = TRUE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr, "@vocab")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("Maps to", result))
})

test_that("build_concept_set_expression_query handles exclude with mapped + descendants", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = TRUE, includeDescendants = TRUE, includeMapped = TRUE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr, "@vocab")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("Maps to", result))
})

test_that("build_concept_set_expression_query handles empty items", {
  expr <- list(items = list())
  result <- CDMConnector:::build_concept_set_expression_query(expr, "@vocab")
  expect_true(grepl("0=1", result))
})

# --- get_concept_ids ---

test_that("get_concept_ids extracts IDs from concept list", {
  concepts <- list(
    list(CONCEPT_ID = 10),
    list(CONCEPT_ID = 20),
    list(CONCEPT_ID = 30)
  )
  result <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(result, c(10L, 20L, 30L))
})

test_that("get_concept_ids handles camelCase conceptId", {
  concepts <- list(list(conceptId = 42))
  result <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(result, 42L)
})

test_that("get_concept_ids handles empty list", {
  expect_equal(CDMConnector:::get_concept_ids(list()), integer(0))
  expect_equal(CDMConnector:::get_concept_ids(NULL), integer(0))
})

test_that("get_concept_ids skips atomic elements", {
  concepts <- list(42, list(CONCEPT_ID = 100))
  result <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(result, 100L)
})

# --- get_codeset_clause ---

test_that("get_codeset_clause with standard-only codeset", {
  result <- CDMConnector:::get_codeset_clause(list(CodesetId = 5), "co.condition_concept_id", "co.condition_source_concept_id")
  expect_true(nchar(result) > 0)
})

# --- get_observation_window ---

test_that("get_observation_window extracts window from primary criteria", {
  pc <- list(ObservationWindow = list(PriorDays = 30, PostDays = 60))
  result <- CDMConnector:::get_observation_window(pc)
  expect_equal(result$prior_days, 30)
  expect_equal(result$post_days, 60)
})

test_that("get_observation_window returns defaults when missing", {
  result <- CDMConnector:::get_observation_window(list())
  expect_equal(result$prior_days, 0)
  expect_equal(result$post_days, 0)
})

# --- get_primary_criteria ---

test_that("get_primary_criteria extracts PrimaryCriteria", {
  cohort <- list(PrimaryCriteria = list(CriteriaList = list()))
  result <- CDMConnector:::get_primary_criteria(cohort)
  expect_true(is.list(result))
  expect_true("CriteriaList" %in% names(result))
})

# Targets uncovered lines in: split_in_clause, get_operator, extract_criteria,
# add_provider_specialty_filter, build_death_sql, build_observation_period_sql,
# build_location_region_sql, build_drug_exposure_sql, build_visit_occurrence_sql,
# build_procedure_occurrence_sql, build_measurement_sql, build_observation_sql,
# build_device_exposure_sql, build_specimen_sql, get_criteria_sql,
# get_codeset_join_expression, build_condition_occurrence_sql, etc.

# --- split_in_clause ---

test_that("split_in_clause handles small values", {
  result <- CDMConnector:::split_in_clause("concept_id", c(1, 2, 3))
  expect_true(grepl("concept_id in", result))
  expect_true(grepl("1,2,3", result))
})

test_that("split_in_clause handles empty/NULL values", {
  expect_equal(CDMConnector:::split_in_clause("x", NULL), "NULL")
  expect_equal(CDMConnector:::split_in_clause("x", integer(0)), "NULL")
})

test_that("split_in_clause chunks large lists", {
  result <- CDMConnector:::split_in_clause("x", 1:2500, max_length = 1000)
  # Should have multiple OR'd clauses
  expect_true(grepl(" or ", result))
})

# --- get_operator ---

test_that("get_operator returns correct SQL operators", {
  expect_equal(CDMConnector:::get_operator("lt"), "<")
  expect_equal(CDMConnector:::get_operator("lte"), "<=")
  expect_equal(CDMConnector:::get_operator("eq"), "=")
  expect_equal(CDMConnector:::get_operator("!eq"), "<>")
  expect_equal(CDMConnector:::get_operator("gt"), ">")
  expect_equal(CDMConnector:::get_operator("gte"), ">=")
})

test_that("get_operator errors on unknown operator", {
  expect_error(CDMConnector:::get_operator("invalid"), "Unknown operator")
})

# --- extract_criteria ---

test_that("extract_criteria extracts ConditionOccurrence", {
  item <- list(ConditionOccurrence = list(CodesetId = 1))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ConditionOccurrence")
  expect_equal(result$data$CodesetId, 1)
})

test_that("extract_criteria extracts DrugExposure", {
  item <- list(DrugExposure = list(CodesetId = 2))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DrugExposure")
})

test_that("extract_criteria extracts Death", {
  item <- list(Death = list(CodesetId = 3))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Death")
})

test_that("extract_criteria handles nested Criteria key", {
  item <- list(Criteria = list(Measurement = list(CodesetId = 4)))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Measurement")
})

test_that("extract_criteria handles Observation", {
  item <- list(Observation = list(CodesetId = 5))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Observation")
})

test_that("extract_criteria handles DeviceExposure", {
  item <- list(DeviceExposure = list(CodesetId = 6))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DeviceExposure")
})

test_that("extract_criteria handles Specimen", {
  item <- list(Specimen = list(CodesetId = 7))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Specimen")
})

test_that("extract_criteria handles VisitDetail", {
  item <- list(VisitDetail = list(CodesetId = 8))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "VisitDetail")
})

test_that("extract_criteria handles ObservationPeriod", {
  item <- list(ObservationPeriod = list())
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ObservationPeriod")
})

test_that("extract_criteria handles LocationRegion", {
  item <- list(LocationRegion = list(CodesetId = 9))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "LocationRegion")
})

test_that("extract_criteria handles PayerPlanPeriod", {
  item <- list(PayerPlanPeriod = list())
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "PayerPlanPeriod")
})

test_that("extract_criteria handles ConditionEra", {
  item <- list(ConditionEra = list(CodesetId = 10))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ConditionEra")
})

test_that("extract_criteria handles DrugEra", {
  item <- list(DrugEra = list(CodesetId = 11))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DrugEra")
})

test_that("extract_criteria handles DoseEra", {
  item <- list(DoseEra = list())
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DoseEra")
})

test_that("extract_criteria returns NULL for non-list input", {
  expect_null(CDMConnector:::extract_criteria("not a list"))
})

test_that("extract_criteria returns NULL for unknown types", {
  expect_null(CDMConnector:::extract_criteria(list(Unknown = list())))
})

test_that("extract_criteria sets First default to FALSE", {
  item <- list(ConditionOccurrence = list(CodesetId = 1))
  result <- CDMConnector:::extract_criteria(item)
  expect_false(result$data$First)
})

# --- get_codeset_join_expression ---

test_that("get_codeset_join_expression with both standard and source", {
  result <- CDMConnector:::get_codeset_join_expression(1, "co.concept_id", 2, "co.source_concept_id")
  expect_true(grepl("JOIN", result))
  expect_true(grepl("codeset_id = 1", result))
  expect_true(grepl("codeset_id = 2", result))
})

test_that("get_codeset_join_expression with NULL source", {
  result <- CDMConnector:::get_codeset_join_expression(1, "co.concept_id", NULL, NULL)
  expect_true(grepl("codeset_id = 1", result))
  expect_false(grepl("cns", result))
})

test_that("get_codeset_join_expression with NULL standard", {
  result <- CDMConnector:::get_codeset_join_expression(NULL, "co.concept_id", 2, "co.source_concept_id")
  expect_false(grepl("codeset_id = 1", result))
  expect_true(grepl("codeset_id = 2", result))
})

# --- add_provider_specialty_filter ---

test_that("add_provider_specialty_filter with concept IDs", {
  criteria <- list(ProviderSpecialty = list(list(CONCEPT_ID = 100, concept_id = 100)))
  result <- CDMConnector:::add_provider_specialty_filter(criteria, "@cdm_database_schema", "co")
  expect_true(!is.null(result$join_sql))
  expect_true(grepl("PROVIDER", result$join_sql))
})

test_that("add_provider_specialty_filter with no specialty", {
  criteria <- list()
  result <- CDMConnector:::add_provider_specialty_filter(criteria, "@cdm_database_schema", "co")
  expect_null(result$join_sql)
  expect_length(result$where_parts, 0)
})

# --- Domain SQL builders ---

test_that("build_death_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("DEATH", result))
  expect_true(grepl("Begin Death Criteria", result))
})

test_that("build_death_sql with DeathType filter", {
  criteria <- list(CodesetId = 1, DeathType = list(list(CONCEPT_ID = 100)))
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("death_type_concept_id", result))
})

test_that("build_death_sql with age and gender", {
  criteria <- list(CodesetId = 1, Age = list(Op = "gte", Value = 18),
                   Gender = list(list(CONCEPT_ID = 8507)))
  result <- CDMConnector:::build_death_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
})

test_that("build_observation_period_sql generates basic SQL", {
  criteria <- list(First = TRUE)
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("OBSERVATION_PERIOD", result))
  expect_true(grepl("ordinal = 1", result))
})

test_that("build_observation_period_sql with UserDefinedPeriod", {
  criteria <- list(UserDefinedPeriod = list(startDate = "2020-01-01", endDate = "2020-12-31"))
  result <- CDMConnector:::build_observation_period_sql(criteria)
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_location_region_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_location_region_sql(criteria)
  expect_true(grepl("LOCATION_HISTORY", result))
  expect_true(grepl("codeset_id = 1", result))
})

test_that("build_location_region_sql without codeset", {
  criteria <- list()
  result <- CDMConnector:::build_location_region_sql(criteria)
  expect_true(grepl("LOCATION_HISTORY", result))
  expect_false(grepl("codeset_id", result))
})

test_that("build_drug_exposure_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("DRUG_EXPOSURE", result))
})

test_that("build_drug_exposure_sql with First flag", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

test_that("build_visit_occurrence_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

test_that("build_visit_occurrence_sql with visit type exclude", {
  criteria <- list(CodesetId = 1, VisitType = list(list(CONCEPT_ID = 9201)),
                   VisitTypeExclude = TRUE)
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("not in", result))
})

test_that("build_procedure_occurrence_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("PROCEDURE_OCCURRENCE", result))
})

test_that("build_procedure_occurrence_sql with modifier and quantity", {
  criteria <- list(CodesetId = 1,
    Modifier = list(list(CONCEPT_ID = 100)),
    Quantity = list(Op = "gte", Value = 2))
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("modifier_concept_id", result))
  expect_true(grepl("quantity", result, ignore.case = TRUE))
})

test_that("build_measurement_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("MEASUREMENT", result))
})

test_that("build_measurement_sql with value range", {
  criteria <- list(CodesetId = 1, ValueAsNumber = list(Op = "gt", Value = 100))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("value_as_number", result))
})

test_that("build_measurement_sql with operator filter", {
  criteria <- list(CodesetId = 1, Operator = list(list(CONCEPT_ID = 4172703)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("operator_concept_id", result))
})

test_that("build_observation_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("OBSERVATION", result))
})

test_that("build_observation_sql with ValueAsString", {
  criteria <- list(CodesetId = 1, ValueAsString = list(Text = "test", Op = "contains"))
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("value_as_string", result))
  expect_true(grepl("LIKE", result))
})

test_that("build_device_exposure_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("DEVICE_EXPOSURE", result))
})

test_that("build_device_exposure_sql with quantity", {
  criteria <- list(CodesetId = 1, Quantity = list(Op = "eq", Value = 1))
  result <- CDMConnector:::build_device_exposure_sql(criteria)
  expect_true(grepl("quantity", result, ignore.case = TRUE))
})

test_that("build_specimen_sql generates basic SQL", {
  criteria <- list(CodesetId = 1)
  result <- CDMConnector:::build_specimen_sql(criteria)
  expect_true(grepl("SPECIMEN", result))
})

# --- get_criteria_sql dispatcher ---

test_that("get_criteria_sql dispatches ConditionOccurrence", {
  result <- CDMConnector:::get_criteria_sql(list(ConditionOccurrence = list(CodesetId = 1)))
  expect_true(grepl("CONDITION_OCCURRENCE", result))
})

test_that("get_criteria_sql dispatches DrugExposure", {
  result <- CDMConnector:::get_criteria_sql(list(DrugExposure = list(CodesetId = 1)))
  expect_true(grepl("DRUG_EXPOSURE", result))
})

test_that("get_criteria_sql dispatches ProcedureOccurrence", {
  result <- CDMConnector:::get_criteria_sql(list(ProcedureOccurrence = list(CodesetId = 1)))
  expect_true(grepl("PROCEDURE_OCCURRENCE", result))
})

test_that("get_criteria_sql dispatches VisitOccurrence", {
  result <- CDMConnector:::get_criteria_sql(list(VisitOccurrence = list(CodesetId = 1)))
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

test_that("get_criteria_sql dispatches Observation", {
  result <- CDMConnector:::get_criteria_sql(list(Observation = list(CodesetId = 1)))
  expect_true(grepl("OBSERVATION", result))
})

test_that("get_criteria_sql dispatches Measurement", {
  result <- CDMConnector:::get_criteria_sql(list(Measurement = list(CodesetId = 1)))
  expect_true(grepl("MEASUREMENT", result))
})

test_that("get_criteria_sql dispatches DeviceExposure", {
  result <- CDMConnector:::get_criteria_sql(list(DeviceExposure = list(CodesetId = 1)))
  expect_true(grepl("DEVICE_EXPOSURE", result))
})

test_that("get_criteria_sql dispatches Specimen", {
  result <- CDMConnector:::get_criteria_sql(list(Specimen = list(CodesetId = 1)))
  expect_true(grepl("SPECIMEN", result))
})

test_that("get_criteria_sql dispatches Death", {
  result <- CDMConnector:::get_criteria_sql(list(Death = list(CodesetId = 1)))
  expect_true(grepl("DEATH", result))
})

test_that("get_criteria_sql dispatches ObservationPeriod", {
  result <- CDMConnector:::get_criteria_sql(list(ObservationPeriod = list()))
  expect_true(grepl("OBSERVATION_PERIOD", result))
})

test_that("get_criteria_sql dispatches LocationRegion", {
  result <- CDMConnector:::get_criteria_sql(list(LocationRegion = list()))
  expect_true(grepl("LOCATION", result))
})

test_that("get_criteria_sql dispatches ConditionEra", {
  result <- CDMConnector:::get_criteria_sql(list(ConditionEra = list(CodesetId = 1)))
  expect_true(grepl("CONDITION_ERA", result))
})

test_that("get_criteria_sql dispatches DrugEra", {
  result <- CDMConnector:::get_criteria_sql(list(DrugEra = list(CodesetId = 1)))
  expect_true(grepl("DRUG_ERA", result))
})

# --- build_condition_occurrence_sql with complex criteria ---

test_that("build_condition_occurrence_sql with ConditionType exclude", {
  criteria <- list(CodesetId = 1,
    ConditionType = list(list(CONCEPT_ID = 38000177)),
    ConditionTypeExclude = TRUE)
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("not in", result))
  expect_true(grepl("condition_type_concept_id", result))
})

test_that("build_condition_occurrence_sql with DateAdjustment", {
  criteria <- list(CodesetId = 1,
    DateAdjustment = list(StartWith = "start_date", EndWith = "end_date",
                          StartOffset = 0, EndOffset = 0))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
})

test_that("build_condition_occurrence_sql with VisitType", {
  criteria <- list(CodesetId = 1, VisitType = list(list(CONCEPT_ID = 9201)))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
  expect_true(grepl("visit_concept_id", result))
})

test_that("build_condition_occurrence_sql with stop_reason", {
  criteria <- list(CodesetId = 1, StopReason = list(Text = "recovered", Op = "contains"))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("stop_reason", result))
  expect_true(grepl("LIKE", result))
})

test_that("build_condition_occurrence_sql with Age filter", {
  criteria <- list(CodesetId = 1, Age = list(Op = "bt", Value = 18, Extent = 65))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("build_condition_occurrence_sql with OccurrenceStartDate", {
  criteria <- list(CodesetId = 1,
    OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

# --- build_drug_exposure_sql with filters ---

test_that("build_drug_exposure_sql with days_supply and quantity", {
  criteria <- list(CodesetId = 1,
    DaysSupply = list(Op = "gte", Value = 30),
    Quantity = list(Op = "gt", Value = 0))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("days_supply", result))
  expect_true(grepl("quantity", result, ignore.case = TRUE))
})

test_that("build_drug_exposure_sql with Refills filter", {
  criteria <- list(CodesetId = 1, Refills = list(Op = "gte", Value = 1))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("refills", result))
})

# --- build_measurement_sql with complex filters ---

test_that("build_measurement_sql with unit codeset", {
  criteria <- list(CodesetId = 1, Unit = list(list(CONCEPT_ID = 8582)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("unit_concept_id", result))
})

test_that("build_measurement_sql with ValueAsConcept", {
  criteria <- list(CodesetId = 1, ValueAsConcept = list(list(CONCEPT_ID = 45877985)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("value_as_concept_id", result))
})

test_that("build_measurement_sql with range high/low", {
  criteria <- list(CodesetId = 1,
    RangeHigh = list(Op = "lte", Value = 100),
    RangeLow = list(Op = "gte", Value = 0))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("range_high", result))
  expect_true(grepl("range_low", result))
})

# --- build_observation_sql with filters ---

test_that("build_observation_sql with ValueAsConcept", {
  criteria <- list(CodesetId = 1, ValueAsConcept = list(list(CONCEPT_ID = 4188539)))
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("value_as_concept_id", result))
})

test_that("build_observation_sql with Unit filter", {
  criteria <- list(CodesetId = 1, Unit = list(list(CONCEPT_ID = 8582)))
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("unit_concept_id", result))
})

# --- Drug exposure complex filters ---

test_that("build_drug_exposure_sql with DrugType exclude", {
  criteria <- list(CodesetId = 1,
    DrugType = list(list(CONCEPT_ID = 38000177)),
    DrugTypeExclude = TRUE)
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("drug_type_concept_id", result))
  expect_true(grepl("not in", result))
})

test_that("build_drug_exposure_sql with RouteConcept", {
  criteria <- list(CodesetId = 1, RouteConcept = list(list(CONCEPT_ID = 4132161)))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("route_concept_id", result))
})

test_that("build_drug_exposure_sql with Age/Gender/VisitType", {
  criteria <- list(CodesetId = 1,
    Age = list(Op = "gte", Value = 18),
    Gender = list(list(CONCEPT_ID = 8507)),
    VisitType = list(list(CONCEPT_ID = 9201)))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

test_that("build_drug_exposure_sql with OccurrenceStartDate", {
  criteria <- list(CodesetId = 1,
    OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("start_date", result))
})

test_that("build_drug_exposure_sql with ProviderSpecialty", {
  criteria <- list(CodesetId = 1,
    ProviderSpecialty = list(list(CONCEPT_ID = 38004456, concept_id = 38004456)))
  result <- CDMConnector:::build_drug_exposure_sql(criteria)
  expect_true(grepl("PROVIDER", result))
})

# --- Visit occurrence complex filters ---

test_that("build_visit_occurrence_sql with DateAdjustment", {
  criteria <- list(CodesetId = 1,
    DateAdjustment = list(StartWith = "start_date", EndWith = "end_date",
                          StartOffset = 0, EndOffset = 0))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
})

test_that("build_visit_occurrence_sql with VisitLength", {
  criteria <- list(CodesetId = 1,
    VisitLength = list(Op = "gte", Value = 3))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("DATEDIFF", result))
})

test_that("build_visit_occurrence_sql with Age and Gender", {
  criteria <- list(CodesetId = 1,
    Age = list(Op = "bt", Value = 18, Extent = 65),
    Gender = list(list(CONCEPT_ID = 8532)))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("build_visit_occurrence_sql with ProviderSpecialty", {
  criteria <- list(CodesetId = 1,
    ProviderSpecialty = list(list(CONCEPT_ID = 38004456, concept_id = 38004456)))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("PROVIDER", result))
})

test_that("build_visit_occurrence_sql with OccurrenceStartDate", {
  criteria <- list(CodesetId = 1,
    OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
})

test_that("build_visit_occurrence_sql with OccurrenceEndDate", {
  criteria <- list(CodesetId = 1,
    OccurrenceEndDate = list(Op = "lt", Value = "2023-12-31"))
  result <- CDMConnector:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("end_date", result))
})

# --- Procedure, Measurement, Observation complex filters ---

test_that("build_procedure_occurrence_sql with Age and OccurrenceStartDate", {
  criteria <- list(CodesetId = 1,
    Age = list(Op = "gte", Value = 18),
    OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("start_date", result))
})

test_that("build_measurement_sql with Abnormal flag", {
  criteria <- list(CodesetId = 1, Abnormal = TRUE)
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("MEASUREMENT", result))
})

test_that("build_measurement_sql with Age and Gender", {
  criteria <- list(CodesetId = 1,
    Age = list(Op = "gte", Value = 18),
    Gender = list(list(CONCEPT_ID = 8507)))
  result <- CDMConnector:::build_measurement_sql(criteria)
  expect_true(grepl("PERSON", result))
})

test_that("build_observation_sql with ValueAsNumber", {
  criteria <- list(CodesetId = 1, ValueAsNumber = list(Op = "gt", Value = 10))
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("value_as_number", result))
})

test_that("build_observation_sql with Age", {
  criteria <- list(CodesetId = 1, Age = list(Op = "gte", Value = 21))
  result <- CDMConnector:::build_observation_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
})

# --- ConditionEra, DrugEra, DoseEra builders ---

test_that("build_condition_era_sql generates basic SQL", {
  result <- CDMConnector:::build_condition_era_sql(list(CodesetId = 1))
  expect_true(grepl("CONDITION_ERA", result))
})

test_that("build_condition_era_sql with EraLength", {
  criteria <- list(CodesetId = 1, EraLength = list(Op = "gte", Value = 30))
  result <- CDMConnector:::build_condition_era_sql(criteria)
  expect_true(grepl("DATEDIFF|era", result, ignore.case = TRUE))
})

test_that("build_drug_era_sql generates basic SQL", {
  result <- CDMConnector:::build_drug_era_sql(list(CodesetId = 1))
  expect_true(grepl("DRUG_ERA", result))
})

test_that("build_drug_era_sql with EraLength and OccurrenceCount", {
  criteria <- list(CodesetId = 1,
    EraLength = list(Op = "gte", Value = 30),
    OccurrenceCount = list(Op = "gte", Value = 2))
  result <- CDMConnector:::build_drug_era_sql(criteria)
  expect_true(grepl("DRUG_ERA", result))
})

test_that("build_dose_era_sql generates basic SQL", {
  result <- CDMConnector:::build_dose_era_sql(list(CodesetId = 1))
  expect_true(grepl("DOSE_ERA", result))
})

test_that("build_visit_detail_sql generates basic SQL", {
  result <- CDMConnector:::build_visit_detail_sql(list(CodesetId = 1))
  expect_true(grepl("VISIT_DETAIL", result))
})

test_that("build_payer_plan_period_sql generates basic SQL", {
  result <- CDMConnector:::build_payer_plan_period_sql(list())
  expect_true(grepl("PAYER_PLAN_PERIOD", result))
})

# --- Window criteria and correlated criteria ---

test_that("build_window_criteria handles start window with days", {
  sw <- list(
    UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1),
    End = list(Days = 365, Coeff = 1)
  )
  result <- CDMConnector:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("DATEADD", result))
})

test_that("build_window_criteria handles unbounded start (no Days)", {
  sw <- list(
    UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Coeff = -1),
    End = list(Days = 0, Coeff = 1)
  )
  result <- CDMConnector:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_START_DATE", result))
})

test_that("build_window_criteria handles unbounded end", {
  sw <- list(
    UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1),
    End = list(Coeff = 1)
  )
  result <- CDMConnector:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria with end window", {
  sw <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 30, Coeff = 1))
  ew <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 30, Coeff = 1))
  result <- CDMConnector:::build_window_criteria(sw, ew, TRUE)
  expect_true(grepl("DATEADD", result))
})

test_that("build_window_criteria with UseIndexEnd and UseEventEnd", {
  sw <- list(UseIndexEnd = TRUE, UseEventEnd = TRUE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 30, Coeff = 1))
  result <- CDMConnector:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("END_DATE", result))
})

# --- get_occurrence_operator ---

test_that("get_occurrence_operator returns correct operators", {
  expect_equal(CDMConnector:::get_occurrence_operator(0), "=")
  expect_equal(CDMConnector:::get_occurrence_operator(1), "<=")
  expect_equal(CDMConnector:::get_occurrence_operator(2), ">=")
})

# --- get_domain_concept_col ---

test_that("get_domain_concept_col returns correct column names", {
  expect_equal(CDMConnector:::get_domain_concept_col(list(ConditionOccurrence = list())), "condition_concept_id")
  expect_equal(CDMConnector:::get_domain_concept_col(list(DrugExposure = list())), "drug_concept_id")
  expect_equal(CDMConnector:::get_domain_concept_col(list(Measurement = list())), "measurement_concept_id")
  expect_null(CDMConnector:::get_domain_concept_col(list(Unknown = list())))
})

# --- is_group_empty ---

test_that("is_group_empty returns TRUE for empty group", {
  expect_true(CDMConnector:::is_group_empty(list(CriteriaList = list(), Groups = list(), DemographicCriteriaList = list())))
  expect_true(CDMConnector:::is_group_empty(list()))
})

test_that("is_group_empty returns FALSE for non-empty group", {
  expect_false(CDMConnector:::is_group_empty(list(CriteriaList = list(list(x = 1)))))
  expect_false(CDMConnector:::is_group_empty(list(Groups = list(list(Type = "ALL")))))
})

# --- get_corelated_criteria_query ---

test_that("get_corelated_criteria_query generates correlated SQL", {
  cc <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    Occurrence = list(Type = 2, Count = 1),
    StartWindow = list(
      UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    )
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("Correlated Criteria", result))
  expect_true(grepl("CONDITION_OCCURRENCE", result))
})

test_that("get_corelated_criteria_query with RestrictVisit", {
  cc <- list(
    Criteria = list(DrugExposure = list(CodesetId = 1)),
    Occurrence = list(Type = 2, Count = 1),
    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1), End = list(Days = 30, Coeff = 1)),
    RestrictVisit = TRUE
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("visit_occurrence_id", result))
})

test_that("get_corelated_criteria_query with IsDistinct", {
  cc <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = TRUE),
    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("domain_concept_id", result))
})

test_that("get_corelated_criteria_query with AtMost occurrence (left join template)", {
  cc <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    Occurrence = list(Type = 1, Count = 0),
    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("LEFT", result))
})

# --- get_demographic_criteria_query ---

test_that("get_demographic_criteria_query with Age and Gender", {
  dc <- list(Age = list(Op = "bt", Value = 18, Extent = 65),
             Gender = list(list(CONCEPT_ID = 8507)))
  result <- CDMConnector:::get_demographic_criteria_query(dc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("Demographic Criteria", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("get_demographic_criteria_query with Race and Ethnicity", {
  dc <- list(Race = list(list(CONCEPT_ID = 8516)),
             Ethnicity = list(list(CONCEPT_ID = 38003563)))
  result <- CDMConnector:::get_demographic_criteria_query(dc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("race_concept_id", result))
  expect_true(grepl("ethnicity_concept_id", result))
})

# --- get_criteria_group_query ---

test_that("get_criteria_group_query with ALL type", {
  group <- list(
    Type = "ALL", Count = 0,
    CriteriaList = list(
      list(
        Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
        Occurrence = list(Type = 2, Count = 1),
        StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
          Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
      )
    ),
    Groups = list(),
    DemographicCriteriaList = list()
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("Criteria Group", result))
  expect_true(grepl("HAVING COUNT", result))
})

test_that("get_criteria_group_query with ANY type", {
  group <- list(
    Type = "ANY", Count = 0,
    CriteriaList = list(
      list(Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
           Occurrence = list(Type = 2, Count = 1),
           StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
             Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1)))
    ),
    Groups = list(), DemographicCriteriaList = list()
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("HAVING COUNT.*> 0", result))
})

test_that("get_criteria_group_query with AT_MOST type", {
  group <- list(
    Type = "AT_MOST", Count = 0,
    CriteriaList = list(
      list(Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
           Occurrence = list(Type = 2, Count = 1),
           StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
             Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1)))
    ),
    Groups = list(), DemographicCriteriaList = list()
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("LEFT", result))
  expect_true(grepl("HAVING COUNT.*<= 0", result))
})

test_that("get_criteria_group_query with AT_LEAST type", {
  group <- list(
    Type = "AT_LEAST", Count = 2,
    CriteriaList = list(
      list(Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
           Occurrence = list(Type = 2, Count = 1),
           StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
             Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))),
      list(Criteria = list(DrugExposure = list(CodesetId = 2)),
           Occurrence = list(Type = 2, Count = 1),
           StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
             Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1)))
    ),
    Groups = list(), DemographicCriteriaList = list()
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("HAVING COUNT.*>= 2", result))
})

test_that("get_criteria_group_query with empty group", {
  group <- list(Type = "ALL", CriteriaList = list(), Groups = list(), DemographicCriteriaList = list())
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("Criteria Group", result))
})

test_that("get_criteria_group_query with nested Groups", {
  group <- list(
    Type = "ALL",
    CriteriaList = list(),
    Groups = list(
      list(Type = "ANY", Count = 0,
        CriteriaList = list(
          list(Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
               Occurrence = list(Type = 2, Count = 1),
               StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
                 Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1)))
        ),
        Groups = list(), DemographicCriteriaList = list())
    ),
    DemographicCriteriaList = list()
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("HAVING COUNT", result))
})

test_that("get_criteria_group_query with DemographicCriteriaList", {
  group <- list(
    Type = "ALL",
    CriteriaList = list(),
    Groups = list(),
    DemographicCriteriaList = list(
      list(Age = list(Op = "gte", Value = 18), Gender = list(list(CONCEPT_ID = 8507)))
    )
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("Demographic", result))
})

# --- wrap_criteria_query ---

test_that("wrap_criteria_query wraps base query with correlated group", {
  base <- "SELECT person_id, 1 as event_id, start_date, end_date, visit_occurrence_id, start_date as sort_date FROM condition"
  group <- list(
    Type = "ALL",
    CriteriaList = list(
      list(Criteria = list(DrugExposure = list(CodesetId = 2)),
           Occurrence = list(Type = 2, Count = 1),
           StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
             Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1)))
    ),
    Groups = list(), DemographicCriteriaList = list()
  )
  result <- CDMConnector:::wrap_criteria_query(base, group)
  expect_true(grepl("DRUG_EXPOSURE", result))
  expect_true(grepl("OBSERVATION_PERIOD", result))
})

# --- get_criteria_sql with CorrelatedCriteria ---

test_that("get_criteria_sql wraps with correlated criteria", {
  criteria <- list(ConditionOccurrence = list(
    CodesetId = 1,
    CorrelatedCriteria = list(
      Type = "ALL",
      CriteriaList = list(
        list(Criteria = list(DrugExposure = list(CodesetId = 2)),
             Occurrence = list(Type = 2, Count = 1),
             StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
               Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1)))
      ),
      Groups = list(), DemographicCriteriaList = list()
    )
  ))
  result <- CDMConnector:::get_criteria_sql(criteria)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
  expect_true(grepl("DRUG_EXPOSURE", result))
})

# --- collect_criteria_types_from_group ---

test_that("collect_criteria_types_from_group finds types in CriteriaList", {
  group <- list(
    CriteriaList = list(
      list(ConditionOccurrence = list(CodesetId = 1)),
      list(DrugExposure = list(CodesetId = 2))
    ),
    Groups = list()
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("ConditionOccurrence" %in% result)
  expect_true("DrugExposure" %in% result)
})

test_that("collect_criteria_types_from_group handles non-list input", {
  result <- CDMConnector:::collect_criteria_types_from_group("not a list")
  expect_equal(length(result), 0)
})

test_that("collect_criteria_types_from_group adds VisitOccurrence for VisitType filter", {
  group <- list(
    CriteriaList = list(
      list(ConditionOccurrence = list(CodesetId = 1, VisitType = list(list(CONCEPT_ID = 9201))))
    ),
    Groups = list()
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("VisitOccurrence" %in% result)
})

test_that("collect_criteria_types_from_group recurses into Groups", {
  group <- list(
    CriteriaList = list(),
    Groups = list(
      list(CriteriaList = list(list(Measurement = list(CodesetId = 1))), Groups = list())
    )
  )
  result <- CDMConnector:::collect_criteria_types_from_group(group)
  expect_true("Measurement" %in% result)
})

# --- build_cohort_query_internal (full integration) ---

test_that("build_cohort_query_internal generates complete cohort SQL", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, name = "test",
           expression = list(items = list(
             list(concept = list(CONCEPT_ID = 201826, CONCEPT_NAME = "Type 2 DM"),
                  isExcluded = FALSE, includeDescendants = TRUE, includeMapped = FALSE)
           )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(!is.null(result$full_sql))
  expect_true(nchar(result$full_sql) > 100)
  expect_true(grepl("CONDITION_OCCURRENCE", result$full_sql))
})

test_that("build_cohort_query_internal with CensorWindow", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, name = "test",
           expression = list(items = list(
             list(concept = list(CONCEPT_ID = 201826),
                  isExcluded = FALSE, includeDescendants = TRUE, includeMapped = FALSE)
           )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0),
    CensorWindow = list(startDate = "2020-01-01", endDate = "2023-12-31")
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("CASE WHEN", result$full_sql))
})

test_that("build_cohort_query_internal with CensoringCriteria", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, name = "cond",
           expression = list(items = list(
             list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
                  includeDescendants = TRUE, includeMapped = FALSE)
           ))),
      list(id = 1, name = "death",
           expression = list(items = list(
             list(concept = list(CONCEPT_ID = 4306655), isExcluded = FALSE,
                  includeDescendants = FALSE, includeMapped = FALSE)
           )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CensoringCriteria = list(list(Death = list(CodesetId = 1))),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("Censor", result$full_sql))
})

test_that("build_cohort_query_internal with EndStrategy DateOffset", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    EndStrategy = list(DateOffset = list(DateField = "StartDate", Offset = 30)),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("DATEADD", result$full_sql))
})

test_that("build_cohort_query_internal with CustomEra Drug end strategy", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 1112807), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "First")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "First"),
    EndStrategy = list(CustomEra = list(DrugCodesetId = 0, GapDays = 30, Offset = 0)),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("CustomEra|strategy_ends|DRUG_EXPOSURE", result$full_sql))
})

test_that("build_cohort_query_internal with InclusionRules", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      ))),
      list(id = 1, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 1112807), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 365, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(
      list(name = "Has drug",
           expression = list(
             Type = "ALL",
             CriteriaList = list(
               list(Criteria = list(DrugExposure = list(CodesetId = 1)),
                    Occurrence = list(Type = 2, Count = 1),
                    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
                      Start = list(Days = 365, Coeff = -1),
                      End = list(Days = 0, Coeff = 1)))
             ),
             Groups = list(), DemographicCriteriaList = list()
           ))
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("inclusion_rule", result$full_sql, ignore.case = TRUE))
  expect_true(grepl("DRUG_EXPOSURE", result$full_sql))
})

# --- Specific uncovered window criteria paths ---

test_that("build_window_criteria start window unbounded Coeff=1 uses OP_END_DATE", {
  sw <- list(
    UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Coeff = 1),  # No Days = unbounded toward end
    End = list(Days = 0, Coeff = 1)
  )
  result <- CDMConnector:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria end window unbounded start Coeff=-1", {
  sw <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 0, Coeff = 1))
  ew <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Coeff = -1),  # No Days = unbounded to start
    End = list(Days = 0, Coeff = 1))
  result <- CDMConnector:::build_window_criteria(sw, ew, TRUE)
  expect_true(grepl("OP_START_DATE", result))
})

test_that("build_window_criteria end window unbounded start Coeff=1", {
  sw <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 0, Coeff = 1))
  ew <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Coeff = 1),  # No Days, Coeff = 1 = unbounded toward end
    End = list(Days = 0, Coeff = 1))
  result <- CDMConnector:::build_window_criteria(sw, ew, TRUE)
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria end window unbounded end Coeff=-1", {
  sw <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 0, Coeff = 1))
  ew <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1),
    End = list(Coeff = -1))  # No Days, Coeff = -1 = unbounded toward start
  result <- CDMConnector:::build_window_criteria(sw, ew, TRUE)
  expect_true(grepl("OP_START_DATE", result))
})

test_that("build_window_criteria end window unbounded end Coeff=1", {
  sw <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1), End = list(Days = 0, Coeff = 1))
  ew <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Days = 0, Coeff = -1),
    End = list(Coeff = 1))  # No Days, Coeff = 1 = unbounded toward end
  result <- CDMConnector:::build_window_criteria(sw, ew, TRUE)
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria with check_observation_period=FALSE and no unbounded", {
  sw <- list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
    Start = list(Coeff = -1),  # Unbounded but check_observation_period = FALSE
    End = list(Days = 0, Coeff = 1))
  result <- CDMConnector:::build_window_criteria(sw, NULL, FALSE)
  # Should NOT contain OP_START_DATE (check_observation_period is FALSE)
  expect_false(grepl("OP_START_DATE", result))
})

# --- IsDistinct with specific CountColumn values ---

test_that("get_corelated_criteria_query with IsDistinct and START_DATE CountColumn", {
  cc <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = TRUE, CountColumn = "START_DATE"),
    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("domain_concept_id", result))
})

test_that("get_corelated_criteria_query with IsDistinct and DOMAIN_CONCEPT CountColumn", {
  cc <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = TRUE, CountColumn = "DOMAIN_CONCEPT"),
    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, "#primary_events", 0, "@cdm_database_schema")
  expect_true(grepl("domain_concept_id", result))
})

# --- get_corelated_criteria_query with event_table wrapping ---

test_that("get_corelated_criteria_query wraps SQL event table with obs period", {
  event_table <- "SELECT person_id, event_id, start_date, end_date, visit_occurrence_id FROM events"
  cc <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    Occurrence = list(Type = 2, Count = 1),
    StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
      Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
  )
  result <- CDMConnector:::get_corelated_criteria_query(cc, event_table, 0, "@cdm_database_schema")
  expect_true(grepl("op_start_date", result))
})

# --- ObservationPeriod with nested CorrelatedCriteria ---

test_that("get_criteria_group_query handles ObservationPeriod with CorrelatedCriteria", {
  group <- list(
    Type = "ALL",
    CriteriaList = list(
      list(
        Criteria = list(ObservationPeriod = list(
          CorrelatedCriteria = list(
            Type = "ALL",
            CriteriaList = list(
              list(
                Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
                Occurrence = list(Type = 2, Count = 1),
                StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
                  Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
              )
            ),
            Groups = list(), DemographicCriteriaList = list()
          )
        )),
        Occurrence = list(Type = 2, Count = 1),
        StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
          Start = list(Days = 0, Coeff = -1), End = list(Days = 365, Coeff = 1))
      )
    ),
    Groups = list(), DemographicCriteriaList = list()
  )
  result <- CDMConnector:::get_criteria_group_query(group, "#primary_events")
  expect_true(grepl("OBSERVATION_PERIOD", result))
  expect_true(grepl("CONDITION_OCCURRENCE", result))
})

# --- CustomEra Condition end strategy ---

test_that("build_cohort_query_internal with CustomEra Condition end strategy", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "First")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "First"),
    EndStrategy = list(CustomEra = list(ConditionCodesetId = 0, GapDays = 30)),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("CustomEra|strategy_ends|CONDITION_OCCURRENCE", result$full_sql))
})

# --- Additional criteria in build_cohort_query_internal ---

test_that("build_cohort_query_internal with AdditionalCriteria", {
  cohort <- list(
    ConceptSets = list(
      list(id = 0, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      ))),
      list(id = 1, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 8507), isExcluded = FALSE,
             includeDescendants = FALSE, includeMapped = FALSE)
      )))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    AdditionalCriteria = list(
      Type = "ALL",
      CriteriaList = list(
        list(Criteria = list(DrugExposure = list(CodesetId = 1)),
             Occurrence = list(Type = 2, Count = 1),
             StartWindow = list(UseIndexEnd = FALSE, UseEventEnd = FALSE,
               Start = list(Days = 365, Coeff = -1),
               End = list(Days = 0, Coeff = 1)))
      ),
      Groups = list(), DemographicCriteriaList = list()
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  result <- CDMConnector:::build_cohort_query_internal(cohort)
  expect_true(grepl("DRUG_EXPOSURE", result$full_sql))
})

# normalize_cohort_keys, getters, builder utilities, SQL builders

# --- cohort_expression_from_json ---

test_that("cohort_expression_from_json parses simple cohort", {
  json <- '{
    "ConceptSets": [{
      "id": 0, "name": "test",
      "expression": {"items": [{"concept": {"CONCEPT_ID": 1234}}]}
    }],
    "PrimaryCriteria": {
      "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 0}}],
      "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
      "PrimaryCriteriaLimit": {"Type": "First"}
    },
    "QualifiedLimit": {"Type": "First"},
    "ExpressionLimit": {"Type": "First"},
    "InclusionRules": [],
    "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0}
  }'
  result <- CDMConnector:::cohort_expression_from_json(json)
  expect_true(is.list(result))
  expect_true(!is.null(result$ConceptSets))
  expect_true(!is.null(result$PrimaryCriteria))
})

test_that("cohort_expression_from_json handles cdmVersionRange", {
  json <- '{"ConceptSets": [], "PrimaryCriteria": {"CriteriaList": []}, "cdmVersionRange": ">=5.0.0"}'
  result <- CDMConnector:::cohort_expression_from_json(json)
  expect_null(result$cdmVersionRange)
})

test_that("cohort_expression_from_json handles censorWindow", {
  # CensorWindow with content should be preserved
  json <- '{"ConceptSets": [], "PrimaryCriteria": {"CriteriaList": []}, "CensorWindow": {"StartDate": "2020-01-01"}}'
  result <- CDMConnector:::cohort_expression_from_json(json)
  expect_true(!is.null(result$CensorWindow))
})

# --- normalize_cohort_keys ---

test_that("normalize_cohort_keys handles non-list input", {
  expect_equal(CDMConnector:::normalize_cohort_keys("string"), "string")
  expect_equal(CDMConnector:::normalize_cohort_keys(42), 42)
})

test_that("normalize_cohort_keys handles empty list", {
  expect_equal(CDMConnector:::normalize_cohort_keys(list()), list())
})

test_that("normalize_cohort_keys handles data.frame", {
  df <- data.frame(x = 1)
  expect_equal(CDMConnector:::normalize_cohort_keys(df), df)
})

test_that("normalize_cohort_keys handles deeply nested lists", {
  x <- list(a = list(b = list(c = 1)))
  result <- CDMConnector:::normalize_cohort_keys(x)
  expect_equal(result$a$b$c, 1)
})

# --- get_primary_criteria ---

test_that("get_primary_criteria returns PrimaryCriteria", {
  cohort <- list(PrimaryCriteria = list(CriteriaList = list()))
  result <- CDMConnector:::get_primary_criteria(cohort)
  expect_true(is.list(result))
})

test_that("get_primary_criteria returns NULL when missing", {
  cohort <- list(other = "something")
  result <- CDMConnector:::get_primary_criteria(cohort)
  expect_null(result)
})

# --- get_concept_sets ---

test_that("get_concept_sets returns concept sets", {
  cohort <- list(ConceptSets = list(list(id = 0, name = "test")))
  result <- CDMConnector:::get_concept_sets(cohort)
  expect_equal(length(result), 1)
})

test_that("get_concept_sets returns empty list when missing", {
  cohort <- list(other = "x")
  result <- CDMConnector:::get_concept_sets(cohort)
  expect_equal(result, list())
})

# --- get_observation_window ---

test_that("get_observation_window returns defaults when missing", {
  pc <- list(CriteriaList = list())
  result <- CDMConnector:::get_observation_window(pc)
  expect_equal(result$prior_days, 0)
  expect_equal(result$post_days, 0)
})

test_that("get_observation_window returns values when present", {
  pc <- list(ObservationWindow = list(PriorDays = 30, PostDays = 60))
  result <- CDMConnector:::get_observation_window(pc)
  expect_equal(result$prior_days, 30)
  expect_equal(result$post_days, 60)
})

# --- get_primary_limit_type ---

test_that("get_primary_limit_type returns type", {
  pc <- list(PrimaryCriteriaLimit = list(Type = "First"))
  expect_equal(CDMConnector:::get_primary_limit_type(pc), "First")
})

test_that("get_primary_limit_type defaults to All", {
  pc <- list(CriteriaList = list())
  expect_equal(CDMConnector:::get_primary_limit_type(pc), "All")
})

# --- get_criteria_list ---

test_that("get_criteria_list returns criteria list", {
  pc <- list(CriteriaList = list(list(ConditionOccurrence = list())))
  result <- CDMConnector:::get_criteria_list(pc)
  expect_equal(length(result), 1)
})

test_that("get_criteria_list defaults to empty list", {
  pc <- list(other = "x")
  result <- CDMConnector:::get_criteria_list(pc)
  expect_equal(result, list())
})

# --- get_concept_ids ---

test_that("get_concept_ids extracts IDs from concept list", {
  concepts <- list(
    list(CONCEPT_ID = 1234),
    list(CONCEPT_ID = 5678)
  )
  ids <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(ids, c(1234L, 5678L))
})

test_that("get_concept_ids handles empty list", {
  expect_equal(CDMConnector:::get_concept_ids(list()), integer(0))
})

test_that("get_concept_ids handles NULL", {
  expect_equal(CDMConnector:::get_concept_ids(NULL), integer(0))
})

test_that("get_concept_ids handles camelCase", {
  concepts <- list(list(conceptId = 999))
  ids <- CDMConnector:::get_concept_ids(concepts)
  expect_equal(ids, 999L)
})

# --- split_in_clause ---

test_that("split_in_clause creates single IN clause", {
  result <- CDMConnector:::split_in_clause("col", c(1, 2, 3))
  expect_true(grepl("col in \\(1,2,3\\)", result))
})

test_that("split_in_clause handles NULL values", {
  expect_equal(CDMConnector:::split_in_clause("col", NULL), "NULL")
})

test_that("split_in_clause handles empty values", {
  expect_equal(CDMConnector:::split_in_clause("col", integer(0)), "NULL")
})

test_that("split_in_clause chunks large value lists", {
  vals <- 1:1500
  result <- CDMConnector:::split_in_clause("col", vals, max_length = 1000)
  expect_true(grepl("or", result))
})

# --- get_operator ---

test_that("get_operator returns correct operators", {
  expect_equal(CDMConnector:::get_operator("lt"), "<")
  expect_equal(CDMConnector:::get_operator("lte"), "<=")
  expect_equal(CDMConnector:::get_operator("eq"), "=")
  expect_equal(CDMConnector:::get_operator("gt"), ">")
  expect_equal(CDMConnector:::get_operator("gte"), ">=")
  expect_equal(CDMConnector:::get_operator("!eq"), "<>")
})

test_that("get_operator errors on unknown operator", {
  expect_error(CDMConnector:::get_operator("xyz"), "Unknown operator")
})

# --- date_string_to_sql ---

test_that("date_string_to_sql converts correctly", {
  result <- CDMConnector:::date_string_to_sql("2020-01-15")
  expect_true(grepl("DATEFROMPARTS", result))
  expect_true(grepl("2020", result))
  expect_true(grepl("15", result))
})

test_that("date_string_to_sql errors on invalid format", {
  expect_error(CDMConnector:::date_string_to_sql("2020/01/15"), "Invalid date format")
})

# --- build_date_range_clause ---

test_that("build_date_range_clause handles gt operator", {
  dr <- list(Op = "gt", Value = "2020-01-01")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl(">", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_date_range_clause handles bt (between)", {
  dr <- list(Op = "bt", Value = "2020-01-01", Extent = "2020-12-31")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl(">=", result))
  expect_true(grepl("<=", result))
})

test_that("build_date_range_clause handles !bt (not between)", {
  dr <- list(Op = "!bt", Value = "2020-01-01", Extent = "2020-12-31")
  result <- CDMConnector:::build_date_range_clause("start_date", dr)
  expect_true(grepl("not", result))
})

test_that("build_date_range_clause returns NULL when missing", {
  expect_null(CDMConnector:::build_date_range_clause("start_date", NULL))
})

# --- build_numeric_range_clause ---

test_that("build_numeric_range_clause handles eq operator", {
  nr <- list(Op = "eq", Value = 30)
  result <- CDMConnector:::build_numeric_range_clause("age", nr)
  expect_true(grepl("= 30", result))
})

test_that("build_numeric_range_clause handles bt (between)", {
  nr <- list(Op = "bt", Value = 20, Extent = 40)
  result <- CDMConnector:::build_numeric_range_clause("age", nr)
  expect_true(grepl(">= 20", result))
  expect_true(grepl("<= 40", result))
})

test_that("build_numeric_range_clause handles !bt (not between)", {
  nr <- list(Op = "!bt", Value = 20, Extent = 40)
  result <- CDMConnector:::build_numeric_range_clause("age", nr)
  expect_true(grepl("not", result))
})

test_that("build_numeric_range_clause handles format parameter", {
  nr <- list(Op = "gt", Value = 3.14)
  result <- CDMConnector:::build_numeric_range_clause("val", nr, format = "%.2f")
  expect_true(grepl("3.14", result))
})

test_that("build_numeric_range_clause handles bt with format", {
  nr <- list(Op = "bt", Value = 1.5, Extent = 2.5)
  result <- CDMConnector:::build_numeric_range_clause("val", nr, format = "%.1f")
  expect_true(grepl("1.5", result))
  expect_true(grepl("2.5", result))
})

test_that("build_numeric_range_clause returns NULL when missing", {
  expect_null(CDMConnector:::build_numeric_range_clause("age", NULL))
})

# --- build_text_filter_clause ---

test_that("build_text_filter_clause handles character input", {
  result <- CDMConnector:::build_text_filter_clause("search term", "col_name")
  expect_true(grepl("LIKE", result))
  expect_true(grepl("search term", result))
})

test_that("build_text_filter_clause handles eq operation", {
  tf <- list(Text = "exact", Op = "eq")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("= 'exact'", result))
})

test_that("build_text_filter_clause handles !eq operation", {
  tf <- list(Text = "exact", Op = "!eq")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("<> 'exact'", result))
})

test_that("build_text_filter_clause handles startsWith", {
  tf <- list(Text = "pre", Op = "startsWith")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("LIKE 'pre%'", result))
})

test_that("build_text_filter_clause handles endsWith", {
  tf <- list(Text = "suf", Op = "endsWith")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("LIKE '%suf'", result))
})

test_that("build_text_filter_clause handles contains", {
  tf <- list(Text = "mid", Op = "contains")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("LIKE '%mid%'", result))
})

test_that("build_text_filter_clause handles !contains", {
  tf <- list(Text = "mid", Op = "!contains")
  result <- CDMConnector:::build_text_filter_clause(tf, "col")
  expect_true(grepl("NOT LIKE", result))
})

test_that("build_text_filter_clause returns NULL for NULL input", {
  expect_null(CDMConnector:::build_text_filter_clause(NULL, "col"))
})

test_that("build_text_filter_clause escapes single quotes", {
  result <- CDMConnector:::build_text_filter_clause("it's", "col")
  expect_true(grepl("it''s", result))
})

# --- extract_criteria ---

test_that("extract_criteria extracts ConditionOccurrence", {
  item <- list(ConditionOccurrence = list(CodesetId = 0))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ConditionOccurrence")
  expect_true(is.list(result$data))
})

test_that("extract_criteria extracts DrugExposure", {
  item <- list(DrugExposure = list(CodesetId = 1))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DrugExposure")
})

test_that("extract_criteria extracts Measurement", {
  item <- list(Measurement = list(CodesetId = 2))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Measurement")
})

test_that("extract_criteria handles correlated Criteria", {
  item <- list(Criteria = list(ConditionOccurrence = list(CodesetId = 0)))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ConditionOccurrence")
})

test_that("extract_criteria returns NULL for non-list", {
  expect_null(CDMConnector:::extract_criteria("string"))
})

test_that("extract_criteria returns NULL for unknown type", {
  item <- list(UnknownType = list(CodesetId = 0))
  result <- CDMConnector:::extract_criteria(item)
  expect_null(result)
})

test_that("extract_criteria handles ProcedureOccurrence", {
  item <- list(ProcedureOccurrence = list(CodesetId = 3))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ProcedureOccurrence")
})

test_that("extract_criteria handles Observation", {
  item <- list(Observation = list(CodesetId = 4))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Observation")
})

test_that("extract_criteria handles Death", {
  item <- list(Death = list(CodesetId = 5))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "Death")
})

test_that("extract_criteria handles VisitOccurrence", {
  item <- list(VisitOccurrence = list(CodesetId = 6))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "VisitOccurrence")
})

test_that("extract_criteria handles DeviceExposure", {
  item <- list(DeviceExposure = list(CodesetId = 7))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DeviceExposure")
})

test_that("extract_criteria handles ConditionEra", {
  item <- list(ConditionEra = list(CodesetId = 8))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "ConditionEra")
})

test_that("extract_criteria handles DrugEra", {
  item <- list(DrugEra = list(CodesetId = 9))
  result <- CDMConnector:::extract_criteria(item)
  expect_equal(result$type, "DrugEra")
})

# --- get_codeset_join_expression ---

test_that("get_codeset_join_expression handles standard codeset", {
  result <- CDMConnector:::get_codeset_join_expression(0, "co.concept_id", NULL, NULL)
  expect_true(grepl("JOIN", result))
  expect_true(grepl("codeset_id = 0", result))
})

test_that("get_codeset_join_expression handles both standard and source", {
  result <- CDMConnector:::get_codeset_join_expression(0, "co.concept_id", 1, "co.source_concept_id")
  expect_true(grepl("cs on", result))
  expect_true(grepl("cns on", result))
})

test_that("get_codeset_join_expression handles NULL codeset", {
  result <- CDMConnector:::get_codeset_join_expression(NULL, "co.concept_id", NULL, NULL)
  expect_equal(result, "")
})

# --- get_codeset_in_expression ---

test_that("get_codeset_in_expression builds IN clause", {
  result <- CDMConnector:::get_codeset_in_expression(0, "concept_id")
  expect_true(grepl("in \\(select concept_id", result))
  expect_true(grepl("codeset_id = 0", result))
})

test_that("get_codeset_in_expression handles exclusion", {
  result <- CDMConnector:::get_codeset_in_expression(0, "concept_id", is_exclusion = TRUE)
  expect_true(grepl("not", result))
})

# --- get_codeset_where_clause ---

test_that("get_codeset_where_clause builds WHERE clause", {
  result <- CDMConnector:::get_codeset_where_clause(0, "concept_id")
  expect_true(grepl("WHERE", result))
})

test_that("get_codeset_where_clause returns empty for NULL", {
  result <- CDMConnector:::get_codeset_where_clause(NULL, "concept_id")
  expect_equal(result, "")
})

# --- get_date_adjustment_expression ---

test_that("get_date_adjustment_expression creates DATEADD expressions", {
  da <- list(startOffset = 5, endOffset = -3)
  result <- CDMConnector:::get_date_adjustment_expression(da, "start_col", "end_col")
  expect_true(grepl("DATEADD", result))
  expect_true(grepl("5", result))
  expect_true(grepl("-3", result))
  expect_true(grepl("start_date", result))
  expect_true(grepl("end_date", result))
})

# --- build_concept_set_expression_query ---

test_that("build_concept_set_expression_query handles simple include", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("1234", result))
  expect_true(grepl("CONCEPT", result))
})

test_that("build_concept_set_expression_query handles descendants", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = TRUE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("CONCEPT_ANCESTOR", result))
})

test_that("build_concept_set_expression_query handles mapped", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = TRUE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("Maps to", result))
})

test_that("build_concept_set_expression_query handles exclusions", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE),
    list(concept = list(CONCEPT_ID = 5678), isExcluded = TRUE, includeDescendants = FALSE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("is null", result))
})

test_that("build_concept_set_expression_query handles empty items", {
  expr <- list(items = list())
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("0=1", result))
})

test_that("build_concept_set_expression_query handles custom vocab schema", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr, vocabulary_schema = "my_vocab")
  expect_true(grepl("my_vocab", result))
})

test_that("build_concept_set_expression_query handles mapped descendants", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = TRUE, includeMapped = TRUE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("CONCEPT_ANCESTOR", result))
  expect_true(grepl("Maps to", result))
})

test_that("build_concept_set_expression_query handles excluded descendants", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE),
    list(concept = list(CONCEPT_ID = 5678), isExcluded = TRUE, includeDescendants = TRUE, includeMapped = FALSE)
  ))
  result <- CDMConnector:::build_concept_set_expression_query(expr)
  expect_true(grepl("CONCEPT_ANCESTOR", result))
  expect_true(grepl("LEFT JOIN", result))
})

# --- get_codeset_clause ---

test_that("get_codeset_clause builds join for standard codeset", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::get_codeset_clause(criteria, "co.condition_concept_id")
  expect_true(grepl("JOIN", result))
  expect_true(grepl("codeset_id = 0", result))
})

test_that("get_codeset_clause handles NULL CodesetId", {
  criteria <- list(other = 1)
  result <- CDMConnector:::get_codeset_clause(criteria, "co.concept_id")
  expect_equal(result, "")
})

# --- add_provider_specialty_filter ---

test_that("add_provider_specialty_filter returns empty result when no filter", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::add_provider_specialty_filter(criteria, "cdm", "co")
  expect_null(result$select_col)
  expect_null(result$join_sql)
  expect_equal(length(result$where_parts), 0)
})

test_that("add_provider_specialty_filter handles ProviderSpecialty", {
  criteria <- list(ProviderSpecialty = list(list(CONCEPT_ID = 38004455)))
  result <- CDMConnector:::add_provider_specialty_filter(criteria, "cdm", "co")
  expect_true(!is.null(result$join_sql))
  expect_true(grepl("PROVIDER", result$join_sql))
  expect_true(length(result$where_parts) >= 1)
})

# --- SQL builder functions ---

test_that("build_condition_occurrence_sql generates valid SQL", {
  criteria <- list(CodesetId = 0)
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("condition_occurrence", result, ignore.case = TRUE))
  expect_true(grepl("person_id", result))
  expect_true(grepl("start_date", result))
})

test_that("build_condition_occurrence_sql handles First flag", {
  criteria <- list(CodesetId = 0, First = TRUE)
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("ordinal", result))
  expect_true(grepl("row_number", result))
})

test_that("build_condition_occurrence_sql handles Age filter", {
  criteria <- list(CodesetId = 0, Age = list(Op = "gt", Value = 18))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("build_condition_occurrence_sql handles DateAdjustment", {
  criteria <- list(
    CodesetId = 0,
    DateAdjustment = list(startOffset = 5, endOffset = -3, StartWith = "start_date", EndWith = "start_date")
  )
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("DATEADD", result))
})

test_that("build_condition_occurrence_sql handles OccurrenceStartDate", {
  criteria <- list(CodesetId = 0, OccurrenceStartDate = list(Op = "gt", Value = "2020-01-01"))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_condition_occurrence_sql handles StopReason", {
  criteria <- list(CodesetId = 0, StopReason = "complete")
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("stop_reason", result))
  expect_true(grepl("LIKE", result))
})

test_that("build_condition_occurrence_sql handles ConditionType", {
  criteria <- list(CodesetId = 0, ConditionType = list(list(CONCEPT_ID = 38000183)))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("condition_type_concept_id", result))
  expect_true(grepl("38000183", result))
})

test_that("build_condition_occurrence_sql handles Gender filter", {
  criteria <- list(CodesetId = 0, Gender = list(list(CONCEPT_ID = 8507)))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("8507", result))
})

test_that("build_condition_occurrence_sql handles VisitType", {
  criteria <- list(CodesetId = 0, VisitType = list(list(CONCEPT_ID = 9201)))
  result <- CDMConnector:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
  expect_true(grepl("visit_concept_id", result))
})
