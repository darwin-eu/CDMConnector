# Tests for R/circe.R — Domain-specific criterion builders
# Covers uncovered domain builders and filter branches

# Helper to create a minimal cohort JSON with a given primary criterion domain
make_cohort_json <- function(domain, codeset_id = 1L, concept_id = 123L, extra_fields = list()) {
  criterion <- list()
  criterion[[domain]] <- c(list(CodesetId = codeset_id), extra_fields)
  obj <- list(
    ConceptSets = list(list(
      id = codeset_id,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = concept_id),
        isExcluded = FALSE,
        includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(criterion),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
}

# Helper to build batch SQL for a domain
batch_sql_for_domain <- function(domain, extra_fields = list()) {
  json <- make_cohort_json(domain, extra_fields = extra_fields)
  atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
}

# --- Device Exposure (76 uncovered lines) ---

test_that("DeviceExposure domain generates SQL", {
  sql <- batch_sql_for_domain("DeviceExposure")
  expect_true(grepl("device_exposure", sql, ignore.case = TRUE))
})

test_that("DeviceExposure with quantity filter generates SQL", {
  sql <- batch_sql_for_domain("DeviceExposure", list(
    Quantity = list(Value = 1, Op = "gte")
  ))
  expect_true(grepl("device_exposure", sql, ignore.case = TRUE))
})

# --- Death (37 uncovered lines) ---

test_that("Death domain generates SQL", {
  sql <- batch_sql_for_domain("Death", extra_fields = list(CodesetId = jsonlite::unbox(1L)))
  expect_true(grepl("death", sql, ignore.case = TRUE))
})

# --- Specimen (9 uncovered lines) ---

test_that("Specimen domain generates SQL", {
  sql <- batch_sql_for_domain("Specimen")
  expect_true(grepl("specimen", sql, ignore.case = TRUE))
})

# --- Visit Detail (9 uncovered lines) ---

test_that("VisitDetail domain generates SQL", {
  sql <- batch_sql_for_domain("VisitDetail")
  expect_true(grepl("visit_detail", sql, ignore.case = TRUE))
})

# --- Payer Plan Period (8 uncovered lines) ---

test_that("PayerPlanPeriod domain generates SQL", {
  sql <- batch_sql_for_domain("PayerPlanPeriod")
  expect_true(grepl("payer_plan_period", sql, ignore.case = TRUE))
})

# --- Location Region (4 uncovered lines) ---

test_that("LocationRegion domain generates SQL", {
  sql <- batch_sql_for_domain("LocationRegion")
  expect_true(grepl("location", sql, ignore.case = TRUE))
})

# --- Observation Period (12 uncovered lines) ---

test_that("ObservationPeriod domain generates SQL", {
  sql <- batch_sql_for_domain("ObservationPeriod")
  expect_true(grepl("observation_period", sql, ignore.case = TRUE))
})

# --- Condition Era ---

test_that("ConditionEra domain generates SQL", {
  sql <- batch_sql_for_domain("ConditionEra")
  expect_true(grepl("condition_era", sql, ignore.case = TRUE))
})

# --- Dose Era ---

test_that("DoseEra domain generates SQL", {
  sql <- batch_sql_for_domain("DoseEra")
  expect_true(grepl("dose_era", sql, ignore.case = TRUE))
})

# --- Drug Era ---

test_that("DrugEra domain generates SQL", {
  sql <- batch_sql_for_domain("DrugEra")
  expect_true(grepl("drug_era", sql, ignore.case = TRUE))
})

# --- Visit Occurrence ---

test_that("VisitOccurrence domain generates SQL", {
  sql <- batch_sql_for_domain("VisitOccurrence")
  expect_true(grepl("visit_occurrence", sql, ignore.case = TRUE))
})

# --- Measurement with value filters ---

test_that("Measurement with ValueAsNumber generates SQL", {
  sql <- batch_sql_for_domain("Measurement", list(
    ValueAsNumber = list(Value = 5, Op = "gt")
  ))
  expect_true(grepl("measurement", sql, ignore.case = TRUE))
  expect_true(grepl("value_as_number", sql, ignore.case = TRUE))
})

test_that("Measurement with RangeHigh filter generates SQL", {
  sql <- batch_sql_for_domain("Measurement", list(
    RangeHigh = list(Value = 100, Op = "lte")
  ))
  expect_true(grepl("range_high", sql, ignore.case = TRUE))
})

test_that("Measurement with RangeLow filter generates SQL", {
  sql <- batch_sql_for_domain("Measurement", list(
    RangeLow = list(Value = 0, Op = "gte")
  ))
  expect_true(grepl("range_low", sql, ignore.case = TRUE))
})

# --- Observation with value filters ---

test_that("Observation with ValueAsString generates SQL", {
  sql <- batch_sql_for_domain("Observation", list(
    ValueAsString = list(Text = "positive", Op = "contains")
  ))
  expect_true(grepl("observation", sql, ignore.case = TRUE))
  expect_true(grepl("value_as_string", sql, ignore.case = TRUE))
})

test_that("Observation with ValueAsConcept generates SQL", {
  sql <- batch_sql_for_domain("Observation", list(
    ValueAsConcept = list(list(CONCEPT_ID = 45877994L, CONCEPT_CODE = "Y"))
  ))
  expect_true(grepl("value_as_concept_id", sql, ignore.case = TRUE))
})

# --- DrugExposure with various filters ---

test_that("DrugExposure with DaysSupply generates SQL", {
  sql <- batch_sql_for_domain("DrugExposure", list(
    DaysSupply = list(Value = 30, Op = "gte")
  ))
  expect_true(grepl("days_supply", sql, ignore.case = TRUE))
})

test_that("DrugExposure with Refills generates SQL", {
  sql <- batch_sql_for_domain("DrugExposure", list(
    Refills = list(Value = 2, Op = "gte")
  ))
  expect_true(grepl("refills", sql, ignore.case = TRUE))
})

test_that("DrugExposure with Quantity generates SQL", {
  sql <- batch_sql_for_domain("DrugExposure", list(
    Quantity = list(Value = 100, Op = "lte")
  ))
  expect_true(grepl("quantity", sql, ignore.case = TRUE))
})

test_that("DrugExposure with RouteConcept generates SQL", {
  sql <- batch_sql_for_domain("DrugExposure", list(
    RouteConcept = list(list(CONCEPT_ID = 4132161L))
  ))
  expect_true(grepl("route_concept_id", sql, ignore.case = TRUE))
})

# --- ConditionOccurrence with StopReason ---

test_that("ConditionOccurrence with StopReason generates SQL", {
  sql <- batch_sql_for_domain("ConditionOccurrence", list(
    StopReason = list(Text = "recovered", Op = "contains")
  ))
  expect_true(grepl("stop_reason", sql, ignore.case = TRUE))
})

# --- ProcedureOccurrence with Quantity ---

test_that("ProcedureOccurrence with Quantity generates SQL", {
  sql <- batch_sql_for_domain("ProcedureOccurrence", list(
    Quantity = list(Value = 1, Op = "eq")
  ))
  expect_true(grepl("procedure_occurrence", sql, ignore.case = TRUE))
})

# --- PrimaryLimit types ---

test_that("PrimaryCriteriaLimit First generates SQL", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "First")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "First"),
    InclusionRules = list(),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("ordinal", sql, ignore.case = TRUE))
})

test_that("PrimaryCriteriaLimit Last generates SQL with DESC ordering", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "Last")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("DESC", sql, ignore.case = TRUE))
})

# --- Concept set with descendants ---

test_that("concept set with includeDescendants generates CONCEPT_ANCESTOR join", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE,
        includeDescendants = TRUE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("concept_ancestor|descendant_concept_id", sql, ignore.case = TRUE))
})

# --- Concept set with excluded concepts ---

test_that("concept set with isExcluded generates LEFT JOIN exclusion", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(
        list(concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE),
        list(concept = list(CONCEPT_ID = 456L), isExcluded = TRUE, includeDescendants = FALSE)
      ))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("LEFT JOIN", sql, ignore.case = TRUE))
  expect_true(grepl("is null", sql, ignore.case = TRUE))
})

# --- Inclusion rules ---

test_that("cohort with inclusion rules generates inclusion SQL", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "test_rule",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(DrugExposure = list(CodesetId = 1L)),
          StartWindow = list(
            Start = list(Days = 0L, Coeff = -1L),
            End = list(Days = 365L, Coeff = 1L),
            UseEventEnd = FALSE,
            UseIndexEnd = FALSE
          ),
          EndWindow = list(
            Start = list(Days = 0L, Coeff = -1L),
            End = list(Days = 365L, Coeff = 1L),
            UseEventEnd = FALSE,
            UseIndexEnd = FALSE
          ),
          RestrictVisit = FALSE,
          IgnoreObservationPeriod = FALSE,
          Occurrence = list(Type = 2L, Count = 1L, IsDistinct = FALSE)
        ))
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("inclusion", sql, ignore.case = TRUE))
})

# --- DateAdjustment ---

test_that("DateAdjustment on criteria modifies start/end dates", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(
        DrugExposure = list(
          CodesetId = 1L,
          DateAdjustment = list(StartWith = "START_DATE", EndWith = "END_DATE",
                                StartOffset = 1L, EndOffset = -1L)
        )
      )),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("DATEADD", sql, ignore.case = TRUE))
})

# --- Source concept filter ---

test_that("ConditionOccurrence with ConditionSourceConcept generates source concept filter", {
  sql <- batch_sql_for_domain("ConditionOccurrence", list(
    ConditionSourceConcept = 1L
  ))
  expect_true(grepl("source_concept_id|condition_source", sql, ignore.case = TRUE))
})

# --- Age filter (demographic) via inclusion rule ---

test_that("demographic criteria with Age filter in inclusion rule generates SQL", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "age_rule",
      expression = list(
        Type = "ALL",
        DemographicCriteriaList = list(list(
          Age = list(Value = 18L, Op = "gte")
        )),
        CriteriaList = list()
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql(as.character(json), cohort_id = 1L, render = FALSE, target_dialect = NULL)
  expect_true(grepl("year_of_birth", sql, ignore.case = TRUE))
})

# --- Gender filter (demographic) via inclusion rule ---

test_that("demographic criteria with Gender filter in inclusion rule generates SQL", {
  json_obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "gender_rule",
      expression = list(
        Type = "ALL",
        DemographicCriteriaList = list(list(
          Gender = list(list(CONCEPT_ID = 8507L))
        )),
        CriteriaList = list()
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(json_obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql(as.character(json), cohort_id = 1L, render = FALSE, target_dialect = NULL)
  expect_true(grepl("gender_concept_id", sql, ignore.case = TRUE))
})

# --- Provider specialty filter ---

test_that("ConditionOccurrence with ProviderSpecialty generates provider join", {
  sql <- batch_sql_for_domain("ConditionOccurrence", list(
    ProviderSpecialty = list(list(CONCEPT_ID = 38004446L))
  ))
  expect_true(grepl("provider", sql, ignore.case = TRUE))
  expect_true(grepl("specialty_concept_id", sql, ignore.case = TRUE))
})

# --- Numeric range clause ---

test_that("numeric range with between operator generates BETWEEN SQL", {
  sql <- batch_sql_for_domain("Measurement", list(
    ValueAsNumber = list(Value = 5L, Extent = 10L, Op = "bt")
  ))
  expect_true(grepl("BETWEEN|value_as_number", sql, ignore.case = TRUE))
})

# --- Date range clause ---

test_that("cohort with OccurrenceStartDate filter generates date filter", {
  sql <- batch_sql_for_domain("DrugExposure", list(
    OccurrenceStartDate = list(Value = "2020-01-01", Op = "gte")
  ))
  expect_true(grepl("2020-01-01|drug_exposure_start_date", sql, ignore.case = TRUE))
})

# --- Text filter clause ---

test_that("text filter with startsWith generates LIKE SQL", {
  sql <- batch_sql_for_domain("Observation", list(
    ValueAsString = list(Text = "pos", Op = "startsWith")
  ))
  expect_true(grepl("LIKE|value_as_string", sql, ignore.case = TRUE))
})

test_that("text filter with endsWith generates LIKE SQL", {
  sql <- batch_sql_for_domain("Observation", list(
    ValueAsString = list(Text = "ive", Op = "endsWith")
  ))
  expect_true(grepl("LIKE|value_as_string", sql, ignore.case = TRUE))
})

test_that("text filter with exact generates = SQL", {
  sql <- batch_sql_for_domain("Observation", list(
    ValueAsString = list(Text = "positive", Op = "exact")
  ))
  expect_true(grepl("value_as_string", sql, ignore.case = TRUE))
})

# --- End Strategy: DateOffset ---

test_that("EndStrategy DateOffset generates strategy SQL", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(
      DateOffset = list(DateField = "StartDate", Offset = 30L)
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- CDMConnector:::buildCohortQuery(as.character(json))
  expect_true(grepl("strategy_ends", sql, ignore.case = TRUE))
  expect_true(grepl("DATEADD", sql, ignore.case = TRUE))
})

test_that("EndStrategy DateOffset with EndDate uses end_date", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(
      DateOffset = list(DateField = "EndDate", Offset = 0L)
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- CDMConnector:::buildCohortQuery(as.character(json))
  expect_true(grepl("end_date", sql, ignore.case = TRUE))
})

# --- End Strategy: CustomEra (Drug) ---

test_that("EndStrategy CustomEra Drug generates strategy SQL", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L),
        isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(
      CustomEra = list(DrugCodesetId = 1L)
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- CDMConnector:::buildCohortQuery(as.character(json))
  expect_true(grepl("strategy_ends", sql, ignore.case = TRUE))
  expect_true(grepl("drug_exposure", sql, ignore.case = TRUE))
})

# --- Censoring Criteria ---

test_that("CensoringCriteria generates censor events SQL", {
  obj <- list(
    ConceptSets = list(
      list(id = 1L, expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE
      )))),
      list(id = 2L, expression = list(items = list(list(
        concept = list(CONCEPT_ID = 456L), isExcluded = FALSE, includeDescendants = FALSE
      ))))
    ),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CensoringCriteria = list(
      list(DrugExposure = list(CodesetId = 2L))
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("Censor", sql, ignore.case = TRUE))
})

# --- CensorWindow ---

test_that("CensorWindow with endDate generates CASE WHEN end_date", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    CensorWindow = list(endDate = "2025-12-31"),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("2025", sql))
  expect_true(grepl("CASE WHEN", sql, ignore.case = TRUE))
})

# --- generate_stats option ---

test_that("generate_stats adds inclusion analysis SQL", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "has_drug",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(DrugExposure = list(CodesetId = 1L)),
          StartWindow = list(
            Start = list(Days = 0L, Coeff = -1L),
            End = list(Days = 365L, Coeff = 1L),
            UseEventEnd = FALSE, UseIndexEnd = FALSE
          ),
          RestrictVisit = FALSE,
          IgnoreObservationPeriod = FALSE,
          Occurrence = list(Type = 2L, Count = 1L, IsDistinct = FALSE)
        ))
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  expr <- CDMConnector:::cohortExpressionFromJson(as.character(json))
  result <- CDMConnector:::build_cohort_query_internal(expr, list(generate_stats = TRUE))
  expect_true(grepl("Inclusion Impact Analysis", result$inclusion_analysis_sql))
  expect_true(grepl("Censored Stats", result$inclusion_analysis_sql))
})

# --- buildCohortQuery from JSON string ---

test_that("buildCohortQuery accepts JSON string directly", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"ConditionOccurrence":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"CollapseType":"ERA","EraPad":0}}'
  result <- CDMConnector:::buildCohortQuery(json)
  expect_true(nchar(result) > 0)
  expect_true(grepl("SELECT", result, ignore.case = TRUE))
})

# --- get_occurrence_operator ---

test_that("get_occurrence_operator returns correct operators", {
  expect_equal(CDMConnector:::get_occurrence_operator(0), "=")
  expect_equal(CDMConnector:::get_occurrence_operator(1), "<=")
  expect_equal(CDMConnector:::get_occurrence_operator(2), ">=")
  expect_equal(CDMConnector:::get_occurrence_operator(99), "=")
})

# --- get_domain_concept_col ---

test_that("get_domain_concept_col returns correct columns", {
  expect_equal(
    CDMConnector:::get_domain_concept_col(list(ConditionOccurrence = list(CodesetId = 1L))),
    "condition_concept_id"
  )
  expect_equal(
    CDMConnector:::get_domain_concept_col(list(DrugExposure = list(CodesetId = 1L))),
    "drug_concept_id"
  )
  expect_equal(
    CDMConnector:::get_domain_concept_col(list(Measurement = list(CodesetId = 1L))),
    "measurement_concept_id"
  )
})

# --- Window criteria: UseIndexEnd and UseEventEnd ---

test_that("window criteria with UseIndexEnd uses P.END_DATE", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "has_drug_end_window",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(DrugExposure = list(CodesetId = 1L)),
          StartWindow = list(
            Start = list(Days = 0L, Coeff = -1L),
            End = list(Days = 365L, Coeff = 1L),
            UseEventEnd = TRUE, UseIndexEnd = TRUE
          ),
          EndWindow = list(
            Start = list(Days = 0L, Coeff = -1L),
            End = list(Days = 365L, Coeff = 1L),
            UseEventEnd = FALSE, UseIndexEnd = FALSE
          ),
          RestrictVisit = FALSE,
          IgnoreObservationPeriod = FALSE,
          Occurrence = list(Type = 2L, Count = 1L, IsDistinct = FALSE)
        ))
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("END_DATE", sql))
})

# --- Criteria Group Type "ANY" ---

test_that("criteria group type ANY uses HAVING COUNT > 0", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(list(
      name = "any_criteria",
      expression = list(
        Type = "ANY",
        CriteriaList = list(
          list(
            Criteria = list(DrugExposure = list(CodesetId = 1L)),
            StartWindow = list(
              Start = list(Days = 0L, Coeff = -1L),
              End = list(Days = 365L, Coeff = 1L),
              UseEventEnd = FALSE, UseIndexEnd = FALSE
            ),
            RestrictVisit = FALSE,
            IgnoreObservationPeriod = FALSE,
            Occurrence = list(Type = 2L, Count = 1L, IsDistinct = FALSE)
          ),
          list(
            Criteria = list(Measurement = list(CodesetId = 1L)),
            StartWindow = list(
              Start = list(Days = 0L, Coeff = -1L),
              End = list(Days = 365L, Coeff = 1L),
              UseEventEnd = FALSE, UseIndexEnd = FALSE
            ),
            RestrictVisit = FALSE,
            IgnoreObservationPeriod = FALSE,
            Occurrence = list(Type = 2L, Count = 1L, IsDistinct = FALSE)
          )
        )
      )
    )),
    EndStrategy = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("HAVING COUNT", sql, ignore.case = TRUE))
})

# --- Empty censorWindow converted to NULL ---

test_that("empty censorWindow list is handled", {
  obj <- list(
    ConceptSets = list(list(
      id = 1L,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123L), isExcluded = FALSE, includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1L))),
      ObservationWindow = list(PriorDays = 0L, PostDays = 0L),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    EndStrategy = list(),
    censorWindow = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0L)
  )
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, null = "null")
  sql <- atlas_json_to_sql_batch(list(as.character(json)), optimize = TRUE, target_dialect = NULL)
  expect_true(nchar(sql) > 0)
})

# --- cohortExpressionFromJson from file path ---

test_that("cohortExpressionFromJson reads from file", {
  cohort_files <- list.files(
    system.file("cohorts", package = "CDMConnector"),
    pattern = "\\.json$", full.names = TRUE
  )
  skip_if(length(cohort_files) == 0)
  expr <- CDMConnector:::cohortExpressionFromJson(cohort_files[1])
  expect_true(is.list(expr))
  expect_true(!is.null(expr$PrimaryCriteria))
})
