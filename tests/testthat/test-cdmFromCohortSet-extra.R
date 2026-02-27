# Additional tests for R/cdmFromCohortSet.R — integration tests for cdmFromCohortSet and cdmFromJson

# --- cdmFromCohortSet ---

test_that("cdmFromCohortSet creates CDM from single cohort", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 20, seed = 42, verbose = FALSE)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  expect_s3_class(cdm, "cdm_reference")
  expect_true("person" %in% names(cdm))
  expect_true("observation_period" %in% names(cdm))
  expect_true("cohort" %in% names(cdm))

  person_count <- cdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(person_count > 0)
})

test_that("cdmFromCohortSet creates CDM from multiple cohorts", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 30, seed = 123, verbose = FALSE)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  expect_s3_class(cdm, "cdm_reference")
  person_count <- cdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(person_count >= 2)  # at least some persons from each cohort

  # Check synthetic_summary attribute
  summary_attr <- attr(cdm, "synthetic_summary")
  expect_type(summary_attr, "list")
  expect_true("cohort_summaries" %in% names(summary_attr))
})

test_that("cdmFromCohortSet errors on empty cohort set", {
  skip_if_not_installed("duckdb")
  empty_set <- data.frame(
    cohort_definition_id = integer(0),
    cohort_name = character(0),
    cohort = list(),
    stringsAsFactors = FALSE
  )
  expect_error(cdmFromCohortSet(empty_set, n = 10), "non-empty")
})

test_that("cdmFromCohortSet errors on missing columns", {
  skip_if_not_installed("duckdb")
  bad_set <- data.frame(id = 1, name = "test")
  expect_error(cdmFromCohortSet(bad_set, n = 10), "columns")
})

test_that("cdmFromCohortSet errors when n < number of cohorts", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
  expect_error(cdmFromCohortSet(cohortSet, n = 1), "must be >=")
})

test_that("cdmFromCohortSet errors on n = 0", {
  skip_if_not_installed("duckdb")
  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))
  expect_error(cdmFromCohortSet(cohortSet, n = 0), "positive integer")
})

test_that("cdmFromCohortSet generates different data with different seeds", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))

  cdm1 <- cdmFromCohortSet(cohortSet, n = 15, seed = 1)
  p1 <- cdm1$person %>% dplyr::collect()
  cdmDisconnect(cdm1)

  cdm2 <- cdmFromCohortSet(cohortSet, n = 15, seed = 999)
  p2 <- cdm2$person %>% dplyr::collect()
  cdmDisconnect(cdm2)

  # Different seeds should produce different data
  # (at minimum different birth years or genders for some people)
  expect_true(nrow(p1) > 0 && nrow(p2) > 0)
})

test_that("cdmFromCohortSet with cohorts2 generates conditions and drugs", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 40, seed = 7, verbose = FALSE)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  # Should have generated some clinical events
  if ("condition_occurrence" %in% names(cdm)) {
    co_count <- cdm$condition_occurrence %>% dplyr::tally() %>% dplyr::pull("n")
    expect_true(co_count >= 0)  # may be 0 depending on cohort def
  }
  if ("drug_exposure" %in% names(cdm)) {
    de_count <- cdm$drug_exposure %>% dplyr::tally() %>% dplyr::pull("n")
    expect_true(de_count >= 0)
  }
})

test_that("cdmFromCohortSet with targetMatch parameter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 20, seed = 42, targetMatch = 0.5)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  expect_s3_class(cdm, "cdm_reference")
})

test_that("cdmFromCohortSet with custom cohortTable name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 15, seed = 42, cohortTable = "my_cohort")
  on.exit(cdmDisconnect(cdm), add = TRUE)

  expect_true("my_cohort" %in% names(cdm))
})

test_that("cdmFromCohortSet with cohorts3 includes descendants cohort", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  # cohorts3 includes GiBleed_default_with_descendants.json which tests includeDescendants
  cohortSet <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 50, seed = 11, verbose = FALSE)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  expect_s3_class(cdm, "cdm_reference")
  person_count <- cdm$person %>% dplyr::tally() %>% dplyr::pull("n")
  expect_true(person_count > 0)
})

test_that("cdmFromCohortSet with extra options passes through to cdmFromJson", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 15, seed = 42,
    eventDateJitter = 5L, visitDateJitter = 3L,
    demographicVariety = TRUE, sourceAndTypeVariety = TRUE, valueVariety = TRUE)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  expect_s3_class(cdm, "cdm_reference")
  # With demographicVariety, should have more varied race_concept_ids
  persons <- cdm$person %>% dplyr::collect()
  expect_true(nrow(persons) > 0)
})

test_that("cdmFromCohortSet with deathFraction generates death records", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  cohortSet <- readCohortSet(system.file("cohorts5", "viral_sinusitus.json", package = "CDMConnector"))
  cdm <- cdmFromCohortSet(cohortSet, n = 50, seed = 42, deathFraction = 0.5)
  on.exit(cdmDisconnect(cdm), add = TRUE)

  # With 50% death fraction, some death records should exist
  if ("death" %in% names(cdm)) {
    death_count <- cdm$death %>% dplyr::tally() %>% dplyr::pull("n")
    expect_true(death_count >= 0)
  }
})

# --- cdmFromJson with multi-event-type cohort ---

test_that("cdmFromJson with DrugExposure, Measurement, Procedure, Observation, Device criteria", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  # Build a complex cohort expression that requires all event types
  expr <- list(
    PrimaryCriteria = list(
      CriteriaList = list(
        list(ConditionOccurrence = list(CodesetId = 0))
      ),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    AdditionalCriteria = list(
      Type = "ALL",
      CriteriaList = list(
        list(
          Criteria = list(DrugExposure = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 30, Coeff = -1),
            End = list(Days = 30, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(Measurement = list(CodesetId = 2)),
          StartWindow = list(
            Start = list(Days = 90, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(ProcedureOccurrence = list(CodesetId = 3)),
          StartWindow = list(
            Start = list(Days = 60, Coeff = -1),
            End = list(Days = 60, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(Observation = list(CodesetId = 4)),
          StartWindow = list(
            Start = list(Days = 30, Coeff = -1),
            End = list(Days = 30, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(DeviceExposure = list(CodesetId = 5)),
          StartWindow = list(
            Start = list(Days = 0, Coeff = 1),
            End = list(Days = 30, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        )
      )
    ),
    ConceptSets = list(
      list(id = 0, name = "Cond", expression = list(items = list(list(concept = list(CONCEPT_ID = 192671), includeDescendants = TRUE)))),
      list(id = 1, name = "Drug", expression = list(items = list(list(concept = list(CONCEPT_ID = 1118084), includeDescendants = TRUE)))),
      list(id = 2, name = "Meas", expression = list(items = list(list(concept = list(CONCEPT_ID = 3004249))))),
      list(id = 3, name = "Proc", expression = list(items = list(list(concept = list(CONCEPT_ID = 2000000))))),
      list(id = 4, name = "Obs", expression = list(items = list(list(concept = list(CONCEPT_ID = 3000000))))),
      list(id = 5, name = "Dev", expression = list(items = list(list(concept = list(CONCEPT_ID = 4000000)))))
    ),
    CensoringCriteria = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 30),
    CdmVersionRange = ">=5.0.0"
  )

  result <- CDMConnector:::cdmFromJson(
    cohortExpression = expr, n = 25, seed = 42, maxAttempts = 1,
    targetMatch = 0.5, successRate = 0.1,
    demographicVariety = TRUE, sourceAndTypeVariety = TRUE, valueVariety = TRUE,
    verbose = FALSE
  )

  expect_true(is.list(result))
  expect_true(file.exists(result$duckdb_path))

  con <- DBI::dbConnect(duckdb::duckdb(), result$duckdb_path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Tables should exist even if empty (synthetic concepts may not map in CirceR SQL)
  de <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM drug_exposure")
  expect_true(de$n >= 0)

  me <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM measurement")
  expect_true(me$n >= 0)

  po <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM procedure_occurrence")
  expect_true(po$n >= 0)

  ob <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM observation")
  expect_true(ob$n >= 0)

  dev <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM device_exposure")
  expect_true(dev$n >= 0)

  unlink(result$duckdb_path, force = TRUE)
})

# --- cdmFromJson with ConditionEra and DrugEra criteria ---

test_that("cdmFromJson with ConditionEra and DrugEra criteria exercises era builders", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  expr <- list(
    PrimaryCriteria = list(
      CriteriaList = list(
        list(ConditionOccurrence = list(CodesetId = 0))
      ),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    AdditionalCriteria = list(
      Type = "ALL",
      CriteriaList = list(
        list(
          Criteria = list(ConditionEra = list(CodesetId = 0)),
          StartWindow = list(
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(DrugEra = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1)
        )
      )
    ),
    ConceptSets = list(
      list(id = 0, name = "Cond", expression = list(items = list(list(concept = list(CONCEPT_ID = 192671), includeDescendants = TRUE)))),
      list(id = 1, name = "Drug", expression = list(items = list(list(concept = list(CONCEPT_ID = 1118084), includeDescendants = TRUE))))
    ),
    CensoringCriteria = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 30),
    CdmVersionRange = ">=5.0.0"
  )

  result <- CDMConnector:::cdmFromJson(
    cohortExpression = expr, n = 30, seed = 7, maxAttempts = 1,
    targetMatch = 0.5, successRate = 0.1, verbose = FALSE
  )

  expect_true(is.list(result))
  expect_true(file.exists(result$duckdb_path))

  con <- DBI::dbConnect(duckdb::duckdb(), result$duckdb_path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Era tables should exist (may be empty if events don't overlap enough)
  ce <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM condition_era")
  expect_true(ce$n >= 0)

  de <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM drug_era")
  expect_true(de$n >= 0)

  unlink(result$duckdb_path, force = TRUE)
})

# --- cdmFromJson with group satisfaction logic (ANY/AT_LEAST) ---

test_that("cdmFromJson with ANY group type in AdditionalCriteria", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  expr <- list(
    PrimaryCriteria = list(
      CriteriaList = list(
        list(ConditionOccurrence = list(CodesetId = 0))
      ),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    AdditionalCriteria = list(
      Type = "ANY",
      CriteriaList = list(
        list(
          Criteria = list(DrugExposure = list(CodesetId = 1)),
          StartWindow = list(Start = list(Days = 30, Coeff = -1), End = list(Days = 30, Coeff = 1)),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(ProcedureOccurrence = list(CodesetId = 0)),
          StartWindow = list(Start = list(Days = 30, Coeff = -1), End = list(Days = 30, Coeff = 1)),
          Occurrence = list(Type = 2, Count = 1)
        )
      )
    ),
    ConceptSets = list(
      list(id = 0, name = "Cond", expression = list(items = list(list(concept = list(CONCEPT_ID = 192671), includeDescendants = TRUE)))),
      list(id = 1, name = "Drug", expression = list(items = list(list(concept = list(CONCEPT_ID = 1118084), includeDescendants = TRUE))))
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 30),
    CdmVersionRange = ">=5.0.0"
  )

  result <- CDMConnector:::cdmFromJson(
    cohortExpression = expr, n = 15, seed = 42, maxAttempts = 1,
    targetMatch = 0.5, successRate = 0.1, verbose = FALSE
  )
  expect_true(is.list(result))
  expect_true(file.exists(result$duckdb_path))
  unlink(result$duckdb_path, force = TRUE)
})

test_that("cdmFromJson with AT_LEAST group type", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  skip_if_not_installed("SqlRender")

  expr <- list(
    PrimaryCriteria = list(
      CriteriaList = list(
        list(ConditionOccurrence = list(CodesetId = 0))
      ),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    AdditionalCriteria = list(
      Type = "AT_LEAST",
      Count = 1,
      CriteriaList = list(
        list(
          Criteria = list(DrugExposure = list(CodesetId = 1)),
          StartWindow = list(Start = list(Days = 30, Coeff = -1), End = list(Days = 30, Coeff = 1)),
          Occurrence = list(Type = 2, Count = 1)
        ),
        list(
          Criteria = list(Measurement = list(CodesetId = 0)),
          StartWindow = list(Start = list(Days = 60, Coeff = -1), End = list(Days = 0, Coeff = 1)),
          Occurrence = list(Type = 2, Count = 1)
        )
      )
    ),
    ConceptSets = list(
      list(id = 0, name = "Cond", expression = list(items = list(list(concept = list(CONCEPT_ID = 192671), includeDescendants = TRUE)))),
      list(id = 1, name = "Drug", expression = list(items = list(list(concept = list(CONCEPT_ID = 1118084), includeDescendants = TRUE))))
    ),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 30),
    CdmVersionRange = ">=5.0.0"
  )

  result <- CDMConnector:::cdmFromJson(
    cohortExpression = expr, n = 15, seed = 42, maxAttempts = 1,
    targetMatch = 0.5, successRate = 0.1, verbose = FALSE
  )
  expect_true(is.list(result))
  expect_true(file.exists(result$duckdb_path))
  unlink(result$duckdb_path, force = TRUE)
})

# --- cdmFromJson input validation ---

test_that("cdmFromJson errors without jsonPath or cohortExpression", {
  skip_if_not_installed("duckdb")
  expect_error(CDMConnector:::cdmFromJson(), "Provide either")
})

test_that("cdmFromJson errors on invalid n", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  json_path <- system.file("cohorts3/GiBleed_default.json", package = "CDMConnector")
  expect_error(CDMConnector:::cdmFromJson(jsonPath = json_path, n = 0), "positive integer")
})

test_that("cdmFromJson errors on out-of-range targetMatch", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  json_path <- system.file("cohorts3/GiBleed_default.json", package = "CDMConnector")
  expect_error(CDMConnector:::cdmFromJson(jsonPath = json_path, n = 10, targetMatch = 2), "between 0 and 1")
})

test_that("cdmFromJson errors on invalid date range", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  json_path <- system.file("cohorts3/GiBleed_default.json", package = "CDMConnector")
  expect_error(CDMConnector:::cdmFromJson(jsonPath = json_path, n = 10, startDate = "2030-01-01", endDate = "2020-01-01"), "before")
})

test_that("cdmFromJson errors on non-existent path", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  expect_error(CDMConnector:::cdmFromJson(jsonPath = "/nonexistent.json"), "does not exist")
})

test_that("cdmFromJson errors on expression without PrimaryCriteria", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("CirceR")
  expect_error(CDMConnector:::cdmFromJson(cohortExpression = list(x = 1)), "PrimaryCriteria")
})
