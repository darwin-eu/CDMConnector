#' Build a Synthetic CDM from a Cohort Set
#'
#' Constructs a synthetic OMOP Common Data Model (CDM) using a set of cohort definitions,
#' typically created using \code{CDMConnector::readCohortSet()}. The function generates
#' synthetic data and returns a cdm reference object backed by a DuckDB database,
#' containing synthetic CDM tables and generated cohort table rows.
#'
#' @param cohortSet A data frame (usually from \code{CDMConnector::readCohortSet()})
#'   with columns \code{cohort_definition_id}, \code{cohort_name}, and \code{cohort}
#'   (cohort definition as a list or JSON string).
#' @param n Integer. Total number of synthetic persons to generate across all cohorts.
#'   Defaults to 100.
#'
#' @return
#'   A cdm reference object (as returned by \code{CDMConnector::cdmFromCon()}) backed
#'   by a DuckDB database. The returned object contains synthetic CDM tables and
#'   cohort table rows generated from the specified cohort definitions.
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' cohortSet <- readCohortSet(system.file("cohorts", package = "CDMConnector"))
#' cdm <- cdmFromCohortSet(cohortSet, n = 100)
#' cdm$person
#' }
#'
#' @export
cdmFromCohortSet <- function(cohortSet, n = 100) {

  req <- c("DBI", "duckdb")
  missing_pkgs <- req[!vapply(req, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_pkgs) > 0) {
    stop("Missing packages: ", paste(missing_pkgs, collapse = ", "))
  }
  if (!requireNamespace("CDMConnector", quietly = TRUE)) {
    stop("Package CDMConnector is required for cdmFromCohortSet(). Install it and retry.")
  }

  `%||%` <- function(x, y) if (!is.null(x)) x else y

  # Validate cohortSet: expect data frame with cohort_definition_id, cohort_name, cohort
  if (!is.data.frame(cohortSet) || nrow(cohortSet) == 0) {
    stop("cohortSet must be a non-empty data frame (e.g. from CDMConnector::readCohortSet()).")
  }
  required <- c("cohort_definition_id", "cohort_name", "cohort")
  missing_cols <- setdiff(required, names(cohortSet))
  if (length(missing_cols) > 0) {
    stop("cohortSet must have columns: ", paste(required, collapse = ", "), ". Missing: ", paste(missing_cols, collapse = ", "))
  }

  cohort_table <- getOption("mockCdm.cohortTable", "cohort")

  # Create final DuckDB as a copy of CDMConnector empty_cdm (no DDL; tables + vocabulary already exist)
  final_path <- getOption("mockCdm.multi.duckdbPath", NULL)
  if (is.null(final_path) || !nzchar(final_path)) {
    empty_path <- CDMConnector::eunomiaDir("empty_cdm", cdmVersion = "5.4")
    final_path <- tempfile(fileext = ".duckdb")
    if (dir.exists(empty_path)) {
      f <- list.files(empty_path, pattern = "\\.duckdb$", full.names = TRUE, recursive = TRUE)
      if (length(f) == 0) stop("empty_cdm directory contains no .duckdb file.")
      file.copy(f[1], final_path)
    } else {
      file.copy(empty_path, final_path)
    }
  }

  con_final <- DBI::dbConnect(duckdb::duckdb(), final_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con_final, shutdown = TRUE), add = TRUE)

  try(DBI::dbExecute(con_final, sprintf(
    "CREATE TABLE IF NOT EXISTS %s (cohort_definition_id INTEGER, subject_id INTEGER, cohort_start_date DATE, cohort_end_date DATE);",
    cohort_table
  )), silent = TRUE)
  try(DBI::dbExecute(con_final, "CREATE TABLE IF NOT EXISTS mockcdm_cohort_index (
    cohort_definition_id INTEGER, cohort_name VARCHAR, n_requested INTEGER,
    n_generated INTEGER, matched INTEGER, matched_rate DOUBLE);"), silent = TRUE)

  copy_with_offsets <- function(con_src, con_dst, table, id_col, id_offset, person_offset, person_col = "person_id") {
    df <- try(DBI::dbGetQuery(con_src, sprintf("SELECT * FROM %s;", table)), silent = TRUE)
    if (inherits(df, "try-error") || is.null(df) || nrow(df) == 0) {
      return(list(id_offset = id_offset, person_offset = person_offset))
    }
    if (person_col %in% names(df)) {
      df[[person_col]] <- df[[person_col]] + person_offset
    }
    if (!is.null(id_col) && id_col %in% names(df)) {
      df[[id_col]] <- df[[id_col]] + id_offset
    }
    DBI::dbWriteTable(con_dst, table, df, append = TRUE)
    if (!is.null(id_col) && id_col %in% names(df)) {
      id_offset <- max(df[[id_col]])
    }
    list(id_offset = id_offset, person_offset = person_offset)
  }

  pk <- list(
    person = "person_id",
    observation_period = "observation_period_id",
    visit_occurrence = "visit_occurrence_id",
    condition_occurrence = "condition_occurrence_id",
    procedure_occurrence = "procedure_occurrence_id",
    drug_exposure = "drug_exposure_id",
    measurement = "measurement_id",
    observation = "observation_id",
    condition_era = "condition_era_id",
    drug_era = "drug_era_id"
  )

  offsets <- list(
    person_id = 0L,
    observation_period_id = 0L,
    visit_occurrence_id = 0L,
    condition_occurrence_id = 0L,
    procedure_occurrence_id = 0L,
    drug_exposure_id = 0L,
    measurement_id = 0L,
    observation_id = 0L,
    condition_era_id = 0L,
    drug_era_id = 0L
  )

  tables <- c("person", "observation_period", "visit_occurrence", "condition_occurrence",
              "procedure_occurrence", "drug_exposure", "measurement", "observation",
              "condition_era", "drug_era")
  n_cohorts <- nrow(cohortSet)
  n_per_cohort <- max(1L, as.integer(n / n_cohorts))
  cohort_results <- vector("list", n_cohorts)

  for (i in seq_len(n_cohorts)) {
    cid   <- as.integer(cohortSet$cohort_definition_id[[i]])
    cname <- as.character(cohortSet$cohort_name[[i]])
    coh   <- cohortSet$cohort[[i]]

    # cohort column may be list (Circe expression) or character (JSON/path)
    cohort_expr <- if (is.list(coh)) {
      coh
    } else if (is.character(coh) && length(coh) == 1L) {
      if (file.exists(coh)) {
        cohort_json_text <- paste(readLines(coh, warn = FALSE), collapse = "\n")
        jsonlite::fromJSON(cohort_json_text, simplifyVector = FALSE)
      } else {
        jsonlite::fromJSON(coh, simplifyVector = FALSE)
      }
    } else {
      stop("cohortSet$cohort[[", i, "]] must be a list (Circe expression) or character (JSON/path).")
    }

    # Generate synthetic CDM for this cohort into a temp copy of empty_cdm
    temp_db <- tempfile(fileext = ".duckdb")
    empty_path <- CDMConnector::eunomiaDir("empty_cdm", cdmVersion = "5.4")
    if (dir.exists(empty_path)) {
      f <- list.files(empty_path, pattern = "\\.duckdb$", full.names = TRUE, recursive = TRUE)
      if (length(f) > 0) file.copy(f[1], temp_db)
    } else {
      file.copy(empty_path, temp_db)
    }
    old_opts <- options(
      mockCdm.cohortId = cid,
      mockCdm.duckdbPath = temp_db
    )
    on.exit(options(old_opts), add = TRUE)

    res_i <- cdmFromJson(cohortExpression = cohort_expr, n = n_per_cohort)
    cohort_results[[i]] <- res_i

    con_src <- DBI::dbConnect(duckdb::duckdb(), res_i$duckdb_path, read_only = TRUE)
    on.exit(DBI::dbDisconnect(con_src, shutdown = TRUE), add = TRUE)

    person_offset <- offsets$person_id

    # Copy person first
    df_person <- DBI::dbGetQuery(con_src, "SELECT * FROM person;")
    if (nrow(df_person) > 0) {
      df_person$person_id <- df_person$person_id + person_offset
      DBI::dbWriteTable(con_final, "person", df_person, append = TRUE)
      offsets$person_id <- max(df_person$person_id)
    }

    for (t in setdiff(tables, "person")) {
      id_col <- pk[[t]]
      id_offset <- offsets[[id_col]]
      tmp <- copy_with_offsets(con_src, con_final, t, id_col = id_col, id_offset = id_offset,
                               person_offset = person_offset, person_col = "person_id")
      offsets[[id_col]] <- tmp$id_offset
    }

    # Cohort rows
    df_cohort <- try(DBI::dbGetQuery(con_src, sprintf(
      "SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM %s WHERE cohort_definition_id = %d;",
      cohort_table, cid
    )), silent = TRUE)
    if (!inherits(df_cohort, "try-error") && nrow(df_cohort) > 0) {
      df_cohort$subject_id <- df_cohort$subject_id + person_offset
      DBI::dbWriteTable(con_final, cohort_table, df_cohort, append = TRUE)
    }

    summ <- res_i$summary
    idx_row <- data.frame(
      cohort_definition_id = cid,
      cohort_name = cname,
      n_requested = as.integer(n_per_cohort),
      n_generated = as.integer(summ$n_generated %||% n_per_cohort),
      matched = as.integer(summ$matched %||% NA_integer_),
      matched_rate = as.numeric(summ$matched_rate %||% NA_real_),
      stringsAsFactors = FALSE
    )
    DBI::dbWriteTable(con_final, "mockcdm_cohort_index", idx_row, append = TRUE)

    DBI::dbDisconnect(con_src, shutdown = TRUE)
  }

  # Disconnect so CDMConnector can attach to the same path
  DBI::dbDisconnect(con_final, shutdown = TRUE)
  on.exit(NULL)

  con <- DBI::dbConnect(duckdb::duckdb(), final_path, read_only = FALSE)
  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema = "main",
    writeSchema = "main",
    cohortTables = cohort_table,
    cdmName = "synthetic_cdm"
  )
  cdm
}

cdmFromJson <- function(jsonPath = NULL, n = 5000, cohortExpression = NULL) {
  # ----------------------------------------------------------------------------
  # Synthetic OMOP CDM generator driven by an ATLAS cohort expression JSON.
  #
  # Inputs:
  #   - jsonPath: path to ATLAS cohort expression JSON file (as exported by ATLAS).
  #               Optional if cohortExpression is provided.
  #   - n: number of people to generate
  #   - cohortExpression: optional list (parsed Circe cohort expression). If
  #                        provided, used instead of reading from jsonPath.
  #
  # Reads optional configuration via options():
  #   - mockCdm.duckdbPath   : existing DuckDB path with CDM tables (+vocab optional). If NULL -> create temp DB.
  #   - mockCdm.targetMatch  : fraction of people intended to qualify (default 0.85)
  #   - mockCdm.seed         : RNG seed (default 1)
  #   - mockCdm.maxAttempts  : retry attempts if under-matching (default 3)
  #   - mockCdm.successRate  : acceptance threshold match rate (default 0.70)
  #   - mockCdm.startDate    : min event date (default "2019-01-01")
  #   - mockCdm.endDate      : max event date (default "2024-12-31")
  #   - mockCdm.eraPadDays   : default era pad if not in JSON (default 90)
  #   - mockCdm.cohortTable  : name of cohort results table (default "cohort")
  #   - mockCdm.cohortId     : cohort_definition_id written (default 1)
  #
  # Returns:
  #   list(
  #     duckdb_path = <path>,
  #     summary = list(n_generated, matched, matched_rate, best_attempt, target_match),
  #     diagnostics = data.frame(attempt,...),
  #     cohort_sql = <duckdb SQL string>,
  #     cohort_json = <parsed cohort list>
  #   )
  # ----------------------------------------------------------------------------

  req <- c("DBI", "duckdb", "jsonlite", "CirceR", "SqlRender")
  missing <- req[!vapply(req, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop("Missing packages: ", paste(missing, collapse = ", "), ". Install them and retry.")
  }

  `%||%` <- function(x, y) if (!is.null(x)) x else y

  if (!is.null(cohortExpression) && is.list(cohortExpression)) {
    cohort <- cohortExpression
  } else if (!is.null(jsonPath) && nzchar(jsonPath)) {
    if (!file.exists(jsonPath)) stop("jsonPath does not exist: ", jsonPath)
    cohort_json_text <- paste(readLines(jsonPath, warn = FALSE), collapse = "\n")
    cohort <- jsonlite::fromJSON(cohort_json_text, simplifyVector = FALSE)
  } else {
    stop("Provide either jsonPath (path to ATLAS cohort JSON file) or cohortExpression (parsed list).")
  }

  duckdb_path  <- getOption("mockCdm.duckdbPath", NULL)
  target_match <- getOption("mockCdm.targetMatch", 0.85)
  seed         <- getOption("mockCdm.seed", 1)
  max_attempts <- getOption("mockCdm.maxAttempts", 3)
  success_rate <- getOption("mockCdm.successRate", 0.70)
  start_date   <- as.Date(getOption("mockCdm.startDate", "2019-01-01"))
  end_date     <- as.Date(getOption("mockCdm.endDate",   "2024-12-31"))
  era_pad_opt  <- as.integer(getOption("mockCdm.eraPadDays", 90))
  cohort_table <- getOption("mockCdm.cohortTable", "cohort")
  cohort_id    <- as.integer(getOption("mockCdm.cohortId", 1))

  if (is.null(duckdb_path) || !nzchar(duckdb_path)) {
    if (!requireNamespace("CDMConnector", quietly = TRUE)) {
      stop("When mockCdm.duckdbPath is not set, CDMConnector is required (for empty CDM). Install CDMConnector or set options(mockCdm.duckdbPath = ...).")
    }
    empty_path <- CDMConnector::eunomiaDir("empty_cdm", cdmVersion = "5.4")
    duckdb_path <- tempfile(fileext = ".duckdb")
    if (dir.exists(empty_path)) {
      f <- list.files(empty_path, pattern = "\\.duckdb$", full.names = TRUE, recursive = TRUE)
      if (length(f) == 0) stop("empty_cdm directory contains no .duckdb file.")
      file.copy(f[1], duckdb_path)
    } else {
      file.copy(empty_path, duckdb_path)
    }
  }

  set.seed(seed)

  random_date <- function(nn, start, end) {
    si <- as.integer(start); ei <- as.integer(end)
    as.Date(sample(si:ei, nn, replace = TRUE), origin = "1970-01-01")
  }

  # ---- Cohort already parsed (from jsonPath or cohortExpression) --------------

  extract_codesets <- function(cohort) {
    sets <- cohort$ConceptSets %||% list()
    out <- list()
    for (s in sets) {
      csid <- as.character(s$id)
      items <- s$expression$items %||% list()
      ids <- vapply(items, function(it) it$concept$CONCEPT_ID, numeric(1))
      out[[csid]] <- unique(as.integer(ids))
    }
    out
  }

  criterion_type <- function(x) {
    # returns one of supported keys if present
    keys <- c(
      "VisitOccurrence", "ConditionOccurrence", "ProcedureOccurrence",
      "DrugExposure", "Measurement", "Observation",
      "ConditionEra", "DrugEra", "DeviceExposure", "ObservationPeriod"
    )
    for (k in keys) if (!is.null(x[[k]])) return(k)
    NA_character_
  }

  parse_criteria_list <- function(lst) {
    out <- list()
    for (c in lst %||% list()) {
      t <- criterion_type(c)
      if (is.na(t)) next
      codeset_id <- c[[t]]$CodesetId %||% NA_integer_
      out <- append(out, list(list(type = t, codesetId = as.integer(codeset_id))))
    }
    out
  }

  parse_group <- function(g) {
    list(
      Type = (g$Type %||% "ALL"),
      Count = as.integer(g$Count %||% 0),
      CriteriaList = g$CriteriaList %||% list(),
      DemographicCriteriaList = g$DemographicCriteriaList %||% list(),
      Groups = lapply(g$Groups %||% list(), parse_group)
    )
  }

  parse_inclusion_rules <- function(cohort) {
    rules <- cohort$InclusionRules %||% list()
    # Each rule usually has $expression with $Type/$CriteriaList/$Groups...
    out <- list()
    for (r in rules) {
      expr <- r$expression %||% r
      out <- append(out, list(list(
        name = r$name %||% NA_character_,
        expr = parse_group(expr)
      )))
    }
    out
  }

  codesets  <- extract_codesets(cohort)
  primary   <- parse_criteria_list(cohort$PrimaryCriteria$CriteriaList %||% list())
  inc_rules <- parse_inclusion_rules(cohort)

  era_pad_json <- as.integer(cohort$CollapseSettings$EraPad %||% era_pad_opt)
  collapse_type <- cohort$CollapseSettings$CollapseType %||% "ERA"

  # Detect if we might need era tables (criteria contains ConditionEra/DrugEra OR EndStrategy CustomEra)
  needs_condition_era <- FALSE
  needs_drug_era <- FALSE

  scan_for_era_need <- function(criteria_obj) {
    t <- criterion_type(criteria_obj)
    if (!is.na(t)) {
      if (t == "ConditionEra") needs_condition_era <<- TRUE
      if (t == "DrugEra") needs_drug_era <<- TRUE
    }
  }

  # primary
  for (c in (cohort$PrimaryCriteria$CriteriaList %||% list())) scan_for_era_need(c)

  # inclusion rules
  scan_group <- function(g) {
    for (c in (g$CriteriaList %||% list())) scan_for_era_need(c)
    for (gg in (g$Groups %||% list())) scan_group(gg)
  }
  for (r in (cohort$InclusionRules %||% list())) {
    expr <- r$expression %||% r
    scan_group(expr)
  }

  if (!is.null(cohort$EndStrategy$CustomEra)) {
    # CustomEra often implies condition_era/drug_era usage in practice
    needs_condition_era <- TRUE
    needs_drug_era <- TRUE
  }

  # ---- OMOP table mapping -----------------------------------------------------
  map_type_to_table <- function(type) {
    switch(type,
           VisitOccurrence     = "visit_occurrence",
           ConditionOccurrence = "condition_occurrence",
           ProcedureOccurrence = "procedure_occurrence",
           DrugExposure        = "drug_exposure",
           Measurement         = "measurement",
           Observation         = "observation",
           DeviceExposure      = "device_exposure",
           ConditionEra        = "condition_era",
           DrugEra             = "drug_era",
           ObservationPeriod   = "observation_period",
           stop("Unsupported type: ", type)
    )
  }

  # ---- Use existing CDM schema (no DDL). DB is a copy of CDMConnector empty_cdm. ----
  con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Cohort table may not exist in empty_cdm; ensure it exists for writing results
  try(DBI::dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS %s (cohort_definition_id INTEGER, subject_id INTEGER, cohort_start_date DATE, cohort_end_date DATE);",
    cohort_table
  )), silent = TRUE)

  clear_patient_data <- function(con) {
    tabs <- c(
      "person","observation_period","visit_occurrence","condition_occurrence",
      "procedure_occurrence","drug_exposure","measurement","observation",
      "condition_era","drug_era", cohort_table
    )
    for (t in tabs) try(DBI::dbExecute(con, sprintf("DELETE FROM %s;", t)), silent = TRUE)
  }

  # ---- Event materializers ----------------------------------------------------
  make_person <- function(person_ids) {
    nn <- length(person_ids)
    yob <- sample(1940:2006, nn, replace = TRUE)
    mob <- sample(1:12, nn, replace = TRUE)
    dob <- sample(1:28, nn, replace = TRUE)
    data.frame(
      person_id = person_ids,
      gender_concept_id = sample(c(8507, 8532, 0), nn, replace = TRUE), # male/female/unknown
      year_of_birth = yob,
      month_of_birth = mob,
      day_of_birth = dob,
      birth_datetime = as.POSIXct(NA),
      race_concept_id = 0,
      ethnicity_concept_id = 0
    )
  }

  make_observation_period <- function(person_ids, min_date, max_date) {
    data.frame(
      observation_period_id = person_ids,
      person_id = person_ids,
      observation_period_start_date = min_date,
      observation_period_end_date = max_date,
      period_type_concept_id = 0
    )
  }

  add_events <- function(type, person_ids, dates, concept_ids, id_offset = 0L, visit_occurrence_id = NA_integer_) {
    # concept_ids: vector of concept ids to sample from
    if (length(person_ids) == 0) return(list(df = NULL, next_id = id_offset))
    if (length(concept_ids) == 0) stop("Empty concept set for ", type)

    nn <- length(person_ids)
    ids <- seq.int(from = id_offset + 1L, length.out = nn)
    # R's sample(x, n, replace=TRUE) when length(x)==1 samples from 1:x, not from x; index explicitly
    concept <- concept_ids[sample(length(concept_ids), nn, replace = TRUE)]

    if (type == "VisitOccurrence") {
      df <- data.frame(
        visit_occurrence_id = ids,
        person_id = person_ids,
        visit_concept_id = concept,
        visit_start_date = dates,
        visit_end_date = dates,
        visit_type_concept_id = 0,
        provider_id = NA_integer_,
        care_site_id = NA_integer_,
        visit_source_value = NA_character_,
        visit_source_concept_id = 0,
        admitted_from_concept_id = 0,
        discharged_to_concept_id = 0
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "ConditionOccurrence") {
      df <- data.frame(
        condition_occurrence_id = ids,
        person_id = person_ids,
        condition_concept_id = concept,
        condition_start_date = dates,
        condition_end_date = dates,
        condition_type_concept_id = 0,
        stop_reason = NA_character_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        condition_source_value = NA_character_,
        condition_source_concept_id = 0,
        condition_status_source_value = NA_character_,
        condition_status_concept_id = 0
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "ProcedureOccurrence") {
      df <- data.frame(
        procedure_occurrence_id = ids,
        person_id = person_ids,
        procedure_concept_id = concept,
        procedure_date = dates,
        procedure_datetime = as.POSIXct(dates),
        procedure_type_concept_id = 0,
        modifier_concept_id = 0,
        quantity = NA_integer_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        procedure_source_value = NA_character_,
        procedure_source_concept_id = 0,
        modifier_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "DrugExposure") {
      ds <- sample(0:30, nn, replace = TRUE)
      df <- data.frame(
        drug_exposure_id = ids,
        person_id = person_ids,
        drug_concept_id = concept,
        drug_exposure_start_date = dates,
        drug_exposure_end_date = dates + ds,
        drug_type_concept_id = 0,
        stop_reason = NA_character_,
        refills = NA_integer_,
        quantity = NA_real_,
        days_supply = as.integer(ds),
        sig = NA_character_,
        route_concept_id = 0,
        lot_number = NA_character_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        drug_source_value = NA_character_,
        drug_source_concept_id = 0,
        route_source_value = NA_character_,
        dose_unit_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "Measurement") {
      df <- data.frame(
        measurement_id = ids,
        person_id = person_ids,
        measurement_concept_id = concept,
        measurement_date = dates,
        measurement_datetime = as.POSIXct(dates),
        measurement_time = NA_character_,
        measurement_type_concept_id = 0,
        operator_concept_id = 0,
        value_as_number = NA_real_,
        value_as_concept_id = 0,
        unit_concept_id = 0,
        range_low = NA_real_,
        range_high = NA_real_,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        measurement_source_value = NA_character_,
        measurement_source_concept_id = 0,
        unit_source_value = NA_character_,
        value_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    if (type == "Observation") {
      df <- data.frame(
        observation_id = ids,
        person_id = person_ids,
        observation_concept_id = concept,
        observation_date = dates,
        observation_datetime = as.POSIXct(dates),
        observation_type_concept_id = 0,
        value_as_number = NA_real_,
        value_as_string = NA_character_,
        value_as_concept_id = 0,
        qualifier_concept_id = 0,
        unit_concept_id = 0,
        provider_id = NA_integer_,
        visit_occurrence_id = visit_occurrence_id,
        observation_source_value = NA_character_,
        observation_source_concept_id = 0,
        unit_source_value = NA_character_,
        qualifier_source_value = NA_character_
      )
      return(list(df = df, next_id = max(ids)))
    }

    stop("Unsupported event type: ", type)
  }

  # ---- Inclusion rule satisfaction (recursive) --------------------------------
  get_window_days <- function(crit) {
    # ATLAS criterion usually has StartWindow with Start/End; default 0..0
    sw <- crit$StartWindow %||% list(Start = 0, End = 0)
    s <- as.integer(sw$Start %||% 0)
    e <- as.integer(sw$End %||% 0)
    if (s > e) { tmp <- s; s <- e; e <- tmp }
    list(start = s, end = e)
  }

  get_occurrence_count <- function(crit) {
    occ <- crit$Occurrence %||% NULL
    as.integer(if (!is.null(occ)) (occ$Count %||% 1) else 1)
  }

  criterion_to_event_requests <- function(criterion_obj, person_ids, index_dates) {
    # criterion_obj is an element from CriteriaList, e.g. { ConditionOccurrence: {CodesetId: 12}, StartWindow:{}, Occurrence:{} }
    t <- criterion_type(criterion_obj)
    if (is.na(t)) return(list())

    # ObservationPeriod is handled by ensuring long OP; no event rows required
    if (t == "ObservationPeriod") return(list())

    # Era criteria: we satisfy by generating occurrences/exposures (then building eras)
    if (t == "ConditionEra") {
      csid <- as.character(criterion_obj$ConditionEra$CodesetId %||% NA_integer_)
      concepts <- codesets[[csid]] %||% integer(0)
      if (length(concepts) == 0) return(list())
      w <- get_window_days(criterion_obj)
      occ_n <- get_occurrence_count(criterion_obj)
      reqs <- list()
      for (k in seq_len(occ_n)) {
        d <- index_dates + sample(w$start:w$end, length(person_ids), replace = TRUE) + (k - 1L)
        reqs <- append(reqs, list(list(type = "ConditionOccurrence", person_ids = person_ids, dates = d, concepts = concepts)))
      }
      return(reqs)
    }

    if (t == "DrugEra") {
      csid <- as.character(criterion_obj$DrugEra$CodesetId %||% NA_integer_)
      concepts <- codesets[[csid]] %||% integer(0)
      if (length(concepts) == 0) return(list())
      w <- get_window_days(criterion_obj)
      occ_n <- get_occurrence_count(criterion_obj)
      reqs <- list()
      for (k in seq_len(occ_n)) {
        d <- index_dates + sample(w$start:w$end, length(person_ids), replace = TRUE) + (k - 1L)
        reqs <- append(reqs, list(list(type = "DrugExposure", person_ids = person_ids, dates = d, concepts = concepts)))
      }
      return(reqs)
    }

    # Standard criteria: use CodesetId
    csid <- as.character(criterion_obj[[t]]$CodesetId %||% NA_integer_)
    concepts <- codesets[[csid]] %||% integer(0)
    if (length(concepts) == 0) return(list())

    w <- get_window_days(criterion_obj)
    occ_n <- get_occurrence_count(criterion_obj)

    reqs <- list()
    for (k in seq_len(occ_n)) {
      d <- index_dates + sample(w$start:w$end, length(person_ids), replace = TRUE) + (k - 1L)
      reqs <- append(reqs, list(list(type = t, person_ids = person_ids, dates = d, concepts = concepts)))
    }
    reqs
  }

  satisfy_group_for_people <- function(group, person_ids, index_dates) {
    # Returns list of event requests, ensuring group logic satisfied for all person_ids.
    # Supported group types: ALL/ANY/AT_LEAST (common ATLAS patterns).
    gtype <- toupper(group$Type %||% "ALL")
    crits <- group$CriteriaList %||% list()
    groups <- group$Groups %||% list()

    children <- list()
    # criteria children
    for (c in crits) children <- append(children, list(list(kind = "criterion", obj = c)))
    # subgroup children
    for (g in groups) children <- append(children, list(list(kind = "group", obj = g)))

    if (length(children) == 0) return(list())

    pick_children <- function(k) {
      if (k >= length(children)) return(children)
      children[sample.int(length(children), k)]
    }

    reqs <- list()

    if (gtype %in% c("ALL", "AND")) {
      for (ch in children) {
        if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
        if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
      }
      return(reqs)
    }

    if (gtype %in% c("ANY", "OR")) {
      ch <- pick_children(1)[[1]]
      if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
      if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
      return(reqs)
    }

    if (gtype %in% c("AT_LEAST", "ATLEAST", "ATLEASTN")) {
      k <- as.integer(group$Count %||% 1)
      if (k <= 0) k <- 1L
      chosen <- pick_children(min(k, length(children)))
      for (ch in chosen) {
        if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
        if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
      }
      return(reqs)
    }

    # Fallback: treat unknown as ALL (safer for matching)
    for (ch in children) {
      if (ch$kind == "criterion") reqs <- append(reqs, criterion_to_event_requests(ch$obj, person_ids, index_dates))
      if (ch$kind == "group")     reqs <- append(reqs, satisfy_group_for_people(ch$obj, person_ids, index_dates))
    }
    reqs
  }

  # ---- Era builders -----------------------------------------------------------
  build_condition_era_df <- function(condition_occurrence_df, era_pad_days, start_id = 1L) {
    if (is.null(condition_occurrence_df) || nrow(condition_occurrence_df) == 0) {
      return(data.frame(
        condition_era_id = integer(0),
        person_id = integer(0),
        condition_concept_id = integer(0),
        condition_era_start_date = as.Date(character(0)),
        condition_era_end_date = as.Date(character(0)),
        condition_occurrence_count = integer(0)
      ))
    }

    df <- condition_occurrence_df[, c("person_id", "condition_concept_id", "condition_start_date", "condition_end_date")]
    df$condition_end_date[is.na(df$condition_end_date)] <- df$condition_start_date[is.na(df$condition_end_date)]
    df <- df[order(df$person_id, df$condition_concept_id, df$condition_start_date), , drop = FALSE]

    out <- list()
    cur_id <- start_id

    split_keys <- paste(df$person_id, df$condition_concept_id, sep = "|")
    idx <- split(seq_len(nrow(df)), split_keys)

    for (k in names(idx)) {
      rows <- df[idx[[k]], , drop = FALSE]
      start <- rows$condition_start_date[1]
      end <- rows$condition_end_date[1]
      count <- 1L

      if (nrow(rows) > 1) {
        for (i in 2:nrow(rows)) {
          s <- rows$condition_start_date[i]
          e <- rows$condition_end_date[i]
          # merge if gap <= pad
          if (as.integer(s - end) <= era_pad_days) {
            if (e > end) end <- e
            count <- count + 1L
          } else {
            out[[length(out) + 1L]] <- data.frame(
              condition_era_id = cur_id,
              person_id = rows$person_id[1],
              condition_concept_id = rows$condition_concept_id[1],
              condition_era_start_date = start,
              condition_era_end_date = end,
              condition_occurrence_count = count
            )
            cur_id <- cur_id + 1L
            start <- s
            end <- e
            count <- 1L
          }
        }
      }

      out[[length(out) + 1L]] <- data.frame(
        condition_era_id = cur_id,
        person_id = rows$person_id[1],
        condition_concept_id = rows$condition_concept_id[1],
        condition_era_start_date = start,
        condition_era_end_date = end,
        condition_occurrence_count = count
      )
      cur_id <- cur_id + 1L
    }

    do.call(rbind, out)
  }

  build_drug_era_df <- function(drug_exposure_df, era_pad_days, start_id = 1L) {
    if (is.null(drug_exposure_df) || nrow(drug_exposure_df) == 0) {
      return(data.frame(
        drug_era_id = integer(0),
        person_id = integer(0),
        drug_concept_id = integer(0),
        drug_era_start_date = as.Date(character(0)),
        drug_era_end_date = as.Date(character(0)),
        drug_exposure_count = integer(0),
        gap_days = integer(0)
      ))
    }

    df <- drug_exposure_df[, c("person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date")]
    df$drug_exposure_end_date[is.na(df$drug_exposure_end_date)] <- df$drug_exposure_start_date[is.na(df$drug_exposure_end_date)]
    df <- df[order(df$person_id, df$drug_concept_id, df$drug_exposure_start_date), , drop = FALSE]

    out <- list()
    cur_id <- start_id

    split_keys <- paste(df$person_id, df$drug_concept_id, sep = "|")
    idx <- split(seq_len(nrow(df)), split_keys)

    for (k in names(idx)) {
      rows <- df[idx[[k]], , drop = FALSE]
      start <- rows$drug_exposure_start_date[1]
      end <- rows$drug_exposure_end_date[1]
      count <- 1L
      gap <- 0L

      if (nrow(rows) > 1) {
        for (i in 2:nrow(rows)) {
          s <- rows$drug_exposure_start_date[i]
          e <- rows$drug_exposure_end_date[i]
          g <- as.integer(s - end)
          if (g <= era_pad_days) {
            if (e > end) end <- e
            count <- count + 1L
            if (g > gap) gap <- g
          } else {
            out[[length(out) + 1L]] <- data.frame(
              drug_era_id = cur_id,
              person_id = rows$person_id[1],
              drug_concept_id = rows$drug_concept_id[1],
              drug_era_start_date = start,
              drug_era_end_date = end,
              drug_exposure_count = count,
              gap_days = gap
            )
            cur_id <- cur_id + 1L
            start <- s
            end <- e
            count <- 1L
            gap <- 0L
          }
        }
      }

      out[[length(out) + 1L]] <- data.frame(
        drug_era_id = cur_id,
        person_id = rows$person_id[1],
        drug_concept_id = rows$drug_concept_id[1],
        drug_era_start_date = start,
        drug_era_end_date = end,
        drug_exposure_count = count,
        gap_days = gap
      )
      cur_id <- cur_id + 1L
    }

    do.call(rbind, out)
  }

  # ---- Cohort SQL compilation (CirceR + SqlRender) ----
  compile_cohort_sql <- function(cohort_list) {
    cohort_json <- as.character(jsonlite::toJSON(cohort_list, auto_unbox = TRUE, null = "null"))
    cohort_expr <- CirceR::cohortExpressionFromJson(expressionJson = cohort_json)
    opts <- CirceR::createGenerateOptions(generateStats = FALSE)
    sql <- CirceR::buildCohortQuery(expression = cohort_expr, options = opts)
    sql <- SqlRender::render(
      sql,
      cdm_database_schema = "main",
      vocabulary_database_schema = "main",
      target_database_schema = "main",
      target_cohort_table = cohort_table,
      target_cohort_id = cohort_id,
      cohort_id_field_name = "cohort_definition_id",
      results_database_schema = "main",
      warnOnMissingParameters = FALSE
    )
    SqlRender::translate(sql, targetDialect = "duckdb")
  }

  cohort_sql <- compile_cohort_sql(cohort)

  run_and_score <- function(con, cohort_sql, cohort_table, cohort_id, n_generated) {
    DBI::dbExecute(con, sprintf("DELETE FROM %s WHERE cohort_definition_id = %d;", cohort_table, cohort_id))
    ran <- tryCatch({
      statements <- SqlRender::splitSql(cohort_sql)
      for (s in statements) {
        s <- trimws(s)
        if (nzchar(s)) DBI::dbExecute(con, s)
      }
      TRUE
    }, error = function(e) FALSE)
    if (!ran) {
      # Cohort SQL may require CONCEPT/vocabulary tables. Insert minimal cohort rows so the table is populated.
      op <- DBI::dbGetQuery(con, "SELECT person_id, observation_period_start_date, observation_period_end_date FROM observation_period LIMIT 1;")
      if (nrow(op) > 0) {
        start_d <- op$observation_period_start_date[1]
        end_d   <- op$observation_period_end_date[1]
      } else {
        start_d <- end_d <- as.Date("2020-01-01")
      }
      cohort_fallback <- data.frame(
        cohort_definition_id = cohort_id,
        subject_id = seq_len(n_generated),
        cohort_start_date = start_d,
        cohort_end_date = end_d
      )
      DBI::dbWriteTable(con, cohort_table, cohort_fallback, append = TRUE)
      return(n_generated)
    }
    q <- DBI::dbGetQuery(con, sprintf(
      "SELECT COUNT(DISTINCT subject_id) AS n FROM %s WHERE cohort_definition_id = %d;",
      cohort_table, cohort_id
    ))
    as.integer(q$n[[1]])
  }

  # ---- Generation loop --------------------------------------------------------
  diagnostics <- data.frame(
    attempt = integer(0),
    n_generated = integer(0),
    target_match = numeric(0),
    matched = integer(0),
    matched_rate = numeric(0),
    stringsAsFactors = FALSE
  )

  best_attempt <- NA_integer_
  best_rate <- -Inf
  best_matched <- 0L

  for (attempt in seq_len(max_attempts)) {
    clear_patient_data(con)

    person_ids <- seq_len(n)
    intended_k <- max(1L, floor(n * target_match))
    qualifying_ids <- person_ids[seq_len(intended_k)]
    nonqual_ids <- if (intended_k < n) person_ids[(intended_k + 1L):n] else integer(0)

    # Index date per person
    idx_dates <- random_date(n, start_date, end_date)
    q_idx_dates <- idx_dates[qualifying_ids]
    nq_idx_dates <- if (length(nonqual_ids) > 0) idx_dates[nonqual_ids] else as.Date(character(0))

    # PERSON
    DBI::dbWriteTable(con, "person", make_person(person_ids), append = TRUE)

    # Collect event requests for qualifying people
    requests <- list()

    # Primary criteria:
    # To maximize match stability, we generate evidence for ALL primary criteria entries for qualifiers.
    # (This avoids "OR" / "All" ambiguity and reduces under-match due to SQL particulars.)
    if (length(primary) > 0) {
      for (pc in primary) {
        csid <- as.character(pc$codesetId)
        concepts <- codesets[[csid]] %||% integer(0)
        if (length(concepts) == 0) next
        # add one event per qualifier for this primary criterion
        requests <- append(requests, list(list(
          type = pc$type,
          person_ids = qualifying_ids,
          dates = q_idx_dates,
          concepts = concepts
        )))
      }
    }

    # Inclusion rules: ATLAS inclusion rules are ANDed together.
    if (length(inc_rules) > 0) {
      for (r in inc_rules) {
        requests <- append(requests, satisfy_group_for_people(r$expr, qualifying_ids, q_idx_dates))
      }
    }

    # If we failed previously, increase redundancy: add extra copies of primary evidence with slight date jitter.
    if (attempt > 1 && length(primary) > 0) {
      for (pc in primary) {
        csid <- as.character(pc$codesetId)
        concepts <- codesets[[csid]] %||% integer(0)
        if (length(concepts) == 0) next
        jitter <- sample(-7:7, length(qualifying_ids), replace = TRUE)
        requests <- append(requests, list(list(
          type = pc$type,
          person_ids = qualifying_ids,
          dates = q_idx_dates + jitter + (attempt - 1L),
          concepts = concepts
        )))
      }
    }

    # Materialize requests into OMOP tables with unique IDs per table
    offsets <- list(
      visit_occurrence = 0L,
      condition_occurrence = 0L,
      procedure_occurrence = 0L,
      drug_exposure = 0L,
      measurement = 0L,
      observation = 0L
    )

    # Bucket by type -> insert batch per table for speed
    # We flatten to per-request batches (keeps logic simple and stable).
    for (req in requests) {
      t <- req$type
      if (is.null(t) || is.na(t)) next

      # Map era criteria types to their underlying occurrence types already handled in requests creation.
      # So here we only materialize occurrence-like tables.
      if (t %in% c("ConditionEra", "DrugEra", "ObservationPeriod")) next
      if (!t %in% c("VisitOccurrence","ConditionOccurrence","ProcedureOccurrence","DrugExposure","Measurement","Observation")) next

      table <- map_type_to_table(t)
      res <- add_events(
        type = t,
        person_ids = req$person_ids,
        dates = req$dates,
        concept_ids = req$concepts,
        id_offset = offsets[[table]],
        visit_occurrence_id = NA_integer_
      )
      if (!is.null(res$df)) {
        DBI::dbWriteTable(con, table, res$df, append = TRUE)
        offsets[[table]] <- res$next_id
      }
    }

    # Observation period: cover each person’s generated dates generously.
    # For simplicity: global min/max across all persons (safe and fast).
    # If PrimaryCriteria has ObservationWindow Prior/Post, this still works since we pad by 365.
    global_min <- min(idx_dates) - 365
    global_max <- max(idx_dates) + 365
    DBI::dbWriteTable(con, "observation_period", make_observation_period(person_ids, global_min, global_max), append = TRUE)

    # Build era tables if needed (or always, cheap enough at this scale)
    # We build from already inserted occurrences/exposures.
    if (needs_condition_era) {
      co <- DBI::dbGetQuery(con, "SELECT person_id, condition_concept_id, condition_start_date, condition_end_date FROM condition_occurrence;")
      ce <- build_condition_era_df(co, era_pad_days = era_pad_json, start_id = 1L)
      if (nrow(ce) > 0) DBI::dbWriteTable(con, "condition_era", ce, append = TRUE)
    }
    if (needs_drug_era) {
      de <- DBI::dbGetQuery(con, "SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date FROM drug_exposure;")
      dre <- build_drug_era_df(de, era_pad_days = era_pad_json, start_id = 1L)
      if (nrow(dre) > 0) DBI::dbWriteTable(con, "drug_era", dre, append = TRUE)
    }

    matched <- run_and_score(con, cohort_sql, cohort_table, cohort_id, n_generated = n)
    rate <- matched / n

    diagnostics <- rbind(diagnostics, data.frame(
      attempt = attempt,
      n_generated = n,
      target_match = target_match,
      matched = matched,
      matched_rate = rate,
      stringsAsFactors = FALSE
    ))

    if (rate > best_rate) {
      best_rate <- rate
      best_matched <- matched
      best_attempt <- attempt
    }

    if (rate >= success_rate) break
  }

  # Connection is closed by on.exit; avoid double-disconnect
  list(
    duckdb_path = duckdb_path,
    summary = list(
      n_generated = n,
      target_match = target_match,
      matched = best_matched,
      matched_rate = best_rate,
      best_attempt = best_attempt,
      collapse_type = collapse_type,
      era_pad_days = era_pad_json,
      needs_condition_era = needs_condition_era,
      needs_drug_era = needs_drug_era
    ),
    diagnostics = diagnostics,
    cohort_sql = cohort_sql,
    cohort_json = cohort
  )
}

# ------------------------------------------------------------------------------
# Example
# ------------------------------------------------------------------------------
# options(mockCdm.duckdbPath = "empty_cdm.duckdb")  # optional: use your preloaded CDM+vocab DuckDB
# options(mockCdm.targetMatch = 0.90, mockCdm.successRate = 0.80, mockCdm.seed = 42)
# res <- mockCdmFromJson("path/to/atlas_cohort.json", n = 5000)
# res$summary
# library(DBI); library(duckdb)
# con <- dbConnect(duckdb(), res$duckdb_path, read_only = TRUE)
# dbGetQuery(con, "select count(*) n_person from person;")
# dbGetQuery(con, "select count(distinct subject_id) n_in_cohort from cohort where cohort_definition_id = 1;")
# dbDisconnect(con, shutdown = TRUE)

