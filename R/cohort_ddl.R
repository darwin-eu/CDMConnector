
createCohortTables <- function(con, writeSchema, name, computeAttrition) {

  if (dbms(con) != "oracle") {
    existingTables <- list_tables(con, writeSchema)

    if (name %in% existingTables) {
      DBI::dbRemoveTable(con, inSchema(writeSchema, name))
    }

    DBI::dbCreateTable(con,
                       name = inSchema(writeSchema, name),
                       fields = c(
                         cohort_definition_id = "INT",
                         # subject_id = "BIGINT",
                         subject_id = "INT",
                         cohort_start_date = "DATE",
                         cohort_end_date = "DATE"))

    stopifnot(name %in% listTables(con, writeSchema))

    if (computeAttrition) {

      nm <- paste0(name, "_inclusion")

      if (nm %in% existingTables) {
        DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
      }

      DBI::dbCreateTable(con,
                         name = inSchema(writeSchema, nm),
                         fields = c(
                           cohort_definition_id = "INT",
                           rule_sequence = "INT",
                           name = "VARCHAR(255)",
                           description = "VARCHAR(1000)")
      )

      nm <- paste0(name, "_inclusion_result") # used for attrition

      if (nm %in% existingTables) {
        DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
      }

      DBI::dbCreateTable(con,
                         name = inSchema(writeSchema, nm),
                         fields = c(
                           cohort_definition_id = "INT",
                           inclusion_rule_mask = "INT",
                           person_count = "INT",
                           mode_id = "INT")
      )

      nm <- paste0(name, "_inclusion_stats")

      if (nm %in% existingTables) {
        DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
      }

      DBI::dbCreateTable(con,
                         name = inSchema(writeSchema, nm),
                         fields = c(
                           cohort_definition_id = "INT",
                           rule_sequence = "INT",
                           person_count = "INT",
                           gain_count = "INT",
                           person_total = "INT",
                           mode_id = "INT")
      )


      nm <- paste0(name, "_summary_stats")

      if (nm %in% existingTables) {
        DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
      }

      DBI::dbCreateTable(con,
                         name = inSchema(writeSchema, nm),
                         fields = c(
                           cohort_definition_id = "INT",
                           base_count = "INT",
                           final_count = "INT",
                           mode_id = "INT")
      )

      nm <- paste0(name, "_censor_stats")

      if (nm %in% existingTables) {
        DBI::dbRemoveTable(con, inSchema(writeSchema, nm))
      }

      DBI::dbCreateTable(con,
                         name = inSchema(writeSchema, nm),
                         fields = c(
                           cohort_definition_id = "INT",
                           lost_count = "INT")
      )
    }
  } else {
    sql <- c(
    "IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
    DROP TABLE @cohort_database_schema.@cohort_table;

    CREATE TABLE @cohort_database_schema.@cohort_table (
      cohort_definition_id BIGINT,
      subject_id BIGINT,
      cohort_start_date DATE,
      cohort_end_date DATE
    );

    {@computeAttrition}?{
    IF OBJECT_ID('@cohort_database_schema.@cohort_inclusion_table', 'U') IS NOT NULL
    DROP TABLE @cohort_database_schema.@cohort_inclusion_table;

    CREATE TABLE @cohort_database_schema.@cohort_inclusion_table (
      cohort_definition_id BIGINT NOT NULL,
      rule_sequence INT NOT NULL,
      name VARCHAR(255) NULL,
      description VARCHAR(1000) NULL
    );

    IF OBJECT_ID('@cohort_database_schema.@cohort_inclusion_result_table', 'U') IS NOT NULL
    DROP TABLE @cohort_database_schema.@cohort_inclusion_result_table;

    CREATE TABLE @cohort_database_schema.@cohort_inclusion_result_table (
      cohort_definition_id BIGINT NOT NULL,
      inclusion_rule_mask BIGINT NOT NULL,
      person_count BIGINT NOT NULL,
      mode_id INT
    );

    IF OBJECT_ID('@cohort_database_schema.@cohort_inclusion_stats_table', 'U') IS NOT NULL
    DROP TABLE @cohort_database_schema.@cohort_inclusion_stats_table;

    CREATE TABLE @cohort_database_schema.@cohort_inclusion_stats_table (
      cohort_definition_id BIGINT NOT NULL,
      rule_sequence INT NOT NULL,
      person_count BIGINT NOT NULL,
      gain_count BIGINT NOT NULL,
      person_total BIGINT NOT NULL,
      mode_id INT
    );

    IF OBJECT_ID('@cohort_database_schema.@cohort_summary_stats_table', 'U') IS NOT NULL
    DROP TABLE @cohort_database_schema.@cohort_summary_stats_table;

    CREATE TABLE @cohort_database_schema.@cohort_summary_stats_table (
      cohort_definition_id BIGINT NOT NULL,
      base_count BIGINT NOT NULL,
      final_count BIGINT NOT NULL,
      mode_id INT
    );

    IF OBJECT_ID('@cohort_database_schema.@cohort_censor_stats_table', 'U') IS NOT NULL
    DROP TABLE @cohort_database_schema.@cohort_censor_stats_table;

    CREATE TABLE @cohort_database_schema.@cohort_censor_stats_table(
      cohort_definition_id int NOT NULL,
      lost_count BIGINT NOT NULL
    );}:{}
    ")

    if ("prefix" %in% names(writeSchema)) {
      prefix <- writeSchema["prefix"]
      cohort_database_schema <- paste0(writeSchema[-which(names(writeSchema) == "prefix")], collapse = ".")
    } else {
      prefix <- ""
      cohort_database_schema <- paste0(writeSchema, collapse = ".")
    }

    sql <- SqlRender::render(sql = sql,
                             cohort_database_schema = toupper(cohort_database_schema),
                             cohort_table = toupper(paste0(prefix, name)),
                             computeAttrition = computeAttrition,
                             cohort_inclusion_table        = toupper(paste0(prefix, name, "_inclusion")),
                             cohort_inclusion_result_table = toupper(paste0(prefix, name, "_inclusion_result")),
                             cohort_inclusion_stats_table  = toupper(paste0(prefix, name, "_inclusion_stats")),
                             cohort_summary_stats_table    = toupper(paste0(prefix, name, "_summary_stats")),
                             cohort_censor_stats_table     = toupper(paste0(prefix, name, "_censor_stats")),
                             warnOnMissingParameters = TRUE)
    sql <- SqlRender::translate(sql, "oracle", tempEmulationSchema = writeSchema)
    sql <- SqlRender::splitSql(sql)
    purrr::walk(sql, ~DBI::dbExecute(con, ., immediate = TRUE))
  }
}


