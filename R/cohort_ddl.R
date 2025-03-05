# Copyright 2025 DARWIN EU®
#
# This file is part of CDMConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

createCohortTables <- function(con, writeSchema, name, computeAttrition) {

  checkmate::assertCharacter(writeSchema, min.len = 1, max.len = 3, any.missing = FALSE)
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE)
  checkmate::assertTRUE(DBI::dbIsValid(con))
  checkmate::assertLogical(computeAttrition, len = 1, any.missing = FALSE)
  if(name != tolower(name)) {
    rlang::abort("cohort table name must be lowercase!")
  }

  # oracle and snowflake use uppercase table names by default which causes
  # issues when switching between ohdsi-sql (unquoted identifiers) and dbplyr sql (quoted identifiers)
  # dbAppendTable does not work using bigrquery https://github.com/r-dbi/bigrquery/issues/539

  # update v1.3 (Jan 2024) - oracle and bigquery not supported. Quote tables/columns on snowflake.
  existingTables <- listTables(con, writeSchema)

  if (name %in% existingTables) {
    DBI::dbRemoveTable(con, .inSchema(writeSchema, name, dbms(con)))
  }

  .dbCreateTable(con,
                     name = .inSchema(writeSchema, name, dbms(con)),
                     fields = c(
                       cohort_definition_id = "INT",
                       subject_id = "BIGINT",
                       cohort_start_date = "DATE",
                       cohort_end_date = "DATE"))

  stopifnot(name %in% listTables(con, writeSchema))

  if (computeAttrition) {

    nm <- paste0(name, "_inclusion")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, .inSchema(writeSchema, nm, dbms(con)))
    }

    .dbCreateTable(con,
                       name = .inSchema(writeSchema, nm, dbms(con)),
                       fields = c(
                         cohort_definition_id = "INT",
                         rule_sequence = "INT",
                         name = ifelse(dbms(con) == "bigquery", "STRING", "VARCHAR(255)"),
                         description = ifelse(dbms(con) == "bigquery", "STRING", "VARCHAR(1000)"))
    )

    nm <- paste0(name, "_inclusion_result") # used for attrition

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, .inSchema(writeSchema, nm, dbms(con)))
    }

    .dbCreateTable(con,
                       name = .inSchema(writeSchema, nm, dbms(con)),
                       fields = c(
                         cohort_definition_id = "INT",
                         inclusion_rule_mask = "INT",
                         person_count = "INT",
                         mode_id = "INT")
    )

    nm <- paste0(name, "_inclusion_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, .inSchema(writeSchema, nm, dbms(con)))
    }

    .dbCreateTable(con,
                       name = .inSchema(writeSchema, nm, dbms(con)),
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
      DBI::dbRemoveTable(con, .inSchema(writeSchema, nm, dbms(con)))
    }

    .dbCreateTable(con,
                       name = .inSchema(writeSchema, nm, dbms(con)),
                       fields = c(
                         cohort_definition_id = "INT",
                         base_count = "INT",
                         final_count = "INT",
                         mode_id = "INT")
    )

    nm <- paste0(name, "_censor_stats")

    if (nm %in% existingTables) {
      DBI::dbRemoveTable(con, .inSchema(writeSchema, nm, dbms(con)))
    }

    .dbCreateTable(con,
                       name = .inSchema(writeSchema, nm, dbms(con)),
                       fields = c(
                         cohort_definition_id = "INT",
                         lost_count = "INT")
    )
  }
}
