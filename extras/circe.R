#
# path <- system.file("cohorts3", "GiBleed.json", package = "CDMConnector")
# path <- system.file("cohorts2", "cerebralVenousSinusThrombosis01.json", package = "CDMConnector")
#
# x <- jsonlite::read_json(path)
# str(x, max.level = 2)
#
#
# # upload a temp table with codeset_id, concept_id, include_descendants, exclude
# library(CDMConnector)
# con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
# cdm <- cdm_from_con(con, "main", "main")
#
# codesets <- dplyr::tibble(
#   codeset_id = purrr::map_int(x$ConceptSets, "id"),
#   concept_id = purrr::map(x$ConceptSets, ~purrr::map_int(.$expression$items, ~.$concept$CONCEPT_ID)),
#   exclude = purrr::map(x$ConceptSets, ~purrr::map_lgl(.$expression$items, ~dplyr::coalesce(.$isExcluded, FALSE))),
#   descendants = purrr::map(x$ConceptSets, ~purrr::map_lgl(.$expression$items, ~dplyr::coalesce(.$includeDescendants, FALSE)))) %>%
#   tidyr::unnest(cols = -"codeset_id")
#
# codesets
#
# # DBI::dbWriteTable(con, inSchema(write_schema, "codesets", dbms(con)), codesets, overwrite = TRUE)
# # DBI::dbWriteTable(con, "codesets", codesets, overwrite = TRUE, temporary = TRUE)
# df <- dplyr::copy_to(con, codesets, overwrite = T, temporary = T)
#
# codesets_db <- df %>%
#   dplyr::filter(descendants) %>%
#   dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
#   dplyr::select("codeset_id", concept_id = "descendant_concept_id", "exclude") %>%
#   dplyr::union(dplyr::select(df, "codeset_id", "concept_id", "exclude")) %>%
#   dplyr::filter(!exclude) %>%
#   dplyr::select("codeset_id", "concept_id") %>%
#   computeQuery()
#
# codesets_db
#
#
# str(x$PrimaryCriteria, max.level = 2)
# str(x$PrimaryCriteria$CriteriaList)
# j = x$PrimaryCriteria$CriteriaList
#
#
#
#
#
# # SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
# # C.visit_occurrence_id, C.condition_start_date as sort_date
# # FROM
# # (
# #   SELECT co.*
# #     FROM @cdm_database_schema.CONDITION_OCCURRENCE co
# #   JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
# # ) C
#
#
# x$ConceptSets[[1]]
#
