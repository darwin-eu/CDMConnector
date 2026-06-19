test_that("cdmTrimVocabulary trims vocabulary tables", {
  skip_if_not_installed("duckdb")

  con <- local_eunomia_con()
  cdm <- cdmFromCon(con, "main", "main", cdmName = "test")

  # count rows before trimming
  concept_before <- cdm$concept |> dplyr::tally() |> dplyr::pull("n")
  ancestor_before <- cdm$concept_ancestor |> dplyr::tally() |> dplyr::pull("n")

  cdm_trimmed <- cdmTrimVocabulary(cdm)

  # concept table should be smaller

  concept_after <- cdm_trimmed$concept |> dplyr::collect() |> nrow()
  expect_true(concept_after < concept_before)
  expect_true(concept_after > 0)

  # concept_ancestor should be smaller
  ancestor_after <- cdm_trimmed$concept_ancestor |> dplyr::collect() |> nrow()
  expect_true(ancestor_after <= ancestor_before)

  # concept_synonym should be empty
  synonym_count <- cdm_trimmed$concept_synonym |> dplyr::collect() |> nrow()
  expect_equal(synonym_count, 0L)

  # graph completeness: all concept_ids in concept_relationship exist in concept
  if ("concept_relationship" %in% names(cdm_trimmed)) {
    concept_ids <- cdm_trimmed$concept |>
      dplyr::select("concept_id") |>
      dplyr::collect() |>
      dplyr::pull("concept_id")

    cr <- cdm_trimmed$concept_relationship |> dplyr::collect()
    if (nrow(cr) > 0) {
      expect_true(all(cr$concept_id_1 %in% concept_ids))
      expect_true(all(cr$concept_id_2 %in% concept_ids))
    }
  }

  # all concept_ids in concept_ancestor exist in concept
  ca <- cdm_trimmed$concept_ancestor |> dplyr::collect()
  if (nrow(ca) > 0) {
    expect_true(all(ca$ancestor_concept_id %in% concept_ids))
    expect_true(all(ca$descendant_concept_id %in% concept_ids))
  }
})
