test_that("downloadEunomiaData", {
  path <- downloadEunomiaData()
  expect_true(is.null(path))
  expect_error(downloadEunomiaData(datasetName = NULL))
  expect_error(downloadEunomiaData(pathToData = NULL))
})
