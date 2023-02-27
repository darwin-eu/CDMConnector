test_that("download_optional_data", {
  path <- download_optional_data()
  expect_true(is.null(path))
})

test_that("downloadEunomiaData", {
  expect_error(downloadEunomiaData(datasetName = NULL))
  expect_error(downloadEunomiaData(pathToData = NULL))
})
