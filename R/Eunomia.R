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

#' Download Eunomia data files
#'
#' Download the Eunomia data files from https://github.com/darwin-eu/EunomiaDatasets
#'
#' @param datasetName The data set name as found on https://github.com/darwin-eu/EunomiaDatasets. The
#'  data set name corresponds to the folder with the data set ZIP files
#' @param cdmVersion The OMOP CDM version. This version will appear in the suffix of the data file,
#'  for example: synpuf_5.3.zip. Must be '5.3' (default) or '5.4'.
#' @param pathToData    The path where the Eunomia data is stored on the file system., By default the
#'  value of the environment variable "EUNOMIA_DATA_FOLDER" is used.
#' @param overwrite Control whether the existing archive file will be overwritten should it already exist.
#' @return
#' Invisibly returns the destination if the download was successful.
#'
#' @importFrom utils download.file
#'
#' @examples
#' \dontrun{
#' downloadEunomiaData("GiBleed")
#' }
#' @export
downloadEunomiaData <- function(datasetName = "GiBleed",
                                cdmVersion = "5.3",
                                pathToData = Sys.getenv("EUNOMIA_DATA_FOLDER"),
                                overwrite = FALSE) {

  checkmate::assertChoice(datasetName, choices = exampleDatasets())
  checkmate::assertChoice(cdmVersion, c("5.3", "5.4"))

  if (is.null(pathToData) || is.na(pathToData) || pathToData == "") {
    stop("The pathToData argument must be specified. Consider setting the EUNOMIA_DATA_FOLDER environment variable, for example in the .Renviron file.")
  }

  if (!dir.exists(pathToData)) { dir.create(pathToData, recursive = TRUE) }

  if (datasetName == "Synthea27NjParquet" && cdmVersion == "5.3") {
    cli::cli_abort("Synthea27NjParquet is only available in CDM version 5.4")
  } else if (!(datasetName %in% c("synpuf-1k", "Synthea27NjParquet")) && cdmVersion == "5.4") {
    cli::cli_abort("{datasetName} is only available in CDM version 5.3")
  }

  zipName <- glue::glue("{datasetName}_{cdmVersion}.zip")

  if (file.exists(file.path(pathToData, zipName)) && !overwrite) {
    rlang::inform(glue::glue(
      "Dataset already exists ({file.path(pathToData, zipName)})
      Specify `overwrite = TRUE` to overwrite existing zip archive"
    ))
    return(invisible(pathToData))
  }

  pb <- cli::cli_progress_bar(format = "[:bar] :percent :elapsed",
                              type = "download")

  # synpuf is the only dataset we have in both 5.3 and 5.4 at the moment.
  if (datasetName == "Synthea27NjParquet") {
    url <- "https://github.com/OHDSI/EunomiaDatasets/raw/main/datasets/Synthea27NjParquet/Synthea27NjParquet_5.4.zip"
  } else {
    url <- glue::glue("https://cdmconnectordata.blob.core.windows.net/cdmconnector-example-data/{datasetName}_{cdmVersion}.zip")
  }

  withr::with_options(list(timeout = 5000), {
    utils::download.file(
      url = url,
      destfile = file.path(pathToData, zipName),
      mode = "wb",
      method = "auto",
      quiet = FALSE,
      extra = list(progressfunction = function(downloaded, total) {
        # Calculate the progress and update the progress bar
        progress <- min(1, downloaded / total)
        cli::cli_progress_update(id = pb, set = progress)
      })
    )
  })

  cli::cli_progress_done(id = pb)
  cat("\nDownload completed!\n")
  return(invisible(pathToData))
}

#' List the available example CDM datasets
#'
#' @return A character vector with example CDM dataset identifiers
#' @export
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' exampleDatasets()[1]
#' #> [1] "GiBleed"
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("GiBleed"))
#' cdm <- cdmFromCon(con)
#' }
exampleDatasets <- function() {
  c("GiBleed",
    "synthea-allergies-10k",
    "synthea-anemia-10k",
    "synthea-breast_cancer-10k",
    "synthea-contraceptives-10k",
    "synthea-covid19-10k",
    "synthea-covid19-200k",
    "synthea-dermatitis-10k",
    "synthea-heart-10k",
    "synthea-hiv-10k",
    "synthea-lung_cancer-10k",
    "synthea-medications-10k",
    "synthea-metabolic_syndrome-10k",
    "synthea-opioid_addiction-10k",
    "synthea-rheumatoid_arthritis-10k",
    "synthea-snf-10k",
    "synthea-surgery-10k",
    "synthea-total_joint_replacement-10k",
    "synthea-veteran_prostate_cancer-10k",
    "synthea-veterans-10k",
    "synthea-weight_loss-10k",
    "synpuf-1k",
    "synpuf-110k",
    "empty_cdm",
    "Synthea27NjParquet")
}

#' Create a copy of an example OMOP CDM dataset
#'
#' @description
#' Eunomia is an OHDSI project that provides several example OMOP CDM datasets for testing and development.
#' This function creates a copy of a Eunomia database in \href{https://duckdb.org/}{duckdb} and returns
#' the path to the new database file. If the dataset does not yet exist on the user's computer it
#' will attempt to download the source data to the the path defined by the EUNOMIA_DATA_FOLDER environment variable.
#'
#' @details
#' Most of the Eunomia datasets available in CDMConnector are from the Synthea project. Synthea is an open-source
#' synthetic patient generator that models the medical history of synthetic patients. The Synthea datasets are
#' generated using the Synthea tool and then converted to the OMOP CDM format using the OHDSI
#' ETL-Synthea project \url{https://ohdsi.github.io/ETL-Synthea/}. Currently the synthea datasets
#' are only available in the OMOP CDM v5.3 format. See \url{https://synthetichealth.github.io/synthea/}
#' for details on the Synthea project.
#'
#' In addition to Synthea, the Eunomia project provides the CMS Synthetic Public Use Files (SynPUFs) in both
#' 5.3 and 5.4 OMOP CDM formats. This data is synthetic US Medicare claims data mapped to OMOP CDM format.
#' The OMOP CDM has a set of optional metadata tables, called Achilles tables, that
#' include pre-computed analytics about the entire dataset such as record and person counts.
#' The Eunomia Synpuf datasets include the Achilles tables.
#'
#' Eunomia also provides empty cdms that can be used as a starting point for creating a new example CDM.
#' This is useful for creating test data for studies or analytic packages.
#' The empty CDM includes the vocabulary tables and all OMOP CDM tables but
#' the clinical tables are empty and need to be populated with data. For additional information on
#' creating small test CDM datasets see \url{https://ohdsi.github.io/omock/} and
#' \url{https://darwin-eu.github.io/TestGenerator/}.
#'
#' To contribute synthetic observational health data to the Eunomia project please
#' open an issue at \url{https://github.com/OHDSI/Eunomia/issues/}
#'
#' Setup: To use the `eunomiaDir` function please set the `EUNOMIA_DATA_FOLDER` in your .Renviron file
#' to a folder on your computer where the datasets will be downloaded to. This file can
#' be opened by calling `usethis::edit_r_environ()`.
#'
#' @param datasetName One of "GiBleed" (default),
#' "synthea-allergies-10k",
#' "synthea-anemia-10k",
#' "synthea-breast_cancer-10k",
#' "synthea-contraceptives-10k",
#' "synthea-covid19-10k",
#' "synthea-covid19-200k",
#' "synthea-dermatitis-10k",
#' "synthea-heart-10k",
#' "synthea-hiv-10k",
#' "synthea-lung_cancer-10k",
#' "synthea-medications-10k",
#' "synthea-metabolic_syndrome-10k",
#' "synthea-opioid_addiction-10k",
#' "synthea-rheumatoid_arthritis-10k",
#' "synthea-snf-10k",
#' "synthea-surgery-10k",
#' "synthea-total_joint_replacement-10k",
#' "synthea-veteran_prostate_cancer-10k",
#' "synthea-veterans-10k",
#' "synthea-weight_loss-10k",
#' "empty_cdm",
#' "synpuf-1k"
#'
#' @param cdmVersion The OMOP CDM version. Must be "5.3" or "5.4".
#' @param databaseFile The full path to the new copy of the example CDM dataset.
#'
#' @return The file path to the new Eunomia dataset copy
#' @examples
#' \dontrun{
#'
#'  # The defaults GiBleed dataset is a small dataset that is useful for testing
#'  library(CDMConnector)
#'  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
#'  cdm <- cdmFromCon(con, "main", "main")
#'  cdmDisconnect(cdm)
#'
#'  # Synpuf datasets include the Achilles tables
#'  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("synpuf-1k", "5.3"))
#'  cdm <- cdmFromCon(con, "main", "main", achillesSchema = "main")
#'  cdmDisconnect(cdm)
#'
#'  # Currently the only 5.4 dataset is synpuf-1k
#'  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("synpuf-1k", "5.4"))
#'  cdm <- cdmFromCon(con, "main", "main", achillesSchema = "main")
#'  cdmDisconnect(cdm)
#'
#' }
#' @export
eunomiaDir <- function(datasetName = "GiBleed",
                       cdmVersion = "5.3",
                       databaseFile = tempfile(fileext = ".duckdb")) {

  if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") {
    rlang::abort("Set the `EUNOMIA_DATA_FOLDER` environment variable in your .Renviron file.")
  }
  checkmate::assertChoice(cdmVersion, c("5.3", "5.4"))
  rlang::check_installed("duckdb")

  checkmate::assertChoice(datasetName, choices = exampleDatasets())

  # duckdb database are tied to a specific version of duckdb until it reaches v1.0
  duckdbVersion <- substr(utils::packageVersion("duckdb"), 1, 3)

  datasetLocation <- file.path(Sys.getenv("EUNOMIA_DATA_FOLDER"),
                               glue::glue("{datasetName}_{cdmVersion}_{duckdbVersion}.duckdb"))
  datasetAvailable <- file.exists(datasetLocation)

  archiveLocation <- file.path(Sys.getenv("EUNOMIA_DATA_FOLDER"),
                               glue::glue("{datasetName}_{cdmVersion}.zip"))
  archiveAvailable <- file.exists(archiveLocation)

  if (!datasetAvailable && !archiveAvailable) {
    rlang::inform(paste("Downloading", datasetName))
    downloadEunomiaData(datasetName = datasetName, cdmVersion = cdmVersion)
    archiveAvailable <- file.exists(archiveLocation)
    if (isFALSE(archiveAvailable)) rlang::abort("Dataset download failed!")
  }

  if (!datasetAvailable && archiveAvailable) {
    rlang::inform(paste("Creating CDM database", archiveLocation))
    tempFileLocation <- tempfile()
    utils::unzip(zipfile = archiveLocation, exdir = tempFileLocation)

    unzipLocation <- file.path(tempFileLocation, glue::glue("{datasetName}"))
    dataFiles <- sort(list.files(path = unzipLocation, pattern = "*.parquet"))

    if (isFALSE(length(dataFiles) > 0)) {
      rlang::abort(glue::glue("Data file does not contain any .parquet files to load into the database!\nTry removing the file {archiveLocation}."))
    }

    con <- DBI::dbConnect(duckdb::duckdb(datasetLocation))
    # If the function is successful dbDisconnect will be called twice generating a warning.
    # If this function is unsuccessful, still close connection on exit.
    on.exit(suppressWarnings(DBI::dbDisconnect(con, shutdown = TRUE)), add = TRUE)
    on.exit(unlink(tempFileLocation), add = TRUE)

    specs <- omopgenerics::omopTableFields(cdmVersion) %>%
      dplyr::filter(.data$type %in% c("cdm_table", "achilles")) %>%
      dplyr::mutate(cdm_datatype = dplyr::if_else(.data$cdm_datatype == "varchar(max)", "varchar(2000)", .data$cdm_datatype)) %>%
      dplyr::mutate(cdm_field_name = dplyr::if_else(.data$cdm_field_name == '"offset"', "offset", .data$cdm_field_name)) %>%
      tidyr::nest(col = -"cdm_table_name") %>%
      dplyr::mutate(col = purrr::map(col, ~setNames(as.character(.$cdm_datatype), .$cdm_field_name)))

    files <- tools::file_path_sans_ext(basename(list.files(unzipLocation)))
    tables <- specs$cdm_table_name

    for (i in cli::cli_progress_along(tables)) {
      if (isFALSE(tables[i] %in% files)) next

      fields <- specs %>%
        dplyr::filter(.data$cdm_table_name == specs$cdm_table_name[i]) %>%
        dplyr::pull(.data$col) %>%
        unlist()

      DBI::dbCreateTable(con,
                         .inSchema("main", specs$cdm_table_name[i], dbms = dbms(con)),
                         fields = fields)

      cols <- paste(glue::glue('"{names(fields)}"'), collapse = ", ")

      table_path <- file.path(unzipLocation, paste0(specs$cdm_table_name[i], ".parquet"))
      sql <- glue::glue("INSERT INTO main.{tables[i]}({cols})
                         SELECT {cols} FROM '{table_path}'")
      DBI::dbExecute(con, sql)
    }

    DBI::dbDisconnect(con, shutdown = TRUE)
  }

  rc <- file.copy(from = datasetLocation, to = databaseFile)
  if (isFALSE(rc)) {
    rlang::abort(glue::glue("File copy from {datasetLocation} to {databaseFile} failed!"))
  }
  return(databaseFile)
}

#' Has the Eunomia dataset been cached?
#'
#' @param datasetName Name of the Eunomia dataset to check. Defaults to "GiBleed".
#' @param cdmVersion Version of the Eunomia dataset to check. Must be "5.3" or "5.4".
#'
#' @return TRUE if the eunomia example dataset is available and FALSE otherwise
#' @export
eunomiaIsAvailable <- function(datasetName = "GiBleed",
                                 cdmVersion = "5.3") {
  if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") {
    rlang::abort("Set the environment variable EUNOMIA_DATA_FOLDER to the eunomia cache location")
  }

  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% c("5.3", "5.4"))

  # check for zip archive of csv source files
  archiveName <- paste0(datasetName, "_", cdmVersion, ".zip")
  archiveLocation <- file.path(Sys.getenv("EUNOMIA_DATA_FOLDER"), archiveName)
  return(file.exists(archiveLocation))
}


#' Require eunomia to be available. The function makes sure that you can later
#' create a eunomia database with `eunomiaDir()`.
#'
#' @param datasetName Name of the Eunomia dataset to check. Defaults to "GiBleed".
#' @param cdmVersion Version of the Eunomia dataset to check. Must be "5.3" or "5.4".
#'
#' @return Path to eunomia database.
#' @export
#'
requireEunomia <- function(datasetName = "GiBleed",
                           cdmVersion = "5.3") {
  # input check
  omopgenerics::assertChoice(datasetName, exampleDatasets(), length = 1)
  omopgenerics::assertChoice(cdmVersion, c("5.3", "5.4"), length = 1)

  # set EUNOMIA_DATA_FOLDER if not defined before
  if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") {
    Sys.setenv("EUNOMIA_DATA_FOLDER" = tempdir())
    cli::cli_inform(c("i" = "`EUNOMIA_DATA_FOLDER` set to: {.path {Sys.getenv('EUNOMIA_DATA_FOLDER')}}."))
  }

  # create dir if it does not exist
  if (!dir.exists(Sys.getenv("EUNOMIA_DATA_FOLDER"))) {
    dir.create(Sys.getenv("EUNOMIA_DATA_FOLDER"), recursive = TRUE)
    cli::cli_inform(c("i" = "folder {.path {Sys.getenv('EUNOMIA_DATA_FOLDER')}} created."))
  }

  # download eunomia if not downloaded before
  if (!eunomiaIsAvailable(datasetName = datasetName, cdmVersion = cdmVersion)) {
    downloadEunomiaData(
      datasetName = datasetName,
      cdmVersion = cdmVersion,
      overwrite = TRUE
    )
  }

  return(invisible(TRUE))
}
