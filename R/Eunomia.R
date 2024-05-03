# Copyright 2024 DARWIN EU®
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
#' @param dataset_name,datasetName The data set name as found on https://github.com/darwin-eu/EunomiaDatasets. The
#'  data set name corresponds to the folder with the data set ZIP files
#' @param cdm_version,cdmVersion The OMOP CDM version. This version will appear in the suffix of the data file,
#'  for example: synpuf_5.3.zip. Must be '5.3' (default) or '5.4'.
#' @param path_to_data,pathToData    The path where the Eunomia data is stored on the file system., By default the
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

  withr::with_options(list(timeout = 5000), {
    utils::download.file(
      url = glue::glue("https://example-data.ohdsi.dev/{datasetName}.zip"),
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
#' cdm <- cdm_from_con(con)
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
    "empty_cdm")
}

#' @export
#' @rdname exampleDatasets
example_datasets <- exampleDatasets


#' @rdname downloadEunomiaData
#' @export
download_eunomia_data <- function(dataset_name = "GiBleed",
                                  cdm_version = "5.3",
                                  path_to_data = Sys.getenv("EUNOMIA_DATA_FOLDER"),
                                  overwrite = FALSE) {
  downloadEunomiaData(datasetName = dataset_name,
                      cdmVersion = cdm_version,
                      pathToData = path_to_data,
                      overwrite = overwrite)
}

#' Create a copy of an example OMOP CDM dataset
#'
#' @description
#' Creates a copy of a Eunomia database, and returns the path to the new database file.
#' If the dataset does not yet exist on the user's computer it will attempt to download the source data
#' to the the path defined by the EUNOMIA_DATA_FOLDER environment variable.
#'
#' @param datasetName,dataset_name One of "GiBleed" (default),
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
#' "synthea-weight_loss-10k"
#'
#' @param cdmVersion,cdm_version The OMOP CDM version. Must be "5.3" or "5.4".
#' @param databaseFile,database_file The full path to the new copy of the example CDM dataset.
#'
#' @return The file path to the new Eunomia dataset copy
#' @examples
#' \dontrun{
#'  con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("GiBleed"))
#'  DBI::dbDisconnect(con, shutdown = TRUE)
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

    specs <- spec_cdm_field[[cdmVersion]] %>%
      dplyr::mutate(cdmDatatype = dplyr::if_else(.data$cdmDatatype == "varchar(max)", "varchar(2000)", .data$cdmDatatype)) %>%
      dplyr::mutate(cdmFieldName = dplyr::if_else(.data$cdmFieldName == '"offset"', "offset", .data$cdmFieldName)) %>%
      dplyr::mutate(cdmDatatype = dplyr::case_when(
        dbms(con) == "postgresql" & .data$cdmDatatype == "datetime" ~ "timestamp",
        dbms(con) == "redshift" & .data$cdmDatatype == "datetime" ~ "timestamp",
        TRUE ~ cdmDatatype)) %>%
      tidyr::nest(col = -"cdmTableName") %>%
      dplyr::mutate(col = purrr::map(col, ~setNames(as.character(.$cdmDatatype), .$cdmFieldName)))

    files <- tools::file_path_sans_ext(basename(list.files(unzipLocation)))
    tables <- specs$cdmTableName

    for (i in cli::cli_progress_along(tables)) {
      if (isFALSE(tables[i] %in% files)) next

      fields <- specs %>%
        dplyr::filter(.data$cdmTableName == specs$cdmTableName[i]) %>%
        dplyr::pull(.data$col) %>%
        unlist()

      DBI::dbCreateTable(con,
                         inSchema("main", specs$cdmTableName[i], dbms = dbms(con)),
                         fields = fields)

      cols <- paste(glue::glue('"{names(fields)}"'), collapse = ", ")

      table_path <- file.path(unzipLocation, paste0(specs$cdmTableName[i], ".parquet"))
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

#' @export
#' @rdname eunomiaDir
eunomia_dir <- function(dataset_name = "GiBleed",
                        cdm_version = "5.3",
                        database_file = tempfile(fileext = ".duckdb")) {

  eunomiaDir(datasetName = dataset_name,
             cdmVersion = cdm_version,
             databaseFile = database_file)
}

#' Has the Eunomia dataset been cached?
#'
#' @param dataset_name,datasetName Name of the Eunomia dataset to check. Defaults to "GiBleed".
#' @param cdm_version,cdmVersion Version of the Eunomia dataset to check. Must be "5.3" or "5.4".
#'
#' @return TRUE if the eunomia example dataset is available and FASLE otherwise
#' @export
eunomia_is_available <- function(dataset_name = "GiBleed",
                                 cdm_version = "5.3") {

  if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") {
    rlang::abort("Set the environment variable EUNOMIA_DATA_FOLDER to the eunomia cache location")
  }

  stopifnot(is.character(cdm_version), length(cdm_version) == 1, cdm_version %in% c("5.3", "5.4"))

  # check for zip archive of csv source files
  archiveName <- paste0(dataset_name, "_", cdm_version, ".zip")
  archiveLocation <- file.path(Sys.getenv("EUNOMIA_DATA_FOLDER"), archiveName)
  return(file.exists(archiveLocation))
}


#' @rdname eunomia_is_available
#' @export
eunomiaIsAvailable <- function(datasetName = "GiBleed",
                               cdmVersion = "5.3") {
  eunomia_is_available(dataset_name = datasetName,
                       cdm_version = cdmVersion)
}

