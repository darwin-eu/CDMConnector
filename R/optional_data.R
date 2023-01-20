#' Download Eunomia data files
#'
#' Download the Eunomia data files from https://github.com/darwin-eu/EunomiaDatasets
#'
#' @param datasetName   The data set name as found on https://github.com/darwin-eu/EunomiaDatasets. The
#'                      data set name corresponds to the folder with the data set ZIP files
#' @param cdmVersion    The OMOP CDM version. This version will appear in the suffix of the data file,
#'                      for example: <datasetName>_<cdmVersion>.zip. Default: '5.3'
#' @param pathToData    The path where the Eunomia data is stored on the file system., By default the
#'                      value of the environment variable "EUNOMIA_DATA_FOLDER" is used.
#' @param overwrite     Control whether the existing archive file will be overwritten should it already
#'                      exist.
#' @return
#' Invisibly returns the destination if the download was successful.
#' @examples
#' \dontrun{
#' downloadEunomiaData("GiBleed")
#' }
#' @export
downloadEunomiaData <- function(datasetName = "GiBleed",
                                cdmVersion = "5.3",
                                pathToData = Sys.getenv("EUNOMIA_DATA_FOLDER"),
                                overwrite = FALSE) {
  if (is.null(pathToData) || is.na(pathToData) || pathToData == "") {
    stop("The pathToData argument must be specified. Consider setting the EUNOMIA_DATA_FOLDER environment variable, for example in the .Renviron file.")
  }

  if (is.null(datasetName) || is.na(datasetName) || datasetName == "") {
    stop("The datasetName argument must be specified.")
  }

  if (pathToData != Sys.getenv("EUNOMIA_DATA_FOLDER")) {
    rlang::inform(paste0(
      "Consider adding `EUNOMIA_DATA_FOLDER='",
      pathToData,
      "'` to ",
      path.expand("~/.Renviron"),
      " and restarting R."
    ))
  }

  if (!dir.exists(pathToData)) {
    dir.create(pathToData, recursive = TRUE)
  }

  datasetNameVersion <- paste0(datasetName, "_", cdmVersion)
  zipName <- paste0(datasetNameVersion, ".zip")

  if (file.exists(file.path(pathToData, zipName)) & !overwrite) {
    cat(paste0(
      "Dataset already exists (",
      file.path(pathToData, zipName),
      "). Specify overwrite=T to overwrite existing zip archive."
    ))
    invisible()
  } else {
    # downloads the file from github
    baseUrl <- "https://raw.githubusercontent.com/darwin-eu/EunomiaDatasets/main/datasets"
    result <- utils::download.file(
      url = paste(baseUrl, datasetName, zipName, sep = "/"),
      destfile = file.path(
        pathToData,
        zipName
      )
    )

    invisible(pathToData)
  }
}

#' Extract the Eunomia data files and load into a SQLite or duckdb database
#'
#' Extract files from a .ZIP file and creates a SQLite or duckdb OMOP CDM database that is then stored in the
#' same directory as the .ZIP file.
#'
#' @param from The path to the .ZIP file that contains the csv CDM source files
#' @param to The path to the .sqlite or .duckdb file that will be created
#' @param dbms The file based database system to use: 'sqlite' (default) or 'duckdb'
#' @param verbose Print progress notes? TRUE or FALSE
#' @importFrom tools file_ext
extractLoadData <- function(from, to, dbms = "sqlite", verbose = FALSE) {
  stopifnot(dbms == "sqlite" || dbms == "duckdb", is.logical(verbose), length(verbose) == 1)
  stopifnot(is.character(from), length(from) == 1, nchar(from) > 0)
  stopifnot(is.character(to), length(to) == 1, nchar(from) > 0)
  if (tools::file_ext(from) != "zip") stop("Source must be a .zip file")
  if (!file.exists(from)) stop(paste0("zipped csv archive '", from, "' not found!"))

  tempFileLocation <- tempfile()
  if(verbose) cli::cat_line(paste0("Unzipping ", from))
  utils::unzip(zipfile = from, exdir = tempFileLocation)


  # get list of files in directory and load them into the SQLite database
  dataFiles <- sort(list.files(path = tempFileLocation, pattern = "*.csv"))
  if (length(dataFiles) <= 0) {
    stop("Data file does not contain .CSV files to load into the database.")
  }
  databaseFileName <- paste0(tools::file_path_sans_ext(basename(from)), ".", dbms)
  databaseFilePath <- file.path(tempFileLocation, databaseFileName)

  if (dbms == "sqlite") {
    rlang::check_installed("RSQLite")
    connection <- DBI::dbConnect(RSQLite::SQLite(), dbname = databaseFilePath)
    on.exit(DBI::dbDisconnect(connection), add = TRUE)
  } else if (dbms == "duckdb") {
    rlang::check_installed("duckdb")
    connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = databaseFilePath)
    # If the function is successful dbDisconnect will be called twice generating a warning.
    # If this function is unsuccessful, still close connection on exit.
    on.exit(suppressWarnings(DBI::dbDisconnect(connection, shutdown = TRUE)), add = TRUE)
  }

  on.exit(unlink(tempFileLocation), add = TRUE)

  if(verbose) {
    cli::cat_rule(paste0("Loading database ", databaseFileName), col = "grey")
  }

  for (i in 1:length(dataFiles)) {
    tableData <- readr::read_csv(
      file = file.path(tempFileLocation, dataFiles[i]),
      col_types = readr::cols(),
      guess_max = 2e6,
      lazy = FALSE
    )
    # CDM table and column names should be lowercase: https://github.com/OHDSI/CommonDataModel/issues/509#issuecomment-1315754238
    names(tableData) <- tolower(names(tableData))
    tableName <- tools::file_path_sans_ext(tolower(dataFiles[i]))
    DBI::dbWriteTable(conn = connection, name = tableName, value = tableData)
    if (verbose) {
      cli::cat_bullet(tableName, bullet = 1)
    }
  }
  # An open duckdb database file cannot be copied on windows
  if (dbms == "duckdb") {
    DBI::dbDisconnect(connection, shutdown = TRUE)
  }
  rc <- file.copy(from = databaseFilePath, to = to, overwrite = TRUE)
  if (isFALSE(rc)) {
    rlang::abort(paste("File copy from", databaseFilePath, "to", to, "failed!"))
  }
  if (verbose) {
    cli::cat_line("Database load complete", col = "grey")
  }
}

#' Create a copy of a Eunomia dataset
#'
#' @description
#' Creates a copy of a Eunomia database, and returns the path to the new database file.
#' If the dataset does not yet exist on the user's computer it will attempt to download the source data
#' to the the path defined by the EUNOMIA_DATA_FOLDER environment variable.
#'
#' @param datasetName    The data set name as found on https://github.com/darwin-eu/EunomiaDatasets. The
#'                       data set name corresponds to the folder with the data set ZIP files
#' @param cdmVersion     The OMOP CDM version. This version will appear in the suffix of the data file,
#'                       for example: <datasetName>_<cdmVersion>.zip. Default: '5.3'
#' @param pathToData     The path where the Eunomia data is stored on the file system., By default the
#'                       value of the environment variable "EUNOMIA_DATA_FOLDER" is used.
#' @param dbms           The database system to use. "sqlite" or "duckdb" (default)
#' @param databaseFile   The path where the database file will be copied to. By default, the database
#'                       will be copied to a temporary folder, and will be deleted at the end of the R
#'                       session.
#'
#' @return The file path to the new Eunomia dataset copy
#' @examples
#' \dontrun{
#'  conn <- DBI::dbConnect(RSQLite::SQLite(), eunomiaDir("GiBleed"))
#'  DBI::dbDisconnect(conn)
#'
#'  conn <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("GiBleed", dbms = "duckdb"))
#'  DBI::dbDisconnect(conn, shutdown = TRUE)
#'
#'  conn <- DatabaseConnector::connect(dbms = "sqlite", server = eunomiaDir("GiBleed"))
#'  DatabaseConnector::disconnect(conn)
#' }
#'
eunomiaDir <- function(datasetName = "GiBleed",
                       cdmVersion = "5.3",
                       pathToData = Sys.getenv("EUNOMIA_DATA_FOLDER"),
                       dbms = "duckdb",
                       databaseFile = tempfile(fileext = paste0(".", dbms))) {

  if (is.null(pathToData) || is.na(pathToData) || pathToData == "") {
    stop("The pathToData argument must be specified. Consider setting the EUNOMIA_DATA_FOLDER environment variable, for example in the .Renviron file.")
  }

  stopifnot(is.character(dbms), length(dbms) == 1, dbms %in% c("sqlite", "duckdb"))
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% c("5.3", "5.4"))

  if (dbms == "duckdb") {
    rlang::check_installed("duckdb")
    # duckdb database are tied to a specific version of duckdb until it reaches v1.0
    duckdbVersion <- substr(utils::packageVersion("duckdb"), 1, 3)
    datasetFileName <- paste0(datasetName, "_", cdmVersion, "_", duckdbVersion, ".", dbms)
  } else {
    datasetFileName <- paste0(datasetName, "_", cdmVersion, ".", dbms)
  }

  # cached sqlite or duckdb file to be copied
  datasetLocation <- file.path(pathToData, datasetFileName)
  datasetAvailable <- file.exists(datasetLocation)

  # zip archive of csv source files
  archiveName <- paste0(datasetName, "_", cdmVersion, ".zip")
  archiveLocation <- file.path(pathToData, archiveName)
  archiveAvailable <- file.exists(archiveLocation)

  if (!datasetAvailable & !archiveAvailable) {
    writeLines(paste("attempting to download", datasetName))
    downloadEunomiaData(datasetName = datasetName, cdmVersion = cdmVersion)
    archiveAvailable <- TRUE
  }

  if (!datasetAvailable & archiveAvailable) {
    writeLines(paste("attempting to extract and load", archiveLocation))
    extractLoadData(from = archiveLocation, to = datasetLocation, dbms = dbms)
    datasetAvailable <- TRUE
  }

  rc <- file.copy(from = datasetLocation, to = databaseFile)
  if (isFALSE(rc)) {
    stop(paste("File copy from", datasetLocation, "to", databaseFile, "failed!"))
  }
  return(databaseFile)
}

#' Create a new Eunomia CDM
#'
#' Create a copy of the duckdb Eunomia CDM and return the file path
#'
#' @param exdir Enclosing directory where the Eunomia CDM should be created.
#' If NULL (default) then a temp folder is created.
#'
#' @return The full path to the new Eunomia CDM that can be passed to `dbConnect()`
#' @export
#' @importFrom utils untar download.file menu
#' @examples
#' \dontrun{
#' library(DBI)
#' library(CDMConnector)
#' con <- dbConnect(duckdb::duckdb(), dbdir = getEunomiaPath())
#' dbListTables(con)
#' dbDisconnect(con)
#' }
eunomia_dir <- function(exdir = NULL) {
  rlang::check_installed("duckdb")
  if (!eunomia_is_available()) {
    rlang::abort("The Eunomia example dataset is not available. Download it by running `CDMConnector::downloadEunomiaData()`")
  }
  if (is.null(exdir)) exdir <- file.path(tempdir(TRUE), paste(sample(letters, 8, replace = TRUE), collapse = ""))

  path <- eunomiaDir(datasetName = "GiBleed",
                     cdmVersion = "5.3",
                     pathToData = Sys.getenv("EUNOMIA_DATA_FOLDER"),
                     dbms = "duckdb",
                     databaseFile = exdir)

  if(!file.exists(path)) rlang::abort("Error creating Eunomia CDM")
  return(path)
}

#' Has the eunomia dataset been cached?
#'
#' @param datasetName Name of the Eunomia dataset to check. Defaults to "GiBleed".
#' @param cdmVersion Version of the Eunomia dataset to check. Must be "5.3" or "5.4".
#'
#' @return TRUE if the eunomia example dataset is available and FASLE otherwise
#' @export
eunomia_is_available <- function(datasetName = "GiBleed",
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



