#' @title Download optional data for this package if required.
#'
#' @description Ensure that the optional data is available locally in the package cache.
#' Will try to download the data only if it is not available.
#'
#' @return Named list. The list has entries: "available": vector of strings.
#' The names of the files that are available in the local file cache.
#' You can access them using get_optional_data_file(). "missing": vector of strings.
#' The names of the files that this function was unable to retrieve.
#'
#' @export
download_optional_data <- function() {
  pkg_info = pkgfilecache::get_pkg_info("CDMConnector");        # to identify the package using the cache

  # Replace these with your optional data files.
  local_filenames = c("cdm.duckdb.tar.xz");    # How the files should be called in the local package file cache
  urls = c("https://github.com/OdyOSG/EunomiaData/raw/main/cdm.duckdb.tar.xz"); # Remote URLs where to download files from
  md5sums = c("993295ca7a65a56fa94933591ee51da0");    # MD5 checksums. Optional but recommended.

  cfiles = pkgfilecache::ensure_files_available(pkg_info, local_filenames, urls, md5sums=md5sums);
  cfiles$file_status = NULL;
  return(cfiles);
}

#' @title Get file names available in package cache.
#'
#' @description Get file names of optional data files which are available in the local package cache.
#' You can access these files with get_optional_data_file().
#'
#' @return vector of strings. The file names available, relative to the package cache.
#'
list_optional_data <- function() {
  pkg_info = pkgfilecache::get_pkg_info("CDMConnector");
  return(pkgfilecache::list_available(pkg_info));
}


#' @title Access a single file from the package cache by its file name.
#'
#' @param filename, string. The filename of the file in the package cache.
#'
#' @param mustWork, logical. Whether an error should be created if the file does not exist.
#' If mustWork=FALSE and the file does not exist, the empty string is returned.
#'
#' @return string. The full path to the file in the package cache.
#' Use this in your application code to open the file.
#'
get_optional_data_filepath <- function(filename, mustWork=TRUE) {
  pkg_info = pkgfilecache::get_pkg_info("CDMConnector");
  return(pkgfilecache::get_filepath(pkg_info, filename, mustWork=mustWork));
}


#' @title Delete all data in the package cache.
#'
#' @return integer. The return value of the unlink() call: 0 for success, 1 for failure.
#' See the unlink() documentation for details.
#'
#' @export
delete_all_optional_data <- function() {
  pkg_info = pkgfilecache::get_pkg_info("CDMConnector");
  return(pkgfilecache::erase_file_cache(pkg_info));
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
  rlang::check_installed("duckdb", version = "0.6.0", reason = "duckdb version 0.6 is required to use eunomia_dir()")
  if (!eunomia_is_available()) {
    rlang::abort("The Eunomia example dataset is not available. Download it by running `CDMConnector::download_optional_data()`")
  }

  if (is.null(exdir)) exdir <- file.path(tempdir(TRUE), paste(sample(letters, 8, replace = TRUE), collapse = ""))
  file <- xzfile(eunomia_cache(), open = "rb")
  untar(file, exdir = exdir)
  close(file)
  path <- file.path(exdir, "cdm.duckdb")
  if(!file.exists(path)) rlang::abort("Error creating Eunomia CDM")
  return(path)
}

#' Has the eunomia dataset been cached?
#'
#' @return TRUE if the eunomia example dataset is available and FASLE otherwise
#' @export
eunomia_is_available <- function() {
  pkg_info = pkgfilecache::get_pkg_info("CDMConnector");
  optional_data <- pkgfilecache::list_available(pkg_info)
  return("cdm.duckdb.tar.xz" %in% optional_data)
}

# return the location to the cached eunomia dataset
eunomia_cache <- function() {
  return(get_optional_data_filepath("cdm.duckdb.tar.xz"))
}

#' @export
#' @describeIn eunomia_dir camelCase alias
eunomiaDir <- eunomia_dir
