#' @keywords internal
#' @import DBI
#' @importMethodsFrom DBI dbConnect
#' @importFrom dbplyr in_schema
#' @importFrom dplyr all_of matches starts_with contains ends_with
#' @importFrom utils head
#' @importFrom rlang :=
#' @importFrom purrr %||%
#' @importFrom generics compile
#' @importFrom methods is
"_PACKAGE"
NULL
utils::globalVariables(".") # so we can use `.` in dplyr pipelines.
