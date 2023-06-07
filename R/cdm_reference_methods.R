
#' Subset a cdm reference object
#'
#' @param x A cdm reference
#' @param name The name of the table to extract from the cdm object
#'
#' @return A single cdm table reference
#' @export
`$.cdm_reference` <- function(x, name) {
  x[[name]]
}

#' Subset a cdm reference object
#'
#' @param x A cdm reference
#' @param i The name or index of the table to extract from the cdm object
#'
#' @return A single cdm table reference
#' @export
`[.cdm_reference` <- function(x, i) {
  cdm_select_tbl(x, dplyr::all_of(i))
}

#' Subset a cdm reference object
#'
#' @param x A cdm reference
#' @param i The name or index of the table to extract from the cdm object
#'
#' @return A single cdm table reference
#' @export
`[[.cdm_reference` <- function(x, i) {
  x_raw <- unclass(x)
  tbl <- x_raw[[i]]

  if(is.null(tbl)) return(NULL)

  attr(tbl, "cdm_reference") <- x
  return(tbl)
}

