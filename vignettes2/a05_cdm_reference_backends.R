## ---- include = FALSE---------------------------------------------------------
library(CDMConnector)
if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") Sys.setenv("EUNOMIA_DATA_FOLDER" = file.path(tempdir(), "eunomia"))
if (!dir.exists(Sys.getenv("EUNOMIA_DATA_FOLDER"))) dir.create(Sys.getenv("EUNOMIA_DATA_FOLDER"))
if (!eunomia_is_available()) downloadEunomiaData()

knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>", 
  build = eunomia_is_available()
)

## ----pressure, echo=FALSE, out.width = '80%'----------------------------------
knitr::include_graphics("locations.png")

## ---- message=FALSE, warning=FALSE--------------------------------------------
library(CDMConnector)
library(dplyr, warn.conflicts = FALSE)
library(ggplot2)

