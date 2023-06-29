## ---- include = FALSE---------------------------------------------------------
library(CDMConnector)
if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") Sys.setenv("EUNOMIA_DATA_FOLDER" = tempdir())
if (!dir.exists(Sys.getenv("EUNOMIA_DATA_FOLDER"))) dir.create(Sys.getenv("EUNOMIA_DATA_FOLDER"))
if (!eunomia_is_available()) downloadEunomiaData()

knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ---- message=FALSE, warning=FALSE--------------------------------------------
library(CDMConnector)
library(dplyr)

## ---- eval=FALSE--------------------------------------------------------------
#  downloadEunomiaData(
#    pathToData = here::here(), # change to the location you want to save the data
#    overwrite = TRUE
#  )
#  # once downloaded, save path to your Renviron: EUNOMIA_DATA_FOLDER="......"
#  # (and then restart R)

