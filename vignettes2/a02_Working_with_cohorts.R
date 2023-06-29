## ---- include = FALSE---------------------------------------------------------

knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

library(CDMConnector)
library(dplyr, warn.conflicts = FALSE)

if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") Sys.setenv("EUNOMIA_DATA_FOLDER" = file.path(tempdir(), "eunomia"))
if (!dir.exists(Sys.getenv("EUNOMIA_DATA_FOLDER"))) dir.create(Sys.getenv("EUNOMIA_DATA_FOLDER"))
if (!eunomia_is_available()) downloadEunomiaData()


