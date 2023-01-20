withr::local_envvar(
  R_USER_CACHE_DIR = tempfile(),
  .local_envir = teardown_env(),
  EUNOMIA_DATA_FOLDER = tempfile()
)

tryCatch(downloadEunomiaData(), error = function(e) NA)

