withr::local_envvar(
  R_USER_CACHE_DIR = tempfile(),
  .local_envir = teardown_env()
)

tryCatch(download_optional_data(), error = function(e) NA)

