# Copyright 2025 DARWIN EU®
#
# This file is part of CodelistGenerator
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

#' @keywords internal
#' @import omopgenerics
#' @import DBI
#' @importMethodsFrom DBI dbConnect
#' @importFrom dplyr all_of matches starts_with contains ends_with
#' @importFrom utils head
#' @importFrom rlang := .env .data
#' @importFrom purrr %||%
#' @importFrom generics compile
#' @importFrom methods is
"_PACKAGE"
NULL
utils::globalVariables(".") # so we can use `.` in dplyr pipelines.
