#' @title JobNode
#' @description A single node in the job DAG.
#' @import R6
#' @export
JobNode <- R6::R6Class(
  "JobNode",

  public = list(
    #' @field id Unique identifier for this job.
    id = NULL,

    #' @field script_path Absolute or relative path to the R script to execute.
    script_path = NULL,

    #' @field upstream JobNodes that must complete before this one runs.
    upstream = NULL,

    #' @field downstream JobNodes that are unblocked when this one completes.
    downstream = NULL,

    #' @field status One of: "pending", "running", "success", "failed".
    status = NULL,

    #' @field last_run POSIXct timestamp of the most recent execution attempt.
    last_run = NULL,

    #' @field on_fail How to handle downstream jobs if this one fails.
    #'   "skip" marks downstream as skipped and lets unrelated branches continue.
    #'   "halt" stops the entire graph immediately.
    on_fail = NULL,

    #' @param id Unique string identifier for this job.
    #' @param script_path Path to the R script this job executes.
    #' @param upstream List of JobNode objects this job depends on.
    #' @param downstream List of JobNode objects that depend on this job.
    #' @param on_fail Either "skip" or "halt". See \code{$on_fail} field.
    initialize = function(id, script_path, upstream = list(), downstream = list(), on_fail = "skip") {
      stopifnot(is.character(id), length(id) == 1)
      stopifnot(file.exists(script_path))
      stopifnot(on_fail %in% c("skip", "halt"))

      self$id <- id
      self$script_path <- script_path
      self$upstream <- upstream
      self$downstream <- downstream
      self$on_fail <- on_fail
      self$status <- "pending"
      self$last_run <- NULL
    },

    #' @description Run the job's script in a fresh R session.
    #' 
    #' Must only be called if and only if `$is_ready` returns TRUE.
    #' 
    #' @return A callr process handle for the caller to poll or wait on.
    run = function() {
      self$status <- "running"
      self$last_run <- Sys.time()

      handle <- callr::rscript_process$new(
        callr::rscript_process_options(script = self$script_path)
      )

      handle
    },

    #' @description Whether all upstream jobs have succeeded.
    is_ready = function() {
      all(sapply(self$upstream, function(j) j$status == "success"))
    },

    #' @description Reset to pending, e.g. for re-runs or testing.
    reset = function() {
      self$status <- "pending"
      self$last_run <- NULL
    },

    print = function(...) {
      cat(sprintf("<JobNode: %s [%s]>\n", self$id, self$status))
      invisible(self)
    }
  )
)
