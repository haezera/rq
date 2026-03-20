#' @title JobNode
#' @description A single node in the job DAG.
#' @import R6
#' @export
JobNode <- R6::R6Class(
  "JobNode",

  private = list(
    .max_retries = NULL
  ),

  public = list(
    #' @field name Human-readable name for this job, used in printing and keying the graph.
    name = NULL,

    #' @field script_path Absolute or relative path to the R script to execute.
    script_path = NULL,

    #' @field upstream JobNodes that must complete before this one runs.
    upstream = NULL,

    #' @field downstream Populated automatically by JobOrchestrator$add_job() — do not set manually.
    downstream = NULL,

    #' @field status One of: "pending", "running", "success", "failed".
    status = NULL,

    #' @field last_run POSIXct timestamp of the most recent execution attempt.
    last_run = NULL,

    #' @field on_fail How to handle downstream jobs if this one fails.
    #'   "skip" marks downstream as skipped and lets unrelated branches continue.
    #'   "halt" stops the entire graph immediately.
    on_fail = NULL,

    #' @field retries_remaining Number of retry attempts still available.
    retries_remaining = NULL,

    #' @param name Unique string identifier for this job.
    #' @param script_path Path to the R script this job executes.
    #' @param upstream List of JobNode objects this job depends on.
    #' @param on_fail Either "skip" or "halt". See \code{$on_fail} field.
    initialize = function(name, script_path, upstream = list(), on_fail = "skip") {
      stopifnot(is.character(name), length(name) == 1)
      stopifnot(file.exists(script_path))
      stopifnot(on_fail %in% c("skip", "halt"))

      self$name <- name
      self$script_path <- script_path
      self$upstream <- upstream
      self$downstream <- list()
      self$on_fail <- on_fail
      self$retries_remaining <- 0L
      private$.max_retries <- 0L
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

    #' @description Reset to pending and restore the original retry count.
    reset = function() {
      self$status <- "pending"
      self$last_run <- NULL
      self$retries_remaining <- private$.max_retries
    },

    #' @description Set the maximum retries for this job (called by JobOrchestrator$add_job).
    #' @param n Integer number of retries.
    set_max_retries = function(n) {
      private$.max_retries <- as.integer(n)
      self$retries_remaining <- as.integer(n)
    },

    #' @description Print a short summary of the job.
    #' @param ... Ignored.
    print = function(...) {
      cat(sprintf("<JobNode: %s [%s]>\n", self$name, self$status))
      invisible(self)
    }
  )
)
