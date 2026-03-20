#' @title Scheduler
#' @description Handles OS-level scheduling of a JobOrchestrator workflow.
#'   On Mac/Linux delegates to \pkg{cronR}; on Windows delegates to \pkg{taskscheduleR}.
#'   Both packages are optional — an informative error is raised if the required
#'   one is not installed.
#' @import R6
#' @export
Scheduler <- R6::R6Class(
  "Scheduler",

  private = list(
    .orchestrator = NULL,
    .script_path  = NULL,

    os = function() {
      if (.Platform$OS.type == "windows") "windows" else "unix"
    },

    check_pkg = function(pkg) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        stop(sprintf(
          "Package '%s' is required for OS scheduling on this platform. Install it with install.packages('%s').",
          pkg, pkg
        ))
      }
    },

    write_runner = function() {
      jobs <- private$.orchestrator$jobs()

      job_lines <- vapply(jobs, function(j) {
        sprintf(
          paste0(
            "  JobNode$new(\n",
            "    name        = %s,\n",
            "    script_path = %s,\n",
            "    on_fail     = %s\n",
            "  )"
          ),
          deparse(j$name),
          deparse(normalizePath(j$script_path, mustWork = FALSE)),
          deparse(j$on_fail)
        )
      }, character(1))

      edge_lines <- character(0)
      for (j in jobs) {
        for (up in j$upstream) {
          edge_lines <- c(edge_lines, sprintf(
            "orc$add_upstream(%s, %s)", deparse(j$name), deparse(up$name)
          ))
        }
      }

      lines <- c(
        "library(rq)",
        "",
        "orc <- JobOrchestrator$new()",
        "",
        paste0("orc$add_job(", job_lines, ")"),
        "",
        if (length(edge_lines) > 0) c(edge_lines, "") else character(0),
        "orc$run()"
      )

      path <- tempfile(pattern = "rq_schedule_", fileext = ".R")
      writeLines(lines, path)
      private$.script_path <- path
      path
    }
  ),

  public = list(
    #' @param orchestrator A JobOrchestrator instance to schedule.
    initialize = function(orchestrator) {
      if (!inherits(orchestrator, "JobOrchestrator")) {
        stop("Expected a JobOrchestrator instance.")
      }
      private$.orchestrator <- orchestrator
    },

    #' @description Schedule the workflow to run on the OS scheduler.
    #'
    #' @param id A unique string identifier for this scheduled task.
    #' @param frequency How often to run. On Mac/Linux: "minutely", "hourly", "daily",
    #'   "weekly", "monthly", or a raw cron expression like "*/15 * * * *".
    #'   On Windows: "MINUTE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "ONCE", "ONLOGON", "ONIDLE".
    #' @param at Time string for the schedule. On Mac/Linux: e.g. "7AM" or "14:30".
    #'   On Windows: 24-hour "HH:MM" format, e.g. "07:00". Defaults to "00:00" on Windows if omitted.
    schedule = function(id, frequency = "daily", at = NULL) {
      script <- private$write_runner()

      if (private$os() == "unix") {
        private$check_pkg("cronR")
        cmd <- cronR::cron_rscript(script)
        args <- list(
          command     = cmd,
          frequency   = frequency,
          id          = id,
          description = paste0("rq workflow: ", id),
          ask         = FALSE
        )
        if (!is.null(at)) args$at <- at
        do.call(cronR::cron_add, args)
      } else {
        private$check_pkg("taskscheduleR")
        taskscheduleR::taskscheduler_create(
          taskname  = id,
          rscript   = script,
          schedule  = frequency,
          starttime = if (!is.null(at)) at else "00:00"
        )
      }

      prettylog("Scheduler", "Scheduled '", id, "' (", frequency, ").")
      invisible(self)
    },

    #' @description Remove a previously scheduled workflow from the OS scheduler.
    #'
    #' @param id The id used when the task was scheduled.
    unschedule = function(id) {
      if (private$os() == "unix") {
        private$check_pkg("cronR")
        cronR::cron_rm(id = id, ask = FALSE)
      } else {
        private$check_pkg("taskscheduleR")
        taskscheduleR::taskscheduler_delete(taskname = id)
      }

      prettylog("Scheduler", "Removed scheduled task '", id, "'.")
      invisible(self)
    },

    #' @description Print a short summary.
    print = function() {
      cat(sprintf("<Scheduler>\n"))
      invisible(self)
    }
  )
)
