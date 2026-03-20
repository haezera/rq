#' @title JobOrchestrator
#' @description Owns the job graph, validates it, and drives execution.
#' @import R6
#' @export
JobOrchestrator <- R6::R6Class(
  "JobOrchestrator",

  private = list(
    jobs = NULL,

    has_cycle = function() {
      visited <- character(0)
      in_stack <- character(0)

      dfs <- function(job) {
        visited  <<- c(visited, job$id)
        in_stack <<- c(in_stack, job$id)

        # use dfs to find if there is a loop
        for (dep in job$downstream) {
          if (!dep$id %in% visited) {
            if (dfs(dep)) return(TRUE)
          } else if (dep$id %in% in_stack) {
            return(TRUE)
          }
        }

        in_stack <<- setdiff(in_stack, job$id)
        FALSE
      }

      # check if there exists a cycle that starts at each node
      # NOTE: is this actually efficient?
      for (job in private$jobs) {
        if (!job$id %in% visited) {
          if (dfs(job)) return(TRUE)
        }
      }

      FALSE
    },

    # if there exists a problem with a job, then propagate to skip all jobs
    # that depend on the current job
    skip_downstream = function(job) {
      for (dep in job$downstream) {
        if (dep$status == "pending") {
          dep$status <- "skipped"
          private$skip_downstream(dep)
        }
      }
    }
  ),

  public = list(
    #' @field max_workers Maximum number of jobs allowed to run concurrently.
    max_workers = NULL,

    #' @param max_workers Cap on concurrent jobs. Defaults to all available cores minus one,
    #'   leaving headroom for the orchestrator itself.
    initialize = function(max_workers = parallel::detectCores() - 1L) {
      private$jobs <- list()
      self$max_workers <- max_workers
    },

    #' @description Register a JobNode with the orchestrator.
    #'   Edges are inferred from the node's upstream/downstream fields.
    #'   Validates the graph is still acyclic after each addition.
    #' 
    #' @param job A JobNode instance.
    add_job = function(job) {
      if (!inherits(job, "JobNode")) stop("Expected a JobNode instance.")
      if (job$id %in% names(private$jobs)) stop(sprintf("Job '%s' already registered.", job$id))

      private$jobs[[job$id]] <- job

      if (private$has_cycle()) {
        private$jobs[[job$id]] <- NULL
        stop(sprintf("Adding job '%s' would introduce a cycle. Please consult your job dependencies", job$id))
      }

      invisible(self)
    },

    #' @description Start executing the graph, blocking until all jobs are done.
    #' @param poll_interval Seconds between status checks.
    run = function(poll_interval = 1) {
      if (length(private$jobs) == 0) stop("No jobs registered.")

      handles <- list()

      terminal <- c("success", "failed", "skipped")
      halted   <- FALSE

      while (TRUE) {
        for (id in names(handles)) {
          handle <- handles[[id]]
          job    <- private$jobs[[id]]

          if (!handle$is_alive()) {
            if (handle$get_exit_status() == 0L) {
              job$status <- "success"
            } else {
              job$status <- "failed"

              if (job$on_fail == "halt") {
                halted <- TRUE
              } else {
                private$skip_downstream(job)
              }
            }

            handles[[id]] <- NULL
          }
        }

        if (halted) {
          # this job is halted, so skip everything that relies on it
          for (job in private$jobs) {
            if (!job$status %in% terminal) job$status <- "skipped"
          }
          stop("Graph halted due to a failed job with on_fail = 'halt'.")
        }

        # if the job is ready to go, kick it off — but respect the worker cap
        for (job in private$jobs) {
          if (length(handles) >= self$max_workers) break
          if (job$status == "pending" && job$is_ready()) {
            handles[[job$id]] <- job$run()
          }
        }

        # all jobs are done, then we can break
        all_done <- all(vapply(private$jobs, function(j) j$status %in% terminal, logical(1)))
        if (all_done && length(handles) == 0) break

        Sys.sleep(poll_interval)
      }

      invisible(self)
    },

    #' @description Summary of each job's final status.
    status = function() {
      ids      <- names(private$jobs)
      statuses <- vapply(private$jobs, function(j) j$status, character(1))
      data.frame(id = ids, status = statuses, row.names = NULL)
    },

    print = function(...) {
      cat(sprintf("<JobOrchestrator: %d job(s)>\n", length(private$jobs)))
      invisible(self)
    }
  )
)
