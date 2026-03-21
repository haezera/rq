#' @title JobOrchestrator
#' @description Owns the job graph, validates it, and drives execution.
#' @import R6
#' @importFrom visNetwork visNetwork visEdges visNodes visOptions
#' @export
JobOrchestrator <- R6::R6Class(
  "JobOrchestrator",

  private = list(
    .jobs = NULL,
    .max_retries = NULL,

    has_cycle = function() {
      visited <- character(0)
      in_stack <- character(0)

      dfs <- function(job) {
        visited  <<- c(visited, job$name)
        in_stack <<- c(in_stack, job$name)

        # use dfs to find if there is a loop
        for (dep in job$downstream) {
          if (!dep$name %in% visited) {
            if (dfs(dep)) return(TRUE)
          } else if (dep$name %in% in_stack) {
            return(TRUE)
          }
        }

        in_stack <<- setdiff(in_stack, job$name)
        FALSE
      }

      # check if there exists a cycle that starts at each node
      # NOTE: is this actually efficient?
      for (job in private$.jobs) {
        if (!job$name %in% visited) {
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
      private$.jobs <- list()
      private$.max_retries <- list()
      self$max_workers <- max_workers
    },

    #' @description Register a JobNode with the orchestrator.
    #'   Downstream edges are inferred automatically from the job's upstream list.
    #'   Validates the graph is still acyclic after each addition.
    #'
    #' @param job A JobNode instance.
    #' @param retries Number of times to retry the job on failure before giving up. Defaults to 0.
    add_job = function(job, retries = 0L) {
      if (!inherits(job, "JobNode")) stop("Expected a JobNode instance.")
      if (job$name %in% names(private$.jobs)) stop(sprintf("Job '%s' already registered.", job$name))
      stopifnot(is.numeric(retries), length(retries) == 1, retries >= 0)
      job$retries_remaining <- as.integer(retries)
      private$.max_retries[[job$name]] <- as.integer(retries)

      # Wire up the reverse edges so users only need to declare upstream.
      for (dep in job$upstream) {
        dep$downstream <- c(dep$downstream, list(job))
      }

      private$.jobs[[job$name]] <- job

      if (private$has_cycle()) {
        private$.jobs[[job$name]] <- NULL
        for (dep in job$upstream) {
          dep$downstream <- Filter(function(d) d$name != job$name, dep$downstream)
        }
        stop(sprintf("Adding job '%s' would introduce a cycle. Please consult your job dependencies", job$name))
      }

      invisible(self)
    },

    #' @description Start executing the graph, blocking until all jobs are done.
    #' @param poll_interval Seconds between status checks.
    run = function(poll_interval = 1) {
      if (length(private$.jobs) == 0) stop("No jobs registered.")

      handles <- list()

      terminal <- c("success", "failed", "skipped")
      halted      <- FALSE
      halted_job  <- NULL

      while (TRUE) {
        for (id in names(handles)) {
          handle <- handles[[id]]
          job    <- private$.jobs[[id]]

          # Drain whatever output has accumulated since the last tick, whether
          # or not the process is still running.
          stdout <- handle$read_output_lines()
          stderr <- handle$read_error_lines()

          for (line in stdout) prettylog(job$name, line)
          for (line in stderr) prettylog(job$name, "[err] ", line)

          if (!handle$is_alive()) {

            if (handle$get_exit_status() == 0L) {
              job$status <- "success"
            } else if (job$retries_remaining > 0L) {
              job$retries_remaining <- job$retries_remaining - 1L
              prettylog(job$name, "failed, retrying (", job$retries_remaining, " attempt(s) left).")
              job$status <- "pending"
            } else {
              job$status <- "failed"

              if (job$on_fail == "halt") {
                halted     <- TRUE
                halted_job <- job$name
              } else {
                private$skip_downstream(job)
              }
            }

            handles[[id]] <- NULL
          }
        }

        if (halted) {
          # this job is halted, so skip everything that relies on it
          for (job in private$.jobs) {
            if (!job$status %in% terminal) job$status <- "skipped"
          }
          stop(sprintf("Graph halted: job '%s' failed with on_fail = 'halt'.", halted_job))
        }

        # if the job is ready to go, kick it off — but respect the worker cap
        for (job in private$.jobs) {
          if (length(handles) >= self$max_workers) break
          if (job$status == "pending" && job$is_ready()) {
            handles[[job$name]] <- job$run()
          }
        }

        # all jobs are done, then we can break
        all_done <- all(vapply(private$.jobs, function(j) j$status %in% terminal, logical(1)))
        if (all_done && length(handles) == 0) break

        Sys.sleep(poll_interval)
      }

      invisible(self)
    },

    #' @description Remove a registered job and detach all edges.
    #'   Upstream jobs will have this job removed from their downstream list.
    #'   Downstream jobs will have this job removed from their upstream list (detached, not removed).
    #'
    #' @param name The name of the job to remove.
    remove_job = function(name) {
      if (!name %in% names(private$.jobs)) stop(sprintf("No job named '%s' is registered.", name))

      job <- private$.jobs[[name]]

      for (dep in job$upstream) {
        dep$downstream <- Filter(function(d) d$name != name, dep$downstream)
      }

      for (dep in job$downstream) {
        dep$upstream <- Filter(function(u) u$name != name, dep$upstream)
      }

      private$.jobs[[name]] <- NULL
      invisible(self)
    },

    #' @description Add an upstream dependency between two registered jobs by name.
    #'   Validates the graph remains acyclic after wiring.
    #'
    #' @param job_name The name of the job that depends on the upstream job.
    #' @param upstream_name The name of the job to add as an upstream dependency.
    add_upstream = function(job_name, upstream_name) {
      if (!job_name %in% names(private$.jobs)) stop(sprintf("No job named '%s' is registered.", job_name))
      if (!upstream_name %in% names(private$.jobs)) stop(sprintf("No job named '%s' is registered.", upstream_name))

      job <- private$.jobs[[job_name]]
      upstream_job <- private$.jobs[[upstream_name]]

      if (upstream_name %in% vapply(job$upstream, function(u) u$name, character(1))) {
        stop(sprintf("'%s' is already an upstream dependency of '%s'.", upstream_name, job_name))
      }

      job$upstream <- c(job$upstream, list(upstream_job))
      upstream_job$downstream <- c(upstream_job$downstream, list(job))

      if (private$has_cycle()) {
        job$upstream <- Filter(function(u) u$name != upstream_name, job$upstream)
        upstream_job$downstream <- Filter(function(d) d$name != job_name, upstream_job$downstream)
        stop(sprintf("Adding '%s' as upstream of '%s' would introduce a cycle.", upstream_name, job_name))
      }

      invisible(self)
    },

    #' @description Remove an upstream dependency between two registered jobs by name.
    #'
    #' @param job_name The name of the job to remove the upstream dependency from.
    #' @param upstream_name The name of the upstream job to detach.
    remove_upstream = function(job_name, upstream_name) {
      if (!job_name %in% names(private$.jobs)) stop(sprintf("No job named '%s' is registered.", job_name))
      if (!upstream_name %in% names(private$.jobs)) stop(sprintf("No job named '%s' is registered.", upstream_name))

      job <- private$.jobs[[job_name]]
      upstream_job <- private$.jobs[[upstream_name]]

      if (!upstream_name %in% vapply(job$upstream, function(u) u$name, character(1))) {
        stop(sprintf("'%s' is not an upstream dependency of '%s'.", upstream_name, job_name))
      }

      job$upstream <- Filter(function(u) u$name != upstream_name, job$upstream)
      upstream_job$downstream <- Filter(function(d) d$name != job_name, upstream_job$downstream)

      invisible(self)
    },

    #' @description Validate the orchestrator state before running.
    #'   Checks that all script paths still exist and the graph is acyclic.
    #'
    #' @return TRUE if valid, FALSE otherwise (with a warning per issue found).
    validate = function() {
      valid <- TRUE

      for (job in private$.jobs) {
        if (!file.exists(job$script_path)) {
          warning(sprintf("Job '%s': script not found at '%s'.", job$name, job$script_path))
          valid <- FALSE
        }
      }

      if (private$has_cycle()) {
        warning("Graph contains a cycle.")
        valid <- FALSE
      }

      valid
    },

    #' @description Reset all registered jobs to pending and restore their retry counts.
    #'   Use this to re-run the entire graph after a failure.
    reset_all = function() {
      for (job in private$.jobs) {
        job$status <- "pending"
        job$last_run <- NULL
        job$retries_remaining <- private$.max_retries[[job$name]]
      }
      invisible(self)
    },

    #' @description Returns the list of registered JobNodes, keyed by id.
    jobs = function() {
      private$.jobs
    },

    #' @description Summary of each job's final status.
    status = function() {
      names    <- names(private$.jobs)
      statuses <- vapply(private$.jobs, function(j) j$status, character(1))
      data.frame(name = names, status = statuses, row.names = NULL)
    },

    #' @description Render the job DAG as an interactive graph in the RStudio Viewer.
    #'   Nodes are colored by current status:
    #'   pending = white, running = gold, success = green, failed = red, skipped = grey.
    visualise = function() {
      status_color <- c(
        pending = "#FFFFFF",
        running  = "#F5A623",
        success  = "#7ED321",
        failed   = "#D0021B",
        skipped  = "#9B9B9B"
      )
      status_font_color <- c(
        pending = "#000000",
        running  = "#FFFFFF",
        success  = "#FFFFFF",
        failed   = "#FFFFFF",
        skipped  = "#FFFFFF"
      )

      job_names <- names(private$.jobs)
      job_index <- setNames(seq_along(private$.jobs), job_names)

      nodes <- data.frame(
        id    = unname(job_index),
        label = vapply(private$.jobs, function(j) paste0(j$name, "\n[", j$status, "]"), character(1)),
        color = vapply(private$.jobs, function(j) status_color[[j$status]],      character(1)),
        font.color = vapply(private$.jobs, function(j) status_font_color[[j$status]], character(1)),
        shape = "box",
        stringsAsFactors = FALSE
      )

      edge_from <- integer(0)
      edge_to   <- integer(0)
      for (job in private$.jobs) {
        for (up in job$upstream) {
          edge_from <- c(edge_from, job_index[[up$name]])
          edge_to   <- c(edge_to,   job_index[[job$name]])
        }
      }

      edges <- if (length(edge_from) > 0) {
        data.frame(from = edge_from, to = edge_to, arrows = "to", stringsAsFactors = FALSE)
      } else {
        data.frame(from = integer(0), to = integer(0), arrows = character(0), stringsAsFactors = FALSE)
      }

      visNetwork(nodes, edges) |>
        visEdges(smooth = list(type = "cubicBezier")) |>
        visOptions(highlightNearest = TRUE)
    },

    #' @description Print a short summary of the orchestrator.
    #' @param ... Ignored.
    print = function(...) {
      cat(sprintf("<JobOrchestrator: %d job(s)>\n", length(private$.jobs)))
      invisible(self)
    }
  )
)
