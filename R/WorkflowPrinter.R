#' @title WorkflowPrinter
#' @description Renders the job DAG as an ASCII tree for inspection.
#' @import R6
#' @export
WorkflowPrinter <- R6::R6Class(
  "WorkflowPrinter",

  private = list(
    status_symbol = function(status) {
      switch(status,
        pending = "[ ]",
        running = "[~]",
        success = "[x]",
        failed  = "[!]",
        skipped = "[-]",
        "[ ]"
      )
    },

    # Recursively print a job and its downstream, tracking indent level and
    # which ancestors are still "open" (have more siblings below them).
    print_job = function(job, prefix, is_last, visited) {
      connector <- if (is_last) "\u2514\u2500\u2500 " else "\u251c\u2500\u2500 "
      symbol    <- private$status_symbol(job$status)

      cat(sprintf("%s%s%s %s\n", prefix, connector, symbol, job$name))

      # A node can have multiple parents in a DAG — mark visited so shared
      # dependencies are only printed once.
      if (job$name %in% visited) return(visited)
      visited <- c(visited, job$name)

      child_prefix <- paste0(prefix, if (is_last) "    " else "\u2502   ")
      n <- length(job$downstream)

      for (i in seq_along(job$downstream)) {
        visited <- private$print_job(job$downstream[[i]], child_prefix, i == n, visited)
      }

      visited
    }
  ),

  public = list(
    #' @description Print the DAG rooted at all jobs with no upstream dependencies.
    #' @param orchestrator A JobOrchestrator instance.
    print_workflow = function(orchestrator) {
      jobs  <- orchestrator$jobs()
      roots <- Filter(function(j) length(j$upstream) == 0, jobs)

      if (length(roots) == 0) stop("No root jobs found — the graph may have a cycle.")

      cat("<Workflow>\n")

      visited <- character(0)
      n <- length(roots)

      for (i in seq_along(roots)) {
        visited <- private$print_job(roots[[i]], "", i == n, visited)
      }

      invisible(self)
    }
  )
)
