JobOperations <- R6::R6Class("JobOperations",
  public = list(
    path = "/jobs",
    url = NULL,
    authentication = NULL,
    client = NULL,
    apiVersion = NULL,
    initialize = function(client = NA, url = NA, authentication = NA, apiVersion) {
      self$url <- url
      self$authentication <- authentication
      self$client <- client
      self$apiVersion <- apiVersion
    },
    addJob = function(jobId,
                       poolInfo,
                       jobPreparationTask = NULL,
                       usesTaskDependencies = FALSE,
                       content = "parsed",
                       metadata = NULL,
                       ...) {
      body <- list(
        id = jobId,
        poolInfo = poolInfo,
        jobPreparationTask = jobPreparationTask,
        usesTaskDependencies = usesTaskDependencies,
        metadata = metadata
      )

      body <- Filter(length, body)

      size <-
        nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

      headers <- character()
      headers['Content-Length'] <- size
      headers['Content-Type'] <-
        'application/json;odata=minimalmetadata'

      request <- AzureRequest$new(
        method = "POST",
        path = self$path,
        query = list("api-version" = self$apiVersion),
        headers = headers,
        body = body
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    getJob = function(jobId, content = "parsed") {
      request <- AzureRequest$new(
        method = "GET",
        path = paste0(self$path, "/", jobId),
        query = list("api-version" = self$apiVersion)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    deleteJob = function(jobId, content = "parsed") {
      headers <- c()
      headers['Content-Length'] <- '0'

      request <- AzureRequest$new(
        method = "DELETE",
        path = paste0(self$path, "/", jobId),
        query = list("api-version" = self$apiVersion),
        headers = headers
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    #' Updates the properties of the specified job.
    #'
    #' @param jobId The id of the job.
    #' @param ... Additional parameters to customize update the job
    #' @return The request to the Batch service was successful.
    #' @export
    updateJob = function(jobId, content = "parsed", ...) {
      headers <- character()

      body <- list(onAllTasksComplete = "terminatejob")
      size <-
        nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

      headers['Content-Length'] <- size
      headers['Content-Type'] <-
        'application/json;odata=minimalmetadata'

      request <- AzureRequest$new(
        method = "PATCH",
        path = paste0(self$path, "/", jobId),
        query = list("api-version" = self$apiVersion),
        headers = headers,
        body = body
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    listJobs = function(query = list(), content = "parsed") {
      request <- AzureRequest$new(
        method = "GET",
        path = self$path,
        query = append(list("api-version" = self$apiVersion), query)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    getJobPreparationStatus = function(jobId, content = "parsed", ...) {
      args <- list(...)
      query = list("api-version" = self$apiVersion)

      if (methods::hasArg("filter")) {
        query["$filter"] <- args$filter
      }

      if (methods::hasArg("select")) {
        query["$select"] <- args$select
      }

      if (methods::hasArg("maxresults")) {
        query["maxresults"] <- args$maxresults
      }

      request <- AzureRequest$new(
        method = "GET",
        path = paste0(self$path, "/", jobId, "/jobpreparationandreleasetaskstatus"),
        query = query
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    #' Gets job task counts by job state.
    #'
    #' @param jobId The id of the job.
    #'
    #' @return A response containing the task counts of different states.
    #' @examples
    #' \dontrun{
    #' getJobTaskCounts(job-001)
    #' }
    #' @export
    getJobTaskCounts = function(jobId, content = "parsed") {
      request <- AzureRequest$new(
        method = "GET",
        path = paste0(self$path, "/", jobId, "/taskcounts"),
        query = list("api-version" = apiVersion)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    terminateJob = function(jobId, content = "response"){
      headers <- c()
      headers['Content-Length'] <- '0'

      request <- AzureRequest$new(
        method = "POST",
        path = paste0(self$path, "/", jobId, "/terminate"),
        query = list("api-version" = apiVersion),
        headers = headers
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    }
  )
)
