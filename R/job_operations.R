JobOperations <- R6::R6Class("PoolOperations",
  public = list(
    path = "/pools",
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
      batchCredentials <- getBatchCredentials()

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
        path = "/jobs",
        query = list("api-version" = apiVersion),
        headers = headers,
        body = body
      )

      callBatchService(request, batchCredentials, content)
    },
    getJob = function(jobId, content = "parsed") {
      batchCredentials <- getBatchCredentials()

      request <- AzureRequest$new(
        method = "GET",
        path = paste0("/jobs/", jobId),
        query = list("api-version" = apiVersion)
      )

      callBatchService(request, batchCredentials, content)
    },
    deleteJob = function(jobId, content = "parsed") {
      batchCredentials <- getBatchCredentials()

      headers <- c()
      headers['Content-Length'] <- '0'

      request <- AzureRequest$new(
        method = "DELETE",
        path = paste0("/jobs/", jobId),
        query = list("api-version" = apiVersion),
        headers = headers
      )

      callBatchService(request, batchCredentials, content)
    },
    #' Updates the properties of the specified job.
    #'
    #' @param jobId The id of the job.
    #' @param ... Additional parameters to customize update the job
    #' @return The request to the Batch service was successful.
    #' @export
    updateJob = function(jobId, content = "parsed", ...) {
      batchCredentials <- getBatchCredentials()

      headers <- character()

      body <- list(onAllTasksComplete = "terminatejob")
      size <-
        nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

      headers['Content-Length'] <- size
      headers['Content-Type'] <-
        'application/json;odata=minimalmetadata'

      request <- AzureRequest$new(
        method = "PATCH",
        path = paste0("/jobs/", jobId),
        query = list("api-version" = apiVersion),
        headers = headers,
        body = body
      )

      callBatchService(request, batchCredentials, content)
    },
    listJobs = function(query = list(), content = "parsed") {
      batchCredentials <- getBatchCredentials()

      request <- AzureRequest$new(
        method = "GET",
        path = paste0("/jobs"),
        query = append(list("api-version" = apiVersion), query)
      )

      callBatchService(request, batchCredentials, content)
    },
    getJobPreparationStatus = function(jobId, content = "parsed", ...) {
      args <- list(...)
      query = list("api-version" = apiVersion)

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
        path = paste0("/jobs/", jobId, "/jobpreparationandreleasetaskstatus"),
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
      batchCredentials <- getBatchCredentials()

      request <- AzureRequest$new(
        method = "GET",
        path = paste0("/jobs/", jobId, "/taskcounts"),
        query = list("api-version" = apiVersion)
      )

      callBatchService(request, batchCredentials, content)
    },
    terminateJob = function(jobId, content = "response"){
      batchCredentials <- getBatchCredentials()

      headers <- c()
      headers['Content-Length'] <- '0'

      request <- AzureRequest$new(
        method = "POST",
        path = paste0("/jobs/", jobId, "/terminate"),
        query = list("api-version" = apiVersion),
        headers = headers
      )

      callBatchService(request, batchCredentials, content)
    }
  )
)
