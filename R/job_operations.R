#' Add a job to the specified pool.
#'
#' @param jobId A string that uniquely identifies the job within the account.
#' @param ... Further named parameters
#' \itemize{
#'  \item{"resourceFiles"}: {A list of files that the Batch service will download to the compute node before running the command line.}
#'  \item{"args"}: {Arguments in the foreach parameters that will be used for the task running.}
#'  \item{"packages"}: {A list of packages that the Batch service will download to the compute node.}
#'  \item{"envir"}: {The R environment that the task will run under.}
#'}
#' @return The request to the Batch service was successful.
#' @export
addJob <- function(jobId,
                   poolInfo,
                   jobPreparationTask = NULL,
                   usesTaskDependencies = FALSE,
                   raw = FALSE,
                   ...) {
  args <- list(...)

  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  body <- list(
    id = jobId,
    poolInfo = poolInfo,
    jobPreparationTask = jobPreparationTask,
    usesTaskDependencies = usesTaskDependencies
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
    headers = headers
  )

  callBatchService(request, batchCredentials, body, contentType = raw)
}

#' Gets information about the specified job.
#'
#' @param jobId The id of the job.
#'
#' @return A response containing the job.
#' @examples
#' getJob(job-001)
#' @export
getJob <- function(jobId){
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/jobs/", jobId),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}

#' Deletes a job.
#' @details Deleting a job also deletes all tasks that are part of that job, and all job statistics. This also overrides the retention period for task data; that is, if the job contains tasks which are still retained on compute nodes, the Batch services deletes those tasks' working directories and all their contents.
#' @param jobId The id of the job to delete..
#'
#' @return The request to the Batch service was successful.
#' @examples
#' deleteJob(job-001)
#' @export
deleteJob <- function(jobId){
  batchCredentials <- getBatchCredentials()

  headers <- c()
  headers['Content-Length'] <- '0'

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/jobs/", jobId),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials)
}

#' Updates the properties of the specified job.
#'
#' @param jobId The id of the job.
#' @param ... Additional parameters to customize update the job
#' @return The request to the Batch service was successful.
#' @export
updateJob <- function(jobId, ...) {
  batchCredentials <- getBatchCredentials()

  headers <- character()

  body = list(onAllTasksComplete = "terminatejob")
  size <-
    nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

  headers['Content-Length'] <- size
  headers['Content-Type'] <-
    'application/json;odata=minimalmetadata'
  request <- AzureRequest$new(
    method = "PATCH",
    path = paste0("/jobs/", jobId),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, body)
}
