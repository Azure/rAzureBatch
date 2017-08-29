getNodeFile <- function(poolId, nodeId, filePath, ...) {
  batchCredentials <- getBatchCredentials()
  args <- list(...)

  verb <- "GET"
  if (!is.null(args$verb) == "HEAD") {
    verb <- args$verb
  }

  request <- AzureRequest$new(
    method = verb,
    path = paste0("/pools/", poolId, "/nodes/", nodeId, "/files/", filePath),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials, raw = TRUE, contentType = TRUE)
}

getTaskFile <- function(jobId, taskId, filePath, ...) {
  batchCredentials <- getBatchCredentials()
  args <- list(...)

  verb <- "GET"
  if (!is.null(args$verb) && args$verb == "HEAD") {
    verb <- args$verb
  }

  request <- AzureRequest$new(
    method = verb,
    path = paste0("/jobs/", jobId, "/tasks/", taskId, "/files/", filePath),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials, raw = TRUE)
}
