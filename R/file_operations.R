getNodeFile <- function(poolId, nodeId, filePath) {
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/pools/", poolId, "/nodes/", nodeId, "/files/", filePath),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}

getTaskFile <- function(jobId, taskId, filePath) {
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/jobs/", jobId, "/tasks/", taskId, "/files/", filePath),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}
