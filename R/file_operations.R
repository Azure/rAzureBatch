getNodeFile <-
  function(poolId,
           nodeId,
           filePath,
           content = "parsed",
           downloadPath = NULL,
           overwrite = FALSE,
           ...) {
    batchCredentials <- getBatchCredentials()
    args <- list(...)

    verb <- "GET"
    if (!is.null(args$verb) && args$verb == "HEAD") {
      verb <- args$verb
    }

    if (!is.null(downloadPath)) {
      write <- httr::write_disk(downloadPath, overwrite)
    }
    else {
      write <- httr::write_memory()
    }

    progress <- NULL
    if (!is.null(args$progress)) {
      progress <- args$progress
    }

    request <- AzureRequest$new(
      method = verb,
      path = paste0("/pools/", poolId, "/nodes/", nodeId, "/files/", filePath),
      query = list("api-version" = apiVersion)
    )

    callBatchService(request,
                     batchCredentials,
                     content,
                     write = write,
                     progress = progress,
                     ...)
  }

getTaskFile <-
  function(jobId,
           taskId,
           filePath,
           content = "parsed",
           downloadPath = NULL,
           overwrite = FALSE,
           ...) {
    batchCredentials <- getBatchCredentials()
    args <- list(...)

    verb <- "GET"
    if (!is.null(args$verb) && args$verb == "HEAD") {
      verb <- args$verb
    }

    progress <- NULL
    if (!is.null(args$progress)) {
      progress <- args$progress
    }

    if (!is.null(downloadPath)) {
      write <- httr::write_disk(downloadPath, overwrite)
    }
    else {
      write <- httr::write_memory()
    }

    request <- AzureRequest$new(
      method = verb,
      path = paste0("/jobs/", jobId, "/tasks/", taskId, "/files/", filePath),
      query = list("api-version" = apiVersion)
    )

    callBatchService(request,
                     batchCredentials,
                     content,
                     write = write,
                     progress = progress,
                     ...)
  }
