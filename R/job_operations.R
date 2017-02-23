addJob <- function(jobId, ...){
  headers <- character()
  args <- list(...)
  config <- args$config

  pool <- config$batchAccount$pool
  stopifnot(!is.null(pool))

  startupFolderName <- "startup"

  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  packages <- args$packages
  commands <- c("ls")

  createContainer(jobId)
  uploadData(jobId, system.file(startupFolderName, "splitter.R", package="rAzureBatch"))
  uploadData(jobId, system.file(startupFolderName, "worker.R", package="rAzureBatch"))
  uploadData(jobId, system.file(startupFolderName, "merger.R", package="rAzureBatch"))

  sasToken <- constructSas("2016-11-30", "r", "c", jobId, storageCredentials$key)
  resourceFiles <- list(generateResourceFile(storageCredentials$name, jobId, "splitter.R", sasToken),
                        generateResourceFile(storageCredentials$name, jobId, "worker.R", sasToken),
                        generateResourceFile(storageCredentials$name, jobId, "merger.R", sasToken))

  body = list(id=jobId,
              poolInfo=list("poolId" = pool$name),
              jobPreparationTask = list(
                commandLine = .linuxWrapCommands(commands),
                runElevated = TRUE,
                waitForSuccess = TRUE,
                resourceFiles = resourceFiles,
                constraints = list(
                  maxTaskRetryCount = 2
                )
              ),
              usesTaskDependencies = TRUE)

  size <- nchar(jsonlite::toJSON(body, method="C", auto_unbox = TRUE))

  headers['Content-Length'] <- size
  headers['Content-Type'] <- 'application/json;odata=minimalmetadata'

  request <- AzureRequest$new(
    method = "POST",
    path = "/jobs",
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, body)
}

getJob <- function(jobId){
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/jobs/", jobId),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}

deleteJob <- function(jobId){
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/jobs/", jobId),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}

updateJob <- function(jobId, ...){
  batchCredentials <- getBatchCredentials()

  headers <- character()

  body = list(onAllTasksComplete = "terminatejob")
  size <- nchar(jsonlite::toJSON(body, method="C", auto_unbox = TRUE))

  headers['Content-Length'] <- size
  headers['Content-Type'] <- 'application/json;odata=minimalmetadata'
  request <- AzureRequest$new(
    method = "PATCH",
    path = paste0("/jobs/", jobId),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, body)
}
