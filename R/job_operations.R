addJob <- function(jobId, ...){
  headers <- character()
  args <- list(...)
  config <- args$config
  resourceFiles <- args$resourceFiles

  raw <- FALSE
  if(!is.null(args$raw)){
    raw <- args$raw
  }

  pool <- config$batchAccount$pool
  stopifnot(!is.null(pool))

  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  packages <- args$packages
  commands <- c("ls")

  body = list(id=jobId,
              poolInfo=list("poolId" = pool$name),
              jobPreparationTask = list(
                commandLine = .linuxWrapCommands(commands),
                userIdentity = list(
                  autoUser = list(
                    scope = "task",
                    elevationLevel = "admin"
                  )
                ),
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

  callBatchService(request, batchCredentials, body, contentType = raw)
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
