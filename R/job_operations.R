addJob <- function(jobId, poolId, splitter, expr, merger, container, inputFile, configName, ...){
  headers <- character()

  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  commands <- c(.getInstallationCommand(configName))

  my_env = as.environment(as.list(.GlobalEnv, all.names = TRUE))
  my_env[["SPLITTER"]] <- splitter
  my_env[["MERGER"]] <- merger
  my_env[["WORKER"]] <- expr

  envFileName <- "environment.RData"
  saveRDS(my_env, file = envFileName)

  sasToken <- constructSas("2016-11-30", "rwcl", "c", container, storageCredentials$key)

  uploadData(container, paste0(getwd(), "/", envFileName), sasToken)

  resourceFile <- generateSasUrl(storageCredentials$name, container, envFileName, sasToken)

  uploadData(container, system.file("startup", "splitter.R", package="rAzureBatch"), sasToken)

  body = list(id=jobId,
              poolInfo=list("poolId"=poolId),
              jobPreparationTask = list(
                commandLine = .linuxWrapCommands(commands),
                runElevated = TRUE,
                resourceFiles <- list(resourceFile)
              ))

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

  addTask(jobId, container, inputFile)
}

addJob <- function(jobId, ...){
  headers <- character()
  args <- list(...)
  config <- args$config

  pool <- config$batchAccount$pool
  stopifnot(!is.null(pool))

  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  packages <- args$packages
  packageVersion <- "AzureBatch_0.1.3.tar.gz"
  commands <- c("sed -i -e 's/Defaults    requiretty.*/ #Defaults    requiretty/g' /etc/sudoers",
                "export PATH=/anaconda/envs/py35/bin:$PATH",
                sprintf("sudo R CMD INSTALL $AZ_BATCH_JOB_PREP_WORKING_DIR/%s", packageVersion))

  createContainer(jobId)
  uploadData(jobId, system.file("startup", "splitter.R", package="rAzureBatch"))
  uploadData(jobId, system.file("startup", "worker.R", package="rAzureBatch"))
  uploadData(jobId, system.file("startup", "merger.R", package="rAzureBatch"))
  uploadData(jobId, system.file("startup", packageVersion, package="rAzureBatch"))

  sasToken <- constructSas("2016-11-30", "r", "c", jobId, storageCredentials$key)
  resourceFiles <- list(generateResourceFile(storageCredentials$name, jobId, "splitter.R", sasToken),
                        generateResourceFile(storageCredentials$name, jobId, "worker.R", sasToken),
                        generateResourceFile(storageCredentials$name, jobId, "merger.R", sasToken),
                        generateResourceFile(storageCredentials$name, jobId, packageVersion, sasToken))

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
