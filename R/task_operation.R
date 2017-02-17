addTask <- function(jobId, taskId = "default", ...){
  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  args <- list(...)
  .doAzureBatchGlobals <- args$envir
  argsList <- args$args
  packages <- args$packages
  argResourceFiles <- args$resourceFiles

  if(!is.null(argsList)){
    assign('argsList', argsList, .doAzureBatchGlobals)
  }

  envFile <- paste0(taskId, ".rds")
  saveRDS(.doAzureBatchGlobals, file = envFile)
  uploadData(jobId, paste0(getwd(), "/", envFile))
  file.remove(envFile)

  sasToken <- constructSas("2016-11-30", "r", "c", jobId, storageCredentials$key)

  taskPrep <- .getInstallationCommand(packages)
  rCommand <- sprintf("Rscript --vanilla --verbose $AZ_BATCH_JOB_PREP_WORKING_DIR/%s %s %s > %s.Rout", "worker.R", "$AZ_BATCH_TASK_WORKING_DIR", envFile, taskId)

  resultFile <- paste0(taskId, "-result", ".rds")
  autoUploadCommand <- sprintf("env PATH=$PATH blobxfer %s %s %s --upload --saskey $BLOBXFER_SASKEY --remoteresource result/%s", storageCredentials$name, jobId, resultFile, resultFile)
  stdoutUploadCommand <- sprintf("env PATH=$PATH blobxfer %s %s $AZ_BATCH_TASK_DIR/%s --upload --saskey $BLOBXFER_SASKEY --remoteresource %s", storageCredentials$name, jobId, "stdout.txt", paste0("stdout/", taskId, "-stdout.txt"))
  stderrUploadCommand <- sprintf("env PATH=$PATH blobxfer %s %s $AZ_BATCH_TASK_DIR/%s --upload --saskey $BLOBXFER_SASKEY --remoteresource %s", storageCredentials$name, jobId, "stderr.txt", paste0("stderr/", taskId, "-stderr.txt"))

  commands <- c("export PATH=/anaconda/envs/py35/bin:$PATH", rCommand, autoUploadCommand, stdoutUploadCommand, stderrUploadCommand)
  if(taskPrep != ""){
    commands <- c(taskPrep, commands)
  }

  resourceFiles <- list(generateResourceFile(storageCredentials$name, jobId, envFile, sasToken))

  if(!is.null(argResourceFiles)){
    resourceFiles <- c(resourceFiles, argResourceFiles)
  }

  sasToken <- constructSas("2016-11-30", "rwcl", "c", jobId, storageCredentials$key)
  sasQuery <- generateSasUrl(sasToken)

  setting = list(name = "BLOBXFER_SASKEY",
                 value = sasQuery)

  body = list(id = taskId,
              commandLine = .linuxWrapCommands(commands),
              runElevated = TRUE,
              resourceFiles = resourceFiles,
              environmentSettings = list(setting))

  size <- nchar(rjson::toJSON(body, method="C"))

  headers <- c()
  headers['Content-Length'] <- size
  headers['Content-Type'] <- "application/json;odata=minimalmetadata"

  request <- AzureRequest$new(
    method = "POST",
    path = paste0("/jobs/", jobId, "/tasks"),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, body)
}

addTaskMerge <- function(jobId, taskId = "default", dependsOn, ...){
  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  args <- list(...)
  .doAzureBatchGlobals <- args$envir
  argsList <- args$args
  packages <- args$packages

  if(!is.null(argsList)){
    assign('argsList', argsList, .doAzureBatchGlobals)
  }

  envFile <- paste0(taskId, ".rds")
  saveRDS(.doAzureBatchGlobals, file = envFile)
  uploadData(jobId, paste0(getwd(), "/", envFile))
  file.remove(envFile)

  sasToken <- constructSas("2016-11-30", "r", "c", jobId, storageCredentials$key)

  taskPrep <- .getInstallationCommand(packages)
  rCommand <- sprintf("Rscript --vanilla --verbose $AZ_BATCH_JOB_PREP_WORKING_DIR/%s %s %s %s %s > %s.Rout", "merger.R", "$AZ_BATCH_TASK_WORKING_DIR", envFile, length(dependsOn), jobId, taskId)

  resultFile <- paste0(taskId, "-result", ".rds")
  autoUploadCommand <- sprintf("env PATH=$PATH blobxfer %s %s %s --upload --saskey $BLOBXFER_SASKEY --remoteresource result/%s", storageCredentials$name, jobId, resultFile, resultFile)
  downloadCommand <- sprintf("env PATH=$PATH blobxfer %s %s %s --download --saskey $BLOBXFER_SASKEY --remoteresource . --include result/*.rds", storageCredentials$name, jobId, "$AZ_BATCH_TASK_WORKING_DIR")

  commands <- c("export PATH=/anaconda/envs/py35/bin:$PATH", downloadCommand, rCommand, autoUploadCommand)
  if(taskPrep != ""){
    commands <- c(taskPrep, commands)
  }

  resourceFiles <- list(generateResourceFile(storageCredentials$name, jobId, envFile, sasToken))

  sasToken <- constructSas("2016-11-30", "rwcl", "c", jobId, storageCredentials$key)
  sasQuery <- generateSasUrl(sasToken)

  setting = list(name = "BLOBXFER_SASKEY",
                 value = sasQuery)

  body = list(id = taskId,
              commandLine = .linuxWrapCommands(commands),
              runElevated = TRUE,
              resourceFiles = resourceFiles,
              environmentSettings = list(setting),
              dependsOn = list(taskIds = dependsOn))

  size <- nchar(rjson::toJSON(body, method="C"))

  headers <- c()
  headers['Content-Length'] <- size
  headers['Content-Type'] <- "application/json;odata=minimalmetadata"

  request <- AzureRequest$new(
    method = "POST",
    path = paste0("/jobs/", jobId, "/tasks"),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, body)
}

listTask <- function(jobId, ...){
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/jobs/", jobId, "/tasks"),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}

waitForTasksToComplete <- function(jobId, timeout, ...){
  print("Waiting for tasks to complete. . .")

  args <- list(...)
  progress <- args$progress
  numOfTasks <- args$tasks

  pb <- txtProgressBar(min = 0, max = numOfTasks, style = 3)

  timeToTimeout <- Sys.time() + timeout

  while(Sys.time() < timeToTimeout){
    tasks <- listTask(jobId)

    taskStates <- lapply(tasks$value, function(x) x$state != "completed")
    count <- 0
    for(i in 1:length(taskStates)){
      if(taskStates[[i]] == FALSE){
        count <- count + 1
      }
    }

    setTxtProgressBar(pb, count)

    if(all(taskStates == FALSE)){
      return(0);
    }

    Sys.sleep(10)
  }

  stop("A timeout has occurred when waiting for tasks to complete.")
}
