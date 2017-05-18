#' Add a task to the specified job.
#'
#' @param jobId The id of the job to which the task is to be added.
#' @param taskId A string that uniquely identifies the task within the job.
#' @param ... Further named parameters
#' \itemize{
#'  \item{"resourceFiles"}: {A list of files that the Batch service will download to the compute node before running the command line.}
#'  \item{"args"}: {Arguments in the foreach parameters that will be used for the task running.}
#'  \item{"packages"}: {A list of packages that the Batch service will download to the compute node.}
#'  \item{"envir"}: {The R environment that the task will run under.}
#'}
#' @return The response of task
#' @examples
#' addTask(job-001, task-001)
addTask <- function(jobId, taskId = "default", ...){
  batchCredentials <- getBatchCredentials()
  storageCredentials <- getStorageCredentials()

  args <- list(...)
  environmentSettings <- args$environmentSettings
  resourceFiles <- args$resourceFiles
  commandLine <- args$commandLine
  dependsOn <- args$dependsOn
  outputFiles <- args$outputFiles

  if(is.null(commandLine)){
    stop("Task requires a command line.")
  }

  body = list(id = taskId,
              commandLine = commandLine,
              userIdentity = list(
                autoUser = list(
                  scope = "pool",
                  elevationLevel = "admin"
                )
              ),
              resourceFiles = resourceFiles,
              environmentSettings = environmentSettings,
              dependsOn = dependsOn,
              outputFiles = outputFiles)

  body <- Filter(length, body)

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

  if(is.null(args$tasks)){
    stop("The number of tasks was not initialized.")
  }

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
