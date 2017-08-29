addPool <- function(poolId, vmSize, content = "parsed", ...) {
  args <- list(...)

  commands <- c(
    "export PATH=/anaconda/envs/py35/bin:$PATH",
    "env PATH=$PATH pip install --no-dependencies blobxfer"
  )

  if (!is.null(args$packages)) {
    commands <- c(commands, args$packages)
  }

  autoscaleFormula <- ""
  if (!is.null(args$autoscaleFormula)) {
    autoscaleFormula <- args$autoscaleFormula
  }

  startTask <- NULL
  if (!is.null(args$startTask)) {
    startTask <- args$startTask
  }

  virtualMachineConfiguration <- NULL
  if (!is.null(args$virtualMachineConfiguration)) {
    virtualMachineConfiguration <- args$virtualMachineConfiguration
  }

  maxTasksPerNode <- ""
  if (!is.null(args$maxTasksPerNode)) {
    maxTasksPerNode <- args$maxTasksPerNode
  }

  enableAutoScale <- FALSE
  if (!is.null(args$enableAutoScale)) {
    enableAutoScale <- args$enableAutoScale
  }

  autoScaleEvaluationInterval <- "PT5M"
  if (!is.null(args$autoScaleEvaluationInterval)) {
    autoScaleEvaluationInterval <- args$autoScaleEvaluationInterval
  }

  stopifnot(grepl("^([a-zA-Z0-9]|[-]|[_]){1,64}$", poolId))

  batchCredentials <- getBatchCredentials()

  body = list(
    vmSize = vmSize,
    id = poolId,
    startTask = startTask,
    virtualMachineConfiguration = virtualMachineConfiguration,
    enableAutoScale = enableAutoScale,
    autoScaleFormula = autoscaleFormula,
    autoScaleEvaluationInterval = autoScaleEvaluationInterval,
    maxTasksPerNode = maxTasksPerNode
  )

  body <- Filter(length, body)

  size <-
    nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

  headers <- c()
  headers['Content-Length'] <- size
  headers['Content-Type'] <-
    'application/json;odata=minimalmetadata'

  request <- AzureRequest$new(
    method = "POST",
    path = "/pools",
    query = list("api-version" = apiVersion),
    headers = headers,
    body = body
  )

  callBatchService(request, batchCredentials, content)
}

deletePool <- function(poolId, content = "parsed") {
  batchCredentials <- getBatchCredentials()

  headers <- c()
  headers['Content-Length'] <- '0'

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/pools/", poolId),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, content)
}

getPool <- function(poolId, content = "parsed") {
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/pools/", poolId),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials, content)
}

resizePool <- function(poolId, content = "parsed", ...) {
  batchCredentials = getBatchCredentials()
  args = list(...)

  autoscaleFormula <- ""
  if (!is.null(args$autoscaleFormula)) {
    autoscaleFormula <- args$autoscaleFormula
  }

  body <- list("autoScaleFormula" = autoscaleFormula)
  size <-
    nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

  headers <- character()
  headers['Content-Type'] <-
    'application/json;odata=minimalmetadata'
  headers['Content-Length'] <- size

  request <- AzureRequest$new(
    method = "POST",
    path = paste0("/pools/", poolId, "/evaluateautoscale"),
    query = list("api-version" = apiVersion),
    headers = headers,
    body = body
  )

  callBatchService(request, batchCredentials, content)
}

listPoolNodes <- function(poolId, content = "parsed", ...) {
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/pools/", poolId, "/nodes"),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials, content)
}

rebootNode <- function(poolId, nodeId, content = "parsed", ...) {
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "POST",
    path = paste0("/pools/", poolId, "/nodes/", nodeId, "/reboot"),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials, content)
}

reimageNode <- function(poolId, nodeId, content = "parsed", ...) {
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "POST",
    path = paste0("/pools/", poolId, "/nodes/", nodeId, "/reimage"),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials, content)
}
