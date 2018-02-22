PoolOperations <- R6::R6Class("PoolOperations",
  public = list(
    path = "/pools",
    url = NULL,
    authentication = NULL,
    client = NULL,
    apiVersion = NULL,
    initialize = function(client = NA, url = NA, authentication = NA, apiVersion) {
      self$url <- url
      self$authentication <- authentication
      self$client <- client
      self$apiVersion <- apiVersion
    },
    addPool = function(poolId, vmSize, content = "parsed", authType = "SharedKey", ...) {
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

      metadata <- NULL
      if (!is.null(args$metadata)) {
        metadata <- args$metadata
      }

      stopifnot(grepl("^([a-zA-Z0-9]|[-]|[_]){1,64}$", poolId))

      body <- list(
        metadata = metadata,
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
        query = list("api-version" = self$apiVersion),
        headers = headers,
        body = body
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    getPool = function(poolId, content = "parsed") {
      request <- AzureRequestV2$new(
        method = "GET",
        path = paste0(self$path, "/", poolId),
        query = list("api-version" = self$apiVersion)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    deletePool = function(poolId, content = "parsed") {
      headers <- c()
      headers['Content-Length'] <- '0'

      request <- AzureRequest$new(
        method = "DELETE",
        path = paste0(self$path, "/", poolId),
        query = list("api-version" = self$apiVersion),
        headers = headers
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    resizePool = function(poolId, content = "parsed", ...) {
      args <- list(...)

      autoscaleFormula <- ""
      if (!is.null(args$autoscaleFormula)) {
        autoscaleFormula <- args$autoscaleFormula
      }

      autoscaleInterval <- "PT5M"
      if (!is.null(args$autoscaleInterval)) {
        autoscaleInterval <- args$autoscaleInterval
      }

      body <- list("autoScaleFormula" = autoscaleFormula,
                   "autoScaleEvaluationInterval" = autoscaleInterval)
      size <-
        nchar(jsonlite::toJSON(body, method = "C", auto_unbox = TRUE))

      headers <- character()
      headers['Content-Type'] <-
        'application/json;odata=minimalmetadata'
      headers['Content-Length'] <- size

      request <- AzureRequest$new(
        method = "POST",
        path = paste0(self$path, "/", poolId, "/enableautoscale"),
        query = list("api-version" = self$apiVersion),
        headers = headers,
        body = body
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    listPools = function(query = list(), content = "parsed") {
      request <- AzureRequest$new(
        method = "GET",
        path = paste0("/pools"),
        query = append(list("api-version" = self$apiVersion), query)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    listPoolNodes = function(poolId, content = "parsed", ...) {
      request <- AzureRequest$new(
        method = "GET",
        path = paste0(self$path, "/", poolId, "/nodes"),
        query = list("api-version" = self$apiVersion)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    rebootNode = function(poolId, nodeId, content = "response", ...) {
      headers <- c()
      headers['Content-Length'] <- '0'

      request <- AzureRequest$new(
        method = "POST",
        path = paste0(self$path, "/", poolId, "/nodes/", nodeId, "/reboot"),
        query = list("api-version" = apiVersion),
        headers = headers
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    reimageNode = function(poolId, nodeId, content = "parsed", ...) {
      request <- AzureRequest$new(
        method = "POST",
        path = paste0(self$path, "/", poolId, "/nodes/", nodeId, "/reimage"),
        query = list("api-version" = self$apiVersion)
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    }
  )
)
