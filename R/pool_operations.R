addPool <- function(poolId, vmSize, ...){
  args <- list(...)

  raw <- FALSE
  if(!is.null(args$raw)){
    raw <- args$raw
  }

  packages <- c()
  if(!is.null(args$packages)){
    packages <- args$packages
  }

  autoscaleFormula <- ""
  if(!is.null(args$autoscaleFormula)){
    autoscaleFormula <- args$autoscaleFormula
  }

  maxTasksPerNode <- ""
  if(!is.null(args$maxTasksPerNode)){
    maxTasksPerNode <- args$maxTasksPerNode
  }

  stopifnot(grepl("^([a-zA-Z0-9]|[-]|[_]){1,64}$", poolId))

  batchCredentials <- getBatchCredentials()

  commands <- c("sed -i -e 's/Defaults    requiretty.*/ #Defaults    requiretty/g' /etc/sudoers",
                "export PATH=/anaconda/envs/py35/bin:$PATH",
                "sudo env PATH=$PATH pip install --no-dependencies blobxfer")

  commands <- paste0(.linuxWrapCommands(commands), ";", .getGithubInstallationCommand(packages))

  body = list(vmSize = vmSize,
              id = poolId,
              startTask = list(
                commandLine = commands,
                userIdentity = list(
                  autoUser = list(
                    scope = "task",
                    elevationLevel = "admin"
                  )
                ),
                waitForSuccess = TRUE
              ),
              virtualMachineConfiguration = list(
                imageReference = list(publisher = "microsoft-ads",
                                    offer = "linux-data-science-vm",
                                    sku = "linuxdsvm",
                                    version = "latest"),
                nodeAgentSKUId ="batch.node.centos 7"),
              enableAutoScale = TRUE,
              autoScaleFormula = autoscaleFormula,
              autoScaleEvaluationInterval = "PT5M",
              maxTasksPerNode = maxTasksPerNode)

  size <- nchar(jsonlite::toJSON(body, method="C", auto_unbox = TRUE))

  headers <- c()
  headers['Content-Length'] <- size
  headers['Content-Type'] <- 'application/json;odata=minimalmetadata'

  request <- AzureRequest$new(
    method = "POST",
    path = "/pools",
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials, body, contentType = raw)
}

deletePool <- function(poolId = ""){
  batchCredentials <- getBatchCredentials()

  headers <- c()
  headers['Content-Length'] <- '0'

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/pools/", poolId),
    query = list("api-version" = apiVersion),
    headers = headers
  )

  callBatchService(request, batchCredentials)
}

getPool <- function(poolId){
  batchCredentials = getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/pools/", poolId),
    query = list("api-version" = apiVersion))

  callBatchService(request, batchCredentials)
}

resizePool <- function(poolId, ...){
  batchCredentials = getBatchCredentials()
  args = list(...)

  autoscaleFormula <- ""
  if(!is.null(args$autoscaleFormula)){
    autoscaleFormula <- .getFormula(args$autoscaleFormula)
  }

  autoscaleInterval <- ""
  if(!is.null(args$autoscaleInterval)){
    autoscaleFormula <- .getFormula(args$autoscaleInterval)
  }

  body <- list("autoScaleFormula" = autoscaleFormula)
  size <- nchar(jsonlite::toJSON(body, method="C", auto_unbox = TRUE))

  headers <- character()
  headers['Content-Type'] <- 'application/json;odata=minimalmetadata'
  headers['Content-Length'] <- size

  request <- AzureRequest$new(
    method = "POST",
    path = paste0("/pools/", poolId, "/evaluateautoscale"),
    query = list("api-version" = apiVersion),
    headers = headers)

  callBatchService(request, batchCredentials, body)
}

listPoolNodes <- function(poolId, ...){
  batchCredentials <- getBatchCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/pools/", poolId, "/nodes"),
    query = list("api-version" = apiVersion)
  )

  callBatchService(request, batchCredentials)
}
