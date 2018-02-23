apiVersion <- "2017-06-01.5.1"

getBatchCredentials <- function(configPath = "az_config.json", ...) {
  config <- getOption("az_config")

  if (!is.null(config) && !is.null(config$batchAccount)) {
    batchAccount <- config$batchAccount

    BatchCredentials$new(name = batchAccount$name,
                         key = batchAccount$key,
                         url = batchAccount$url)
  }
  else if (!is.null(config) && !is.null(config$ServicePrincipal)) {
    servicePrincipal <- config$ServicePrincipal

    servicePrincipal
  }
  else {
    config <- rjson::fromJSON(file = configPath)

    BatchCredentials$new(
      name = config$batchAccount$name,
      key = config$batchAccount$key,
      url = config$batchAccount$url
    )
  }
}

BatchServiceClient <- R6::R6Class(
  inherit = AzureServiceClient,
  "BatchServiceClient",
  public = list(
    poolOperations = NULL,
    jobOperations = NULL,
    taskOperations = NULL,
    fileOperations = NULL,
    apiVersion = "2017-06-01.5.1",
    verbose = FALSE,
    initialize = function(url = NA, authentication = NA) {
      self$url <- url
      self$authentication <- authentication
      self$poolOperations <- PoolOperations$new(self, url, authentication, apiVersion)
      self$jobOperations <- JobOperations$new(self, url, authentication, apiVersion)
      self$taskOperations <- TaskOperations$new(self, url, authentication, apiVersion)
      self$fileOperations <- FileOperations$new(self, url, authentication, apiVersion)
    },
    execute = function(request) {
      requestdate <- httr::http_date(Sys.time())
      request$headers['ocp-date'] <- requestdate
      request$headers['User-Agent'] <-
        paste0(
          "rAzureBatch/",
          packageVersion("rAzureBatch"),
          " ",
          "doAzureParallel/",
          packageVersion("doAzureParallel")
        )

      if (self$authentication$getClassName() == "SharedKeyCredentials") {
        authorizationHeader <- self$authentication$signRequest(request,
                                                   "ocp-")
      }
      # Service Principal Path
      else {
        authorizationHeader <- self$authentication$checkAccessToken(
          'https://batch.core.windows.net/')
      }

      request$headers['Authorization'] <- authorizationHeader
      url <- paste0(self$url, request$path)

      executeResponse(url, request)
    }
  )
)
