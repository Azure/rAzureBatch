storageVersion <- "2016-05-31"

getStorageCredentials <-
  function(configName = "az_config.json", ...) {
    config <- getOption("az_config")

    if (!is.null(config) && !is.null(config$storageAccount)) {
      storageAccount <- config$storageAccount
      credentials <-
        StorageCredentials$new(name = storageAccount$name, key = storageAccount$key)
    }
    else{
      config <- rjson::fromJSON(file = paste0(getwd(), "/", configName))
      credentials <-
        StorageCredentials$new(name = config$storageAccount$name,
                               key = config$storageAccount$key)
    }

    credentials
  }

StorageCredentials <- setRefClass(
  "StorageCredentials",
  fields = list(name = "character", key = "character"),
  methods = list(
    signString = function(x) {
      undecodedKey <- RCurl::base64Decode(key, mode = "raw")
      RCurl::base64(digest::hmac(
        key = undecodedKey,
        object = enc2utf8(x),
        algo = "sha256",
        raw = TRUE
      ))
    }
  )
)

StorageServiceClient <- R6::R6Class(
  inherit = AzureServiceClient,
  "StorageServiceClient",
  public = list(
    blobOperations = NULL,
    containerOperations = NULL,
    apiVersion = "2016-05-31",
    verbose = FALSE,
    initialize = function(url = NA, authentication = NA) {
      self$url <- url
      self$authentication <- authentication
      self$blobOperations <- BlobOperations$new(self, url, authentication, apiVersion)
      self$containerOperations <- ContainerOperations$new(self, url, authentication, apiVersion)
    },
    execute = function(request) {
      requestdate <- httr::http_date(Sys.time())
      request$headers['x-ms-date'] <- requestdate
      request$headers['x-ms-version'] <- self$apiVersion

      request$headers['User-Agent'] <-
        paste0("rAzureBatch/",
               packageVersion("rAzureBatch"))

      authorizationHeader
      if (self$authentication$getClassName() == "SharedKeyCredentials") {
        authorizationHeader <- self$authentication$signRequest(request,
                                                               "x-ms-")
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

callStorageSas <- function(request, accountName, sasToken, ...) {
  args <- list(...)

  requestdate <- httr::http_date(Sys.time())

  url <-
    sprintf("https://%s.blob.core.windows.net%s",
            accountName,
            request$path)

  headers <- request$headers
  headers['x-ms-date'] <- requestdate
  headers['x-ms-version'] <- storageVersion

  request$query <- append(request$query, sasToken)

  requestHeaders <- httr::add_headers(.headers = headers)

  body <- NULL
  httpTraffic <- NULL
  write <- NULL

  httpTrafficFlag <- getOption("azureHttpTraffic")
  verbose <- getOption("azureVerbose")

  if (!is.null(verbose) && verbose) {
    print(headers)
    print(paste0("URL: ", url))
  }

  if (!is.null(httpTrafficFlag) && httpTrafficFlag) {
    httpTraffic <- httr::verbose()
  }

  if (length(request$body) != 0) {
    body <- request$body
  }

  if (methods::hasArg("uploadFile")) {
    body <- args$uploadFile
  }

  if (methods::hasArg("body")) {
    body <- args$body
  }

  if (!is.null(args$write)) {
    write <- args$write
  }

  response <- httr::VERB(
    request$method,
    url,
    httpTraffic,
    write,
    query = request$query,
    config = requestHeaders,
    body = body
  )

  response
}

prepareStorageRequest <- function(request, credentials) {
  requestdate <- httr::http_date(Sys.time())
  request$headers['x-ms-date'] <- requestdate
  request$headers['x-ms-version'] <- storageVersion

  authorizationHeader <-
    signAzureRequest(request, credentials$name, credentials$key, 'x-ms-')

  request$headers['Authorization'] <- authorizationHeader
  request$headers['User-Agent'] <-
    paste0("rAzureBatch/",
           packageVersion("rAzureBatch"))

  request$url <-
    sprintf("https://%s.blob.core.windows.net%s",
            credentials$name,
            request$path)

  request
}

callStorage <- function(request, content = NULL, ...) {
  args <- list(...)

  if (!is.null(args$sasToken) && !is.null(args$accountName))  {
    response <-
      callStorageSas(request, args$sasToken, args$accountName, ...)
  }
  else {
    credentials <- getStorageCredentials()

    request <- prepareStorageRequest(request, credentials)
    response <- executeAzureRequest(request, ...)
  }

  extractAzureResponse(response, content)
}
