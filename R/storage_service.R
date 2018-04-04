storageVersion <- "2016-05-31"

getStorageCredentials <-
  function(configName = "az_config.json", ...) {
    config <- getOption("az_config")

    if (!is.null(config) && !is.null(config$storageAccount)) {
      storageAccount <- config$storageAccount
      if (is.null(storageAccount$endpointSuffix)) {
        storageAccount$endpointSuffix = "core.windows.net"
      }
      credentials <-
        StorageCredentials$new(name = storageAccount$name, key = storageAccount$key, endpointSuffix = storageAccount$endpointSuffix)
    }
    else{
      config <- rjson::fromJSON(file = paste0(getwd(), "/", configName))
      if (!is.null(config) && !is.null(config$storageAccount) && is.null(config$storageAccount$endpointSuffix)) {
        config$storageAccount$endpointSuffix = "core.windows.net"
      }
      credentials <-
        StorageCredentials$new(name = config$storageAccount$name,
                               key = config$storageAccount$key,
                               endpointSuffix = config$storageAccount$endpointSuffix)
    }

    credentials
  }

StorageCredentials <- setRefClass(
  "StorageCredentials",
  fields = list(name = "character", key = "character", endpointSuffix = "character"),
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

callStorageSas <- function(request, accountName, sasToken, endpointSuffix = "core.windows.net", ...) {
  args <- list(...)

  requestdate <- httr::http_date(Sys.time())

  url <-
    sprintf("https://%s.blob.%s%s",
            accountName,
            endpointSuffix,
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
    sprintf("https://%s.blob.%s%s",
            credentials$name,
            credentials$endpointSuffix,
            request$path)

  request
}

callStorage <- function(request, content = NULL, ...) {
  args <- list(...)

  if (!is.null(args$sasToken) && !is.null(args$accountName && !is.null(args$endpointSuffix)))  {
    response <-
      callStorageSas(request, args$accountName, args$sasToken, args$endpointSuffix, ...)
  }
  else {
    credentials <- getStorageCredentials()

    request <- prepareStorageRequest(request, credentials)
    response <- executeAzureRequest(request, ...)
  }

  extractAzureResponse(response, content)
}
