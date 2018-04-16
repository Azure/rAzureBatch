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

StorageServiceClient <- R6::R6Class(
  inherit = AzureServiceClient,
  "StorageServiceClient",
  public = list(
    blobOperations = NULL,
    containerOperations = NULL,
    apiVersion = "2016-05-31",
    verbose = FALSE,
    initialize = function(authentication = NA) {
      self$authentication <- authentication
      self$blobOperations <- BlobOperations$new(self, authentication, apiVersion)
      self$containerOperations <- ContainerOperations$new(self, authentication, apiVersion)
    },
    execute = function(request) {
      requestdate <- httr::http_date(Sys.time())
      request$headers['x-ms-date'] <- requestdate
      request$headers['x-ms-version'] <- self$apiVersion

      request$headers['User-Agent'] <-
        paste0("rAzureBatch/",
               packageVersion("rAzureBatch"))

      if (is.null(request$sasToken)) {
        authorizationHeader <- self$authentication$signRequest(request,
                                                               "x-ms-")
        request$headers['Authorization'] <- authorizationHeader
      }
      # SasToken Path
      else {
        if (!is.null(request$query)) {
          request$query <- append(request$query, request$sasToken)
        }
        else {
          request$query <- request$sasToken
        }
      }

      url <- sprintf("https://%s.blob.core.windows.net%s",
                     self$authentication$name,
                     request$path)
      self$executeRequest(url, request)
    },
    generateSasToken = function(permission,
                                sr,
                                path,
                                start = Sys.time() - 60 * 60 * 24 * 1,
                                end = Sys.time() + 60 * 60 * 24 * 2) {
      myList <- list()
      query <- c()

      startTime <- as.POSIXlt(start, "UTC", "%Y-%m-%dT%H:%M:%S")
      startTime <- paste0(strftime(startTime, "%Y-%m-%dT%H:%I:%SZ"))

      endTime <- as.POSIXlt(end, "UTC", "%Y-%m-%dT%H:%M:%S")
      endTime <- paste0(strftime(endTime, "%Y-%m-%dT%H:%I:%SZ"))

      query[signed_start] <- startTime
      query[signed_expiry] <- endTime
      query[signed_permission] <- permission
      query[signed_version] <- storageVersion
      query[signed_resource] <- sr


      myList[[signed_version]] <- storageVersion
      myList[[signed_resource]] <- sr
      myList[[signed_start]] <- startTime
      myList[[signed_expiry]] <- endTime
      myList[[signed_permission]] <- permission

      canonicalizedResource <-
        paste0("/blob/", self$authentication$name, "/", path, "\n")

      stringToSign <-
        paste0(getValueFromQuery(query, signed_permission))
      stringToSign <-
        paste0(stringToSign, getValueFromQuery(query, signed_start))
      stringToSign <-
        paste0(stringToSign, getValueFromQuery(query, signed_expiry))
      stringToSign <- paste0(stringToSign, canonicalizedResource)

      stringToSign <-
        paste0(stringToSign, getValueFromQuery(query, signed_identifier))
      stringToSign <-
        paste0(stringToSign, getValueFromQuery(query, signed_ip))
      stringToSign <-
        paste0(stringToSign, getValueFromQuery(query, signed_protocol))
      stringToSign <-
        paste0(stringToSign, getValueFromQuery(query, signed_version))

      stringToSign <-
        paste0(stringToSign,
               getValueFromQuery(query, signed_cache_control))
      stringToSign <-
        paste0(stringToSign,
               getValueFromQuery(query, signed_content_disposition))
      stringToSign <-
        paste0(stringToSign,
               getValueFromQuery(query, signed_content_encoding))
      stringToSign <-
        paste0(stringToSign,
               getValueFromQuery(query, signed_content_language))
      stringToSign <-
        paste0(stringToSign,
               getValueFromQuery(query, signed_content_type))

      stringToSign <- substr(stringToSign, 1, nchar(stringToSign) - 1)

      config <- getOption("az_config")
      if (!is.null(config) && !is.null(config$settings)) {
        verbose <- config$settings$verbose
      }
      else{
        verbose <- getOption("verbose")
      }

      if (verbose) {
        print(stringToSign)
      }

      undecodedKey <-
        RCurl::base64Decode(self$authentication$key, mode = "raw")
      encString <- RCurl::base64(digest::hmac(
        key = undecodedKey,
        object = enc2utf8(stringToSign),
        algo = "sha256",
        raw = TRUE
      ))

      myList[[signed_signature]] <- encString
      myList
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

  if (!is.null(args$sasToken) && !is.null(args$accountName) && !is.null(args$endpointSuffix))  {
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
