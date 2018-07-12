storageVersion <- "2016-05-31"

getStorageClient <-
  function() {
    config <- getOption("az_config")

    if (!is.null(config) && !is.null(config$storageClient)) {
      storageAccount <- config$storageAccount
      if (is.null(config$endpointSuffix)) {
        storageAccount$endpointSuffix = "core.windows.net"
      }

      return(config$storageClient)
    }

    return(NULL)
  }

getStorageCredentials <-
  function(configName = "az_config.json", ...) {
    config <- getOption("az_config")

    if (!is.null(config) && !is.null(config$storageClient)) {
      storageAccount <- config$storageAccount
      if (is.null(config$endpointSuffix)) {
        storageAccount$endpointSuffix = "core.windows.net"
      }

      credentials <- list(
        name = config$storageClient$authentication$name,
        key = config$storageClient$authentication$key,
        endpointSuffix = config$endpointSuffix
      )
    }
    else{
      config <- rjson::fromJSON(file = paste0(getwd(), "/", configName))
      if (!is.null(config) && !is.null(config$storageAccount) && is.null(config$storageAccount$endpointSuffix)) {
        config$storageAccount$endpointSuffix = "core.windows.net"
      }
      credentials <-
        StorageCredentials$new(name = config$storageAccount$name,
                               key = config$storageAccount$key)
    }

    credentials
  }

StorageServiceClient <- R6::R6Class(
  inherit = AzureServiceClient,
  "StorageServiceClient",
  public = list(
    url = NULL,
    blobOperations = NULL,
    containerOperations = NULL,
    sasToken = NULL,
    apiVersion = "2016-05-31",
    verbose = FALSE,
    initialize = function(url = NA,
                          authentication = NA,
                          sasToken = NULL) {
      self$url <- url
      self$authentication <- authentication
      self$blobOperations <- BlobOperations$new(self, authentication, apiVersion)
      self$containerOperations <- ContainerOperations$new(self, authentication, apiVersion)
      self$sasToken <- sasToken
    },
    execute = function(request) {
      requestdate <- httr::http_date(Sys.time())
      request$headers['x-ms-date'] <- requestdate
      request$headers['x-ms-version'] <- self$apiVersion

      request$headers['User-Agent'] <-
        paste0("rAzureBatch/",
               packageVersion("rAzureBatch"))

      if (is.null(self$sasToken)) {
        authorizationHeader <- self$authentication$signRequest(request,
                                                               "x-ms-")
        request$headers['Authorization'] <- authorizationHeader
      }
      else {
        if (!is.null(request$query)) {
          request$query <- append(request$query, self$sasToken)
        }
        else {
          request$query <- self$sasToken
        }
      }

      url <- sprintf("%s%s",
                     self$url,
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
