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

  if (!is.null(args$sasToken) && !is.null(args$accountName))  {
    if (is.null(args$endpointSuffix)) {
      args$endpointSuffix <- "core.windows.net"
    }

    response <-
      callStorageSas(request,
                     args$accountName,
                     args$sasToken,
                     args$endpointSuffix,
                     ...)

    response <- extractAzureResponse(response, content)
  }
  else {
    #request <- prepareStorageRequest(request, credentials)
    config <- getOption("az_config")
    config$storageClient$execute(request)

    response <- config$storageClient$extractAzureResponse(response, content)
  }

  return(response)
}

callStorageSas <- function(request, accountName, sasToken, ...) {
  args <- list(...)

  requestdate <- httr::http_date(Sys.time())
  endpointSuffix = "core.windows.net"
  if (!is.null(args$endpointSuffix)) {
    endpointSuffix <- args$endpointSuffix
  }

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
