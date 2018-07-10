AzureRequest <- setRefClass(
  "AzureRequest",
  fields = list(
    method = "character",
    path = "character",
    headers = "character",
    query = "list",
    body = "list",
    url = "character"
  ),

  methods = list(
    signString = function(x, key) {
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


signAzureRequest <- function(request, resource, key, prefix) {
  headers <- request$headers
  canonicalizedHeaders <- ""

  systemLocale <- Sys.getlocale(category = "LC_COLLATE")
  Sys.setlocale("LC_COLLATE", "C")

  for (name in sort(names(headers))) {
    if (grepl(prefix, name)) {
      canonicalizedHeaders <-
        paste0(canonicalizedHeaders, name, ":", headers[name], "\n")
    }
  }

  canonicalizedResource <- paste0("/", resource, request$path, "\n")
  if (!is.null(names(request$query))) {
    for (name in sort(names(request$query))) {
      canonicalizedResource <-
        paste0(canonicalizedResource, name, ":", request$query[[name]], "\n")
    }
  }

  canonicalizedResource <-
    substr(canonicalizedResource, 1, nchar(canonicalizedResource) - 1)

  stringToSign <- createSignature(request$method, request$headers)
  stringToSign <- paste0(stringToSign, canonicalizedHeaders)
  stringToSign <- paste0(stringToSign, canonicalizedResource)

  # sign the request
  authorizationString <-
    paste0("SharedKey ",
           resource,
           ":",
           request$encryptSignature(stringToSign, key))

  Sys.setlocale("LC_COLLATE", systemLocale)
  return(authorizationString)
}

executeAzureRequest <- function(request, ...) {
  args <- list(...)

  body <- NULL
  httpTraffic <- NULL
  write <- NULL
  progressBar <- NULL

  httpTrafficFlag <- getOption("azureHttpTraffic")

  if (length(request$body) != 0) {
    body <- request$body
  }

  if (methods::hasArg("uploadFile")) {
    body <- args$uploadFile
  }

  if (methods::hasArg("body")) {
    body <- args$body
  }

  if (methods::hasArg("write")) {
    write <- args$write
  }

  if (methods::hasArg("progress") && args$progress) {
    progressBar <- httr::progress()
  }

  if (!is.null(httpTrafficFlag) && httpTrafficFlag) {
    httpTraffic <- httr::verbose()
  }

  requestHeaders <- httr::add_headers(request$headers)

  # Execute request with http method
  if (request$method == "GET" ||
      request$method == "POST" ||
      request$method == "DELETE" ||
      request$method == "PUT" ||
      request$method == "PATCH") {
    httr::VERB(
      request$method,
      request$url,
      config = requestHeaders,
      body = body,
      query = request$query,
      encode = "json",
      write,
      httpTraffic,
      progressBar
    )
  }
  else if (request$method == "HEAD") {
    httr::HEAD(
      request$url,
      config = requestHeaders,
      body = body,
      query = request$query,
      encode = "json",
      write,
      httpTraffic,
      progressBar
    )
  }
  else {
    stop(
      sprintf(
        "This HTTP Verb is not found: %s - Please try again with GET, POST, HEAD, PUT, PATCH or DELETE",
        request$method
      )
    )
  }
}

extractAzureResponse <- function(response, content) {
  if (is.null(content)) {
    httr::content(response, encoding = "UTF-8")
  }
  else if (content %in% c("raw", "text", "parsed")) {
    httr::content(response, content, encoding = "UTF-8")
  }
  else if (content == "response") {
    response
  }
  # Legacy code: By default it will, automatically attempt
  # figure out which one is most appropriate
  else {
    httr::content(response, encoding = "UTF-8")
  }
}
