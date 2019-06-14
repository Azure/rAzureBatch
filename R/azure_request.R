AzureRequestV2 <- R6::R6Class(
  "AzureRequestV2",
  public = list(
    method = NULL,
    path = NULL,
    headers = NULL,
    query = NULL,
    body = NULL,
    # Httr Requests Configuration
    write = NULL,
    progress = NULL,
    verbose = NULL,
    content = NULL,
    initialize = function(method = NULL,
                          path = NULL,
                          headers = NULL,
                          query = NULL,
                          body = NULL,
                          write = NULL,
                          progress = NULL,
                          verbose = NULL,
                          content = NULL) {
      self$method <- method
      self$path <- path
      self$headers <- headers
      if (is.null(self$headers)) {
        self$headers <- character()
      }

      self$query <- query
      self$body <- body

      self$write <- write
      self$progress <- progress
      self$verbose <- verbose
      self$content <- content
    },
    encryptSignature = function(x, key) {
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

AzureServiceClient <- R6::R6Class(
  "AzureServiceClient",
  public = list(
    authentication = NULL,
    apiVersion = NULL,
    verbose = FALSE,
    initialize = function(url = NA, authentication = NA) {
      self$url <- url
      self$authentication <- authentication
    },
    executeRequest = function(url, request) {
      requestHeaders <- httr::add_headers(request$headers)

      verbose <- options("azureHttpTraffic")
      if (!is.null(verbose$azureHttpTraffic)
          && verbose$azureHttpTraffic == TRUE) {
        request$verbose <- httr::verbose()
      }

      if (request$method == "GET" ||
          request$method == "POST" ||
          request$method == "DELETE" ||
          request$method == "PUT" ||
          request$method == "PATCH") {
        httr::VERB(
          request$method,
          url,
          config = requestHeaders,
          body = request$body,
          query = request$query,
          encode = "json",
          request$write,
          request$verbose,
          request$progress
        )
      }
      else if (request$method == "HEAD") {
        httr::HEAD(
          url,
          config = requestHeaders,
          body = request$body,
          query = request$query,
          encode = "json",
          request$write,
          request$verbose,
          request$progress
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
    },
    extractAzureResponse = function(response, content) {
      httr::warn_for_status(response)

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
  )
)
