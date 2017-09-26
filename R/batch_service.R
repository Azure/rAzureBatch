apiVersion <- "2017-06-01.5.1"

getBatchCredentials <- function(configPath = "az_config.json", ...) {
  config <- getOption("az_config")

  if (!is.null(config) && !is.null(config$batchAccount)) {
    batchAccount <- config$batchAccount
    credentials <-
      BatchCredentials$new(name = batchAccount$name,
                           key = batchAccount$key,
                           url = batchAccount$url)
  }
  else{
    config <- rjson::fromJSON(file = configPath)
    credentials <-
      BatchCredentials$new(
        name = config$batchAccount$name,
        key = config$batchAccount$key,
        url = config$batchAccount$url
      )
  }

  credentials
}

BatchCredentials <- setRefClass(
  "BatchCredentials",
  fields = list(name = "character", key = "character", url = "character"),
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

prepareBatchRequest <- function(request, credentials) {
  requestdate <- httr::http_date(Sys.time())
  request$headers['ocp-date'] <- requestdate

  authorizationHeader <-
    signAzureRequest(request, credentials$name, credentials$key, 'ocp-')

  request$headers['Authorization'] <- authorizationHeader
  request$headers['User-Agent'] <-
    paste0(
      "rAzureBatch/",
      packageVersion("rAzureBatch"),
      ";",
      "doAzureParallel/",
      packageVersion("doAzureParallel")
    )

  request$url <- paste0(credentials$url, request$path)

  request
}

callBatchService <- function(request, credentials, content, ...){
  request <- prepareBatchRequest(request, credentials)
  response <- executeAzureRequest(request, ...)

  extractAzureResponse(response, content)
}
